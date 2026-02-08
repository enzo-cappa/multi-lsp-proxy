use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::hash_map::Entry;
use std::fs;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use std::{collections::HashMap, fmt};
use tokio::{
    io::{self, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{ChildStdin, ChildStdout, Command},
    sync::mpsc,
};
use tracing::{debug, error, info, trace, warn};
use tracing_subscriber::filter::{EnvFilter, LevelFilter};

use self::config::LspConfig;
use json_patch::merge;
use lsp_types::*;

// Define the types used
type ChildId = usize;

mod config;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RequestId {
    Number(i64),
    String(String),
}
impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestId::Number(n) => write!(f, "{}", n),
            RequestId::String(s) => write!(f, "\"{}\"", s),
        }
    }
}
struct InFlightChildRequest {
    child_id: ChildId,
    orig_req_id: RequestId,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum Message {
    Request { id: RequestId, method: String },
    Response { id: RequestId, result: Value },
    Error { id: RequestId, error: Value },
    Notification {},
}

struct Server {
    capabilities: Option<ServerCapabilities>,
    tx: mpsc::Sender<Value>,
}

impl Server {
    pub fn server_supports_method(&self, method: &str) -> bool {
        trace!(
            "checking caps. method {} caps {:?}",
            method, self.capabilities
        );
        match self.capabilities.as_ref() {
            None => false,
            Some(caps) => {
                match method {
                    // --- Basic Query Providers ---
                    "textDocument/hover" => caps.hover_provider.is_some(),
                    "textDocument/definition" => caps.definition_provider.is_some(),
                    "textDocument/typeDefinition" => caps.type_definition_provider.is_some(),
                    "textDocument/implementation" => caps.implementation_provider.is_some(),
                    "textDocument/references" => caps.references_provider.is_some(),
                    "textDocument/documentHighlight" => caps.document_highlight_provider.is_some(),
                    "textDocument/documentSymbol" => caps.document_symbol_provider.is_some(),

                    // --- Editing & Refactoring ---
                    "textDocument/completion" => caps.completion_provider.is_some(),
                    "textDocument/signatureHelp" => caps.signature_help_provider.is_some(),
                    "textDocument/codeAction" => caps.code_action_provider.is_some(),
                    "textDocument/codeLens" => caps.code_lens_provider.is_some(),
                    "textDocument/formatting" => caps.document_formatting_provider.is_some(),
                    "textDocument/rangeFormatting" => {
                        caps.document_range_formatting_provider.is_some()
                    }
                    "textDocument/onTypeFormatting" => {
                        caps.document_on_type_formatting_provider.is_some()
                    }
                    "textDocument/rename" => caps.rename_provider.is_some(),
                    "textDocument/prepareRename" => caps.rename_provider.is_some(),
                    "textDocument/documentLink" => caps.document_link_provider.is_some(),

                    // --- Semantic Tokens (Nested Logic) ---
                    m if m.starts_with("textDocument/semanticTokens") => {
                        match (caps.semantic_tokens_provider.as_ref(), m) {
                            (Some(p), "textDocument/semanticTokens/full") => {
                                Self::check_semantic_full(p)
                            }
                            (Some(p), "textDocument/semanticTokens/range") => {
                                Self::check_semantic_range(p)
                            }
                            (Some(_), _) => true, // Default true for other semantic sub-methods if provider exists
                            _ => false,
                        }
                    }

                    // --- Workspace ---
                    "workspace/symbol" => caps.workspace_symbol_provider.is_some(),
                    "workspace/executeCommand" => caps.execute_command_provider.is_some(),

                    // --- Notifications (Lifecycle & State) ---
                    // These should almost always be broadcast to all servers to keep their
                    // internal virtual file systems in sync.
                    "textDocument/didOpen"
                    | "textDocument/didChange"
                    | "textDocument/didSave"
                    | "textDocument/didClose"
                    | "initialized"
                    | "exit" => true,

                    // Inside LspRouter::server_supports_method
                    "codeAction/resolve" => {
                        caps.code_action_provider.as_ref().map_or(false, |p| {
                            match p {
                                CodeActionProviderCapability::Simple(enabled) => {
                                    // If it's just a boolean 'true', it usually doesn't
                                    // imply resolve support in older LSP versions.
                                    *enabled
                                }
                                CodeActionProviderCapability::Options(opt) => {
                                    // This is the modern way: check the resolve_provider flag
                                    opt.resolve_provider.unwrap_or(false)
                                }
                            }
                        })
                    }

                    // --- Default Fallback ---
                    // If it's a $/ method (private/custom) or something unknown,
                    // broadcasting is usually safer than dropping.
                    _ => true,
                }
            }
        }
    }

    fn check_semantic_full(p: &SemanticTokensServerCapabilities) -> bool {
        match p {
            SemanticTokensServerCapabilities::SemanticTokensOptions(opt) => opt.full.is_some(),
            SemanticTokensServerCapabilities::SemanticTokensRegistrationOptions(reg) => {
                reg.semantic_tokens_options.full.is_some()
            }
        }
    }

    fn check_semantic_range(p: &SemanticTokensServerCapabilities) -> bool {
        match p {
            SemanticTokensServerCapabilities::SemanticTokensOptions(opt) => opt.range.is_some(),
            SemanticTokensServerCapabilities::SemanticTokensRegistrationOptions(reg) => {
                reg.semantic_tokens_options.range.is_some()
            }
        }
    }
}

struct InFlightRequest {
    expected_responses: usize,
    collected_responses: Vec<Value>,
    is_initialize: bool,
}

enum RegistryCommand {
    /// From the self-spawned timer: "Time is up, send what you've got."
    CheckTimeout {
        id: RequestId,
    },
    /// From a child server: "Here is one of the responses for RequestID."
    PushResponse {
        id: RequestId,
        child_id: ChildId,
        response: Value,
    },
    RegisterChildRequest {
        orig_id: RequestId,
        new_id: RequestId,
        child_id: ChildId,
    },
    RegisterChild {
        id: ChildId,
        tx: mpsc::Sender<Value>,
    },
    RouteMessage {
        message: Value,
    },
}

#[derive(Clone)]
struct RegistryHandle {
    tx: mpsc::Sender<RegistryCommand>,
}

impl RegistryHandle {
    pub async fn push_response(&self, id: RequestId, child_id: ChildId, response: Value) {
        debug!("pushing response! {} {}", id, child_id);
        let _ = self
            .tx
            .send(RegistryCommand::PushResponse {
                id,
                child_id,
                response,
            })
            .await;
    }

    pub async fn register_child_request(
        &self,
        new_id: RequestId,
        orig_id: RequestId,
        child_id: ChildId,
    ) {
        let _ = self
            .tx
            .send(RegistryCommand::RegisterChildRequest {
                orig_id,
                new_id,
                child_id,
            })
            .await;
    }

    pub async fn route(&self, message: Value) {
        let _ = self
            .tx
            .send(RegistryCommand::RouteMessage { message })
            .await;
    }

    pub async fn add_server(&self, child_id: ChildId, tx: mpsc::Sender<Value>) {
        let _ = self
            .tx
            .send(RegistryCommand::RegisterChild { id: child_id, tx })
            .await;
    }
}

struct RegistryActor {
    inbox: mpsc::Receiver<RegistryCommand>,
    // Kept here so the Actor can clone it for timeout tasks
    self_sender: mpsc::Sender<RegistryCommand>,
    output_tx: mpsc::Sender<String>,
    requests: HashMap<RequestId, InFlightRequest>,
    child_requests: HashMap<RequestId, InFlightChildRequest>,
    child_skills: HashMap<ChildId, Server>,
}

impl RegistryActor {
    pub fn new(output_tx: mpsc::Sender<String>) -> (Self, RegistryHandle) {
        let (tx, rx) = mpsc::channel(100);
        let actor = RegistryActor {
            inbox: rx,
            self_sender: tx.clone(),
            output_tx,
            requests: HashMap::new(),
            child_skills: HashMap::new(),
            child_requests: HashMap::new(),
        };
        let handle = RegistryHandle { tx };
        (actor, handle)
    }

    async fn handle_client_message(&mut self, mut message: Value) {
        let request: Message = serde_json::from_value(message.clone()).unwrap();
        let mut targets = Vec::new();
        let mut req_id: Option<&RequestId> = None;
        let mut is_initialize = false;

        if let Message::Response { ref id, result: _ } = request {
            debug!("Got client response, {}", id);
            if let Some(entry) = self.child_requests.remove_entry(id) {
                let (req_id, request) = entry;
                let server = self
                    .child_skills
                    .get(&request.child_id)
                    .expect("Can't find server");
                let repl = message.as_object_mut().expect("Should be an object!");
                repl.remove("id");
                repl.insert(
                    "id".to_string(),
                    serde_json::to_value(request.orig_req_id).unwrap(),
                );
                let _ = server.tx.send(message.clone()).await;
                return;
            } else {
                warn!("Cant find child request in registry: {}", id);
            }
        } else {
            for (child_id, child) in self.child_skills.iter() {
                match request {
                    Message::Notification { .. } => {
                        targets.push(child.tx.clone());
                    }
                    Message::Response { ref id, result: _ } => {}
                    Message::Error { .. } => {
                        targets.push(child.tx.clone());
                    }
                    Message::Request { ref id, ref method } => {
                        req_id = Some(id);
                        if method == "initialize" {
                            targets.push(child.tx.clone());
                            is_initialize = true;
                        } else if child.server_supports_method(method) {
                            debug!("sending request to server {} {}", child_id, method);
                            targets.push(child.tx.clone());
                        }
                    }
                }
            }
        }

        if targets.len() == 0 {
            warn!("No target found! message {}", message);
            return;
        }

        // Now we know exactly how many responses to expect!
        if req_id.is_some() {
            self.register_internal(req_id.unwrap().clone(), targets.len(), is_initialize);
        }

        for tx in targets {
            let _ = tx.send(message.clone()).await;
        }
    }

    fn register_internal(&mut self, id: RequestId, expected_count: usize, is_initiliaze: bool) {
        debug!("registering request {}", id);
        let inflight = InFlightRequest {
            expected_responses: expected_count,
            collected_responses: Vec::with_capacity(expected_count),
            is_initialize: is_initiliaze,
        };
        let id_to_check = id.clone();
        self.requests.insert(id, inflight);
        let registry_tx = self.self_sender.clone();
        tokio::spawn(async move {
            _ = tokio::time::sleep(Duration::from_secs(5)).await;
            _ = registry_tx
                .send(RegistryCommand::CheckTimeout { id: id_to_check })
                .await;
        });
    }

    pub async fn run(mut self) {
        while let Some(cmd) = self.inbox.recv().await {
            match cmd {
                RegistryCommand::PushResponse {
                    id,
                    child_id,
                    mut response,
                } => {
                    debug!("in actor, PushResponse {} {}", id, child_id);
                    let id_clone = id.clone();
                    if let Entry::Occupied(mut occupied_entry) = self.requests.entry(id) {
                        let req = occupied_entry.get_mut();
                        if req.is_initialize {
                            trace!("is initialize {}", response);
                            if let Some(result) = response.get_mut("result")
                                && let Some(caps_json) = result.get("capabilities")
                            {
                                let caps: ServerCapabilities =
                                    serde_json::from_value(caps_json.clone()).unwrap_or_default();
                                trace!(
                                    "found capabilities child id: {}; caps {:?}",
                                    child_id, caps
                                );
                                let current = self.child_skills.remove(&child_id).unwrap();
                                self.child_skills.insert(
                                    child_id,
                                    Server {
                                        capabilities: Some(caps),
                                        ..current
                                    },
                                );
                                let repl = result.as_object_mut().expect("Should be an object!");
                                repl.remove("serverInfo");
                                repl.insert(
                                    "serverInfo".to_string(),
                                    json!({
                                        "name": "mlp",
                                        "version": "0.0.1"
                                    }),
                                );
                            }
                        }
                        req.collected_responses.push(response);
                        if req.collected_responses.len() >= req.expected_responses {
                            let finished_req = occupied_entry.remove();
                            let merged = merge_values(&finished_req.collected_responses, id_clone);
                            let _ = self
                                .output_tx
                                .send(serde_json::to_string(&merged).unwrap())
                                .await
                                .context("Output task died");
                        }
                    }
                    // Expired responses get dropped!
                }
                RegistryCommand::CheckTimeout { id } => {
                    if let Some(req) = self.requests.remove(&id) {
                        debug!("Message timed out {}", id);
                        let response: Value = if !req.collected_responses.is_empty() {
                            merge_values(&req.collected_responses, id)
                        } else {
                            json!({"id": id, "jsonrpc": "2.0","result":null})
                        };

                        let _ = self
                            .output_tx
                            .send(serde_json::to_string(&response).unwrap())
                            .await
                            .context("Output task died");
                    }
                }
                RegistryCommand::RegisterChild { id, tx } => {
                    self.child_skills.insert(
                        id,
                        Server {
                            tx,
                            capabilities: None,
                        },
                    );
                }
                RegistryCommand::RouteMessage { message } => {
                    self.handle_client_message(message).await
                }
                RegistryCommand::RegisterChildRequest {
                    orig_id,
                    new_id,
                    child_id,
                } => {
                    self.child_requests.insert(
                        new_id,
                        InFlightChildRequest {
                            orig_req_id: orig_id,
                            child_id,
                        },
                    );
                }
            }
        }
    }
}

async fn read_content_length<T>(reader: &mut BufReader<T>) -> Result<usize>
where
    T: io::AsyncRead + Unpin,
{
    let mut content_length = None;
    loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            bail!("Connection closed");
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            // We hit the empty line (\r\n\r\n separator)
            break;
        }

        if let Some(content) = line.strip_prefix("Content-Length: ") {
            content_length = Some(content.trim().parse().context("Invalid number")?);
        }
        // Any other headers (Content-Type, etc.) are ignored
    }
    content_length.ok_or_else(|| anyhow!("Missing Content-Length header"))
}

async fn child_stdin_worker(mut stdin: ChildStdin, mut input: mpsc::Receiver<Value>) {
    loop {
        match input.recv().await {
            Some(message) => {
                let message_bytes = serde_json::to_vec(&message).unwrap();
                let header = format!("Content-Length: {}\r\n\r\n", message_bytes.len());

                if let Err(_e) = stdin.write_all(header.as_bytes()).await {
                    break;
                }
                if let Err(_e) = stdin.write_all(&message_bytes).await {
                    break;
                }
                if let Err(_e) = stdin.flush().await {
                    break;
                }
            }
            None => {
                // The main loop dropped the sender, time to shut down.
                break;
            }
        }
    }
}

fn merge_values(responses: &Vec<Value>, id: RequestId) -> Value {
    let error_response = responses.iter().find(|r| r.get("error").is_some());
    if let Some(error) = error_response {
        return error.clone();
    }
    let results: Vec<&Value> = responses
        .iter()
        .filter_map(|r| r.get("result"))
        .filter(|r| !r.is_null()) // Ignore nulls
        .collect();

    if results.is_empty() {
        return json!({ "jsonrpc": "2.0", "id": id, "result": null });
    }

    let mut base_result = match results[0] {
        Value::Array(_) => json!([]),
        _ => json!({}),
    };
    for res in results {
        match (&mut base_result, res) {
            // If both are arrays, append them
            (Value::Array(base), Value::Array(new)) => {
                base.extend(new.iter().cloned());
            }
            // If both are objects, deep merge them
            (Value::Object(_), Value::Object(new)) => {
                merge(&mut base_result, &Value::Object(new.clone()));
            }
            // Fallback: Just take the new value if types don't match
            (_, _) => base_result = res.clone(),
        }
    }

    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": base_result
    })
}

/// Reads the child's stdout. Parses output during initialization to discover
/// new capabilities, and forwards messages to the main sender.
async fn child_stdout_worker(
    child_id: ChildId,
    stdout: ChildStdout,
    handle: RegistryHandle,
    output_tx: mpsc::Sender<String>,
) {
    let mut stdout_reader = BufReader::new(stdout);
    loop {
        let content_length = match read_content_length(&mut stdout_reader).await {
            Ok(len) => len,
            Err(_) => break,
        };
        let mut body = vec![0u8; content_length];
        stdout_reader.read_exact(&mut body).await.unwrap();
        let message: Result<Value> =
            serde_json::from_slice(&body).context("Failed to parse input as LSP data");
        match message {
            Ok(mut message) => {
                if let Some(id_val) = message.get("id") {
                    let msg_id: RequestId = serde_json::from_value(id_val.clone()).unwrap();
                    debug!("Got message from child with id {}", msg_id);
                    if !message.get("method").is_some() {
                        trace!("got response from child_id: {} {}", child_id, message);
                        handle.push_response(msg_id, child_id, message).await
                    } else {
                        trace!("got request from child_id: {} {}", child_id, message);
                        let new_id =
                            RequestId::String(format!("{}{}", (child_id + 1) * 100, msg_id));
                        debug!("request id change from {} to {}", msg_id, new_id);
                        handle
                            .register_child_request(new_id.clone(), msg_id, child_id)
                            .await;
                        let repl = message.as_object_mut().expect("Should be an object!");
                        repl.remove("id");
                        repl.insert("id".to_string(), serde_json::to_value(new_id).unwrap());
                        let method = repl.get("method").and_then(|m| m.as_str()).unwrap_or("");
                        if method == "textDocument/publishDiagnostics" {
                            if let Some(params) = repl.get_mut("params") {
                                if let Some(diagnostics) =
                                    params.get_mut("diagnostics").and_then(|d| d.as_array_mut())
                                {
                                    for diagnostic in diagnostics {
                                        // Get the existing source (e.g., "rust-analyzer" or "pyright")
                                        let original_source = diagnostic
                                            .get("source")
                                            .and_then(|s| s.as_str())
                                            .unwrap_or("lsp");

                                        // Tag it so the client treats them as distinct collections

                                        diagnostic["source"] =
                                            json!(format!("{}:{}", "mlp", original_source));
                                    }
                                }
                            }
                        }
                        let _ = output_tx.send(serde_json::to_string(&repl).unwrap()).await;
                    }
                } else {
                    trace!("got notification from server: {}", message);
                    let _ = output_tx
                        .send(serde_json::to_string(&message).unwrap())
                        .await;
                }
            }
            Err(e) => {
                error!("Child {} stdout closed: {}", child_id, e);
                break;
            }
        }
    }
}

async fn output_actor(mut rx: mpsc::Receiver<String>) {
    let mut stdout = io::stdout();
    while let Some(message) = rx.recv().await {
        let header = format!("Content-Length: {}\r\n\r\n", message.len());
        if let Err(e) = stdout.write_all(header.as_bytes()).await {
            error!("Failed to write header to stdout: {}", e);
            break;
        }
        if let Err(e) = stdout.write_all(message.as_bytes()).await {
            error!("Failed to write body to stdout: {}", e);
            break;
        }
        if let Err(e) = stdout.flush().await {
            error!("Failed to flush stdout: {}", e);
            break;
        }
    }
}

async fn run(config: LspConfig) -> Result<()> {
    // keep tracing_appender guard alive
    let mut _tracing_guard = None;
    if let Some(log_file) = config.log_file.as_ref() {
        // setup tracing
        let directory = log_file.parent().unwrap();
        let file_name = log_file.file_name().unwrap();
        let file_appender = tracing_appender::rolling::never(directory, file_name);
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
        _tracing_guard = Some(guard);

        let env_filter = EnvFilter::builder()
            .with_default_directive(LevelFilter::TRACE.into())
            .from_env_lossy();
        tracing_subscriber::fmt()
            .with_writer(non_blocking)
            .with_env_filter(env_filter)
            .init();
    }

    let (output_tx, output_rx) = mpsc::channel(100);
    tokio::spawn(output_actor(output_rx));

    let (registry_actor, registry_handle) = RegistryActor::new(output_tx.clone());
    tokio::spawn(registry_actor.run());

    // spawn servers
    for (child_id, lang) in config.languages.into_iter().enumerate() {
        let (stdin_tx, stdin_rx) = mpsc::channel::<Value>(100);
        registry_handle.add_server(child_id, stdin_tx).await;
        // spawn LSP server command
        let mut cmd = Command::new(&lang.command);
        let mut child = cmd
            .args(&lang.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .with_context(|| format!("Failed to spawn {} binary.", &lang.command.display()))?;
        info!("spawned {}", lang.command.display());

        let child_stdin = child.stdin.take().unwrap();
        let child_stdout = child.stdout.take().unwrap();

        tokio::spawn(async move {
            child_stdin_worker(child_stdin, stdin_rx).await;
        });
        let child_output = output_tx.clone();
        let handle = registry_handle.clone();
        tokio::spawn(async move {
            child_stdout_worker(child_id, child_stdout, handle, child_output).await
        });
    }

    // LSP server main loop
    // Read new command, send to all child LSP servers
    let mut stdin = BufReader::new(io::stdin());
    loop {
        let content_length = match read_content_length(&mut stdin).await {
            Ok(len) => len,
            Err(e) => {
                error!("Stdin closed: {}", e);
                break;
            }
        };
        let mut body = vec![0u8; content_length];
        if let Err(e) = stdin.read_exact(&mut body).await {
            error!("Failed to read body from stdin: {}", e);
            break;
        }
        let raw = String::from_utf8(body)?;
        let message: Value = serde_json::from_str(&raw)?;
        trace!("Got client message: {}", message);
        registry_handle.route(message).await;
    }
    Ok(())
}

#[derive(Debug, Parser)]
#[command(version)]
struct Cli {
    /// Configuration file path
    #[arg(short = 'c', long)]
    config: PathBuf,
    /// Select language servers by programming language name
    #[arg(short = 'l', long)]
    language: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── merge_values ──────────────────────────────────────────────

    #[test]
    fn merge_single_response_passthrough() {
        let responses = vec![json!({"jsonrpc":"2.0","id":1,"result":{"hover":"hello"}})];
        let merged = merge_values(&responses, RequestId::Number(1));
        assert_eq!(merged["result"]["hover"], "hello");
    }

    #[test]
    fn merge_arrays_concatenated() {
        let responses = vec![
            json!({"jsonrpc":"2.0","id":1,"result":[1,2]}),
            json!({"jsonrpc":"2.0","id":1,"result":[3,4]}),
        ];
        let merged = merge_values(&responses, RequestId::Number(1));
        assert_eq!(merged["result"], json!([1, 2, 3, 4]));
    }

    #[test]
    fn merge_objects_deep_merged() {
        let responses = vec![
            json!({"jsonrpc":"2.0","id":1,"result":{"a":1}}),
            json!({"jsonrpc":"2.0","id":1,"result":{"b":2}}),
        ];
        let merged = merge_values(&responses, RequestId::Number(1));
        assert_eq!(merged["result"]["a"], 1);
        assert_eq!(merged["result"]["b"], 2);
    }

    #[test]
    fn merge_error_takes_precedence() {
        let responses = vec![
            json!({"jsonrpc":"2.0","id":1,"result":{"a":1}}),
            json!({"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"bad"}}),
        ];
        let merged = merge_values(&responses, RequestId::Number(1));
        assert!(merged.get("error").is_some());
        assert_eq!(merged["error"]["code"], -32600);
    }

    #[test]
    fn merge_null_results_ignored() {
        let responses = vec![
            json!({"jsonrpc":"2.0","id":1,"result":null}),
            json!({"jsonrpc":"2.0","id":1,"result":{"a":1}}),
        ];
        let merged = merge_values(&responses, RequestId::Number(1));
        assert_eq!(merged["result"]["a"], 1);
    }

    #[test]
    fn merge_all_null_results() {
        let responses = vec![
            json!({"jsonrpc":"2.0","id":1,"result":null}),
            json!({"jsonrpc":"2.0","id":1,"result":null}),
        ];
        let merged = merge_values(&responses, RequestId::Number(1));
        assert!(merged["result"].is_null());
    }

    #[test]
    fn merge_empty_responses() {
        let responses = vec![];
        let merged = merge_values(&responses, RequestId::Number(1));
        assert!(merged["result"].is_null());
    }

    // ── Server::server_supports_method ────────────────────────────

    fn make_server(caps: Option<ServerCapabilities>) -> Server {
        let (tx, _rx) = mpsc::channel(1);
        Server {
            capabilities: caps,
            tx,
        }
    }

    #[test]
    fn supports_method_no_capabilities_returns_false() {
        let server = make_server(None);
        assert!(!server.server_supports_method("textDocument/hover"));
        assert!(!server.server_supports_method("textDocument/completion"));
    }

    #[test]
    fn supports_method_hover() {
        let mut caps = ServerCapabilities::default();
        caps.hover_provider = Some(HoverProviderCapability::Simple(true));
        let server = make_server(Some(caps));
        assert!(server.server_supports_method("textDocument/hover"));
        assert!(!server.server_supports_method("textDocument/completion"));
    }

    #[test]
    fn supports_method_completion() {
        let mut caps = ServerCapabilities::default();
        caps.completion_provider = Some(CompletionOptions::default());
        let server = make_server(Some(caps));
        assert!(server.server_supports_method("textDocument/completion"));
        assert!(!server.server_supports_method("textDocument/hover"));
    }

    #[test]
    fn supports_method_lifecycle_always_true() {
        let caps = ServerCapabilities::default();
        let server = make_server(Some(caps));
        for method in &[
            "textDocument/didOpen",
            "textDocument/didChange",
            "textDocument/didSave",
            "textDocument/didClose",
            "initialized",
            "exit",
        ] {
            assert!(server.server_supports_method(method), "{} should be true", method);
        }
    }

    #[test]
    fn supports_method_code_action_resolve_with_resolve_provider() {
        let mut caps = ServerCapabilities::default();
        caps.code_action_provider = Some(CodeActionProviderCapability::Options(CodeActionOptions {
            resolve_provider: Some(true),
            ..Default::default()
        }));
        let server = make_server(Some(caps));
        assert!(server.server_supports_method("codeAction/resolve"));
    }

    #[test]
    fn supports_method_code_action_resolve_without_resolve_provider() {
        let mut caps = ServerCapabilities::default();
        caps.code_action_provider = Some(CodeActionProviderCapability::Options(CodeActionOptions {
            resolve_provider: None,
            ..Default::default()
        }));
        let server = make_server(Some(caps));
        assert!(!server.server_supports_method("codeAction/resolve"));
    }

    #[test]
    fn supports_method_semantic_tokens_full() {
        let mut caps = ServerCapabilities::default();
        caps.semantic_tokens_provider =
            Some(SemanticTokensServerCapabilities::SemanticTokensOptions(
                SemanticTokensOptions {
                    full: Some(SemanticTokensFullOptions::Bool(true)),
                    range: None,
                    ..Default::default()
                },
            ));
        let server = make_server(Some(caps));
        assert!(server.server_supports_method("textDocument/semanticTokens/full"));
        assert!(!server.server_supports_method("textDocument/semanticTokens/range"));
    }

    #[test]
    fn supports_method_semantic_tokens_range() {
        let mut caps = ServerCapabilities::default();
        caps.semantic_tokens_provider =
            Some(SemanticTokensServerCapabilities::SemanticTokensOptions(
                SemanticTokensOptions {
                    full: None,
                    range: Some(true),
                    ..Default::default()
                },
            ));
        let server = make_server(Some(caps));
        assert!(!server.server_supports_method("textDocument/semanticTokens/full"));
        assert!(server.server_supports_method("textDocument/semanticTokens/range"));
    }

    #[test]
    fn supports_method_unknown_defaults_true() {
        let caps = ServerCapabilities::default();
        let server = make_server(Some(caps));
        assert!(server.server_supports_method("$/customNotification"));
        assert!(server.server_supports_method("someUnknownMethod"));
    }

    // ── RequestId ─────────────────────────────────────────────────

    #[test]
    fn request_id_display_number() {
        let id = RequestId::Number(123);
        assert_eq!(format!("{}", id), "123");
    }

    #[test]
    fn request_id_display_string() {
        let id = RequestId::String("abc".into());
        assert_eq!(format!("{}", id), "\"abc\"");
    }

    #[test]
    fn request_id_serde_roundtrip_number() {
        let id = RequestId::Number(42);
        let serialized = serde_json::to_string(&id).unwrap();
        let deserialized: RequestId = serde_json::from_str(&serialized).unwrap();
        assert_eq!(id, deserialized);
    }

    #[test]
    fn request_id_serde_roundtrip_string() {
        let id = RequestId::String("req-1".into());
        let serialized = serde_json::to_string(&id).unwrap();
        let deserialized: RequestId = serde_json::from_str(&serialized).unwrap();
        assert_eq!(id, deserialized);
    }

    // ── Message deserialization ───────────────────────────────────

    #[test]
    fn message_deser_request() {
        let json = json!({"jsonrpc":"2.0","id":1,"method":"textDocument/hover"});
        let msg: Message = serde_json::from_value(json).unwrap();
        assert!(matches!(msg, Message::Request { .. }));
    }

    #[test]
    fn message_deser_response() {
        let json = json!({"jsonrpc":"2.0","id":1,"result":{"a":1}});
        let msg: Message = serde_json::from_value(json).unwrap();
        assert!(matches!(msg, Message::Response { .. }));
    }

    #[test]
    fn message_deser_error() {
        let json = json!({"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"bad"}});
        let msg: Message = serde_json::from_value(json).unwrap();
        assert!(matches!(msg, Message::Error { .. }));
    }

    #[test]
    fn message_deser_notification() {
        let json = json!({"jsonrpc":"2.0","method":"initialized"});
        let msg: Message = serde_json::from_value(json).unwrap();
        assert!(matches!(msg, Message::Notification { .. }));
    }

    // ── read_content_length ───────────────────────────────────────

    #[tokio::test]
    async fn read_content_length_valid() {
        let input = b"Content-Length: 42\r\n\r\n";
        let mut reader = BufReader::new(&input[..]);
        let len = read_content_length(&mut reader).await.unwrap();
        assert_eq!(len, 42);
    }

    #[tokio::test]
    async fn read_content_length_missing_header() {
        let input = b"Content-Type: application/json\r\n\r\n";
        let mut reader = BufReader::new(&input[..]);
        let result = read_content_length(&mut reader).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn read_content_length_empty_input() {
        let input = b"";
        let mut reader = BufReader::new(&input[..]);
        let result = read_content_length(&mut reader).await;
        assert!(result.is_err());
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config_content = fs::read_to_string(&cli.config)?;
    let mut lsp_config: LspConfig = toml_edit::easy::from_str(&config_content)?;
    if let Some(lang) = cli.language.as_deref() {
        lsp_config.languages.retain(|l| l.name == lang);
    }
    if lsp_config.languages.is_empty() {
        if let Some(lang) = cli.language.as_deref() {
            bail!("No language server found for {}.", lang);
        }
        bail!("No language server found.");
    }
    run(lsp_config).await?;
    Ok(())
}
