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
    sync::{broadcast, mpsc},
};
use tracing::{debug, info, trace};
use tracing_subscriber::filter::{EnvFilter, LevelFilter};

use self::config::LspConfig;
use json_patch::merge;

// Define the types used
type ChildId = usize;

mod config;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum LspId {
    Number(i64),
    String(String),
}
impl fmt::Display for LspId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LspId::Number(n) => write!(f, "{}", n),
            LspId::String(s) => write!(f, "\"{}\"", s),
        }
    }
}
type RequestId = LspId;

struct InFlightRequest {
    expected_responses: usize,
    collected_responses: Vec<Value>,
}

enum RegistryCommand {
    /// From stdin: "I'm sending a request with this ID, expect N replies."
    RegisterRequest {
        id: RequestId,
        expected_count: usize,
    },
    /// From a child server: "Here is one of the responses for RequestID."
    PushResponse { id: RequestId, response: Value },
    /// From the self-spawned timer: "Time is up, send what you've got."
    CheckTimeout { id: RequestId },
}

#[derive(Clone)]
struct RegistryHandle {
    tx: mpsc::Sender<RegistryCommand>,
}

impl RegistryHandle {
    pub async fn register(&self, id: RequestId, expected: usize) {
        debug!("Registering request {}", id);
        let _ = self
            .tx
            .send(RegistryCommand::RegisterRequest {
                id,
                expected_count: expected,
            })
            .await;
    }

    pub async fn push(&self, id: RequestId, response: Value) {
        let _ = self
            .tx
            .send(RegistryCommand::PushResponse {
                id,
                response: response,
            })
            .await;
    }
}

struct RegistryActor {
    inbox: mpsc::Receiver<RegistryCommand>,
    // Kept here so the Actor can clone it for timeout tasks
    self_sender: mpsc::Sender<RegistryCommand>,
    output_tx: mpsc::Sender<String>,
    requests: HashMap<RequestId, InFlightRequest>,
}

impl RegistryActor {
    pub fn new(output_tx: mpsc::Sender<String>) -> (Self, RegistryHandle) {
        let (tx, rx) = mpsc::channel(100);
        let actor = RegistryActor {
            inbox: rx,
            self_sender: tx.clone(),
            output_tx,
            requests: HashMap::new(),
        };
        let handle = RegistryHandle { tx };
        (actor, handle)
    }

    pub async fn run(mut self) {
        while let Some(cmd) = self.inbox.recv().await {
            match cmd {
                RegistryCommand::RegisterRequest { id, expected_count } => {
                    let inflight = InFlightRequest {
                        expected_responses: expected_count,
                        collected_responses: Vec::with_capacity(expected_count),
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
                RegistryCommand::PushResponse { id, response } => {
                    let id_clone = id.clone();
                    if let Entry::Occupied(mut occupied_entry) = self.requests.entry(id) {
                        let req = occupied_entry.get_mut();
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
        trace!("read line: {}", line);
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

async fn child_stdin_worker(mut stdin: ChildStdin, mut input: broadcast::Receiver<Value>) {
    loop {
        match input.recv().await {
            Ok(message) => {
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
            Err(broadcast::error::RecvError::Lagged(count)) => {
                debug!("Child stdin worker lagged by {} messages", count);
                // Keep looping! Don't exit.
            }
            Err(broadcast::error::RecvError::Closed) => {
                // The main loop dropped the sender, time to shut down.
                break;
            }
        }
    }
}

fn merge_values(responses: &Vec<Value>, id: LspId) -> Value {
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
        let body_str = String::from_utf8_lossy(&body);
        debug!("read body: {}", body_str);
        let message: Result<Value> =
            serde_json::from_slice(&body).context("Failed to parse input as LSP data");
        match message {
            Ok(message) => {
                if let Some(id_val) = message.get("id") {
                    if !message.get("method").is_some() {
                        debug!("got response for message: {}", id_val);
                        let lsp_id: LspId = serde_json::from_value(id_val.clone()).unwrap();
                        handle.push(lsp_id, message).await
                    } else {
                        debug!("got request from server: {}", id_val);
                        let _ = output_tx
                            .send(serde_json::to_string(&message).unwrap())
                            .await;
                    }
                } else {
                    debug!("got notification from server");
                    let _ = output_tx
                        .send(serde_json::to_string(&message).unwrap())
                        .await;
                }
            }
            Err(e) => {
                debug!("Child {} stdout closed: {}", child_id, e);
                break;
            }
        }
    }
}

async fn output_actor(mut rx: mpsc::Receiver<String>) {
    let mut stdout = io::stdout();
    while let Some(message) = rx.recv().await {
        debug!("sending to main stdout: {}", message);
        let header = format!("Content-Length: {}\r\n\r\n", message.len());
        if let Err(e) = stdout.write_all(header.as_bytes()).await {
            debug!("Failed to write header to stdout: {}", e);
            break;
        }
        if let Err(e) = stdout.write_all(message.as_bytes()).await {
            debug!("Failed to write body to stdout: {}", e);
            break;
        }
        if let Err(e) = stdout.flush().await {
            debug!("Failed to flush stdout: {}", e);
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
            .with_default_directive(LevelFilter::DEBUG.into())
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

    // broadcast channel: main stdin to child stdins
    let (broadcast_tx, _) = broadcast::channel::<Value>(100);
    let lang_count = config.languages.len();
    // spawn servers
    for (child_id, lang) in config.languages.into_iter().enumerate() {
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

        let user_input_rx = broadcast_tx.subscribe();
        tokio::spawn(async move {
            child_stdin_worker(child_stdin, user_input_rx).await;
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
                info!("Stdin closed: {}", e);
                break;
            }
        };
        let mut body = vec![0u8; content_length];
        if let Err(e) = stdin.read_exact(&mut body).await {
            debug!("Failed to read body from stdin: {}", e);
            break;
        }
        let raw = String::from_utf8(body)?;
        let message: Value = serde_json::from_str(&raw)?;
        if let Some(id_val) = message.get("id") {
            if let Ok(lsp_id) = serde_json::from_value(id_val.clone()) {
                // Only register if it's a request (has an id AND a method)
                if message.get("method").is_some() {
                    registry_handle.register(lsp_id, lang_count).await;
                }
            }
        }
        broadcast_tx.send(message).unwrap();
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
