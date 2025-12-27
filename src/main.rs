use anyhow::{Context, Result, bail};
use clap::Parser;
use rand::Rng;
use serde_json::Value;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fs;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::{
    io::{self, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{ChildStdin, ChildStdout, Command},
    sync::{broadcast, mpsc},
};
use tokio_stream::{StreamExt, StreamMap, wrappers::ReceiverStream};
use tracing::{debug, info, trace};
use tracing_subscriber::filter::{EnvFilter, LevelFilter};

use self::config::LspConfig;
use json_patch::merge;
use tokio::sync::RwLock;

// Define the types used
type Message = String;
type ChildId = usize;

// The central, shared state manager
type CapabilityManager = Arc<RwLock<HashMap<ChildId, Value>>>;

mod config;

type RequestId = String; // Use string representation to handle both numbers and strings

struct InFlightRequest {
    expected_responses: usize,
    collected_responses: Vec<Value>,
    // A channel to send the final merged result back to the client task
    response_tx: tokio::sync::oneshot::Sender<String>,
}

type RequestRegistry = Arc<RwLock<HashMap<RequestId, InFlightRequest>>>;

async fn read_content_length<T>(reader: &mut BufReader<T>) -> Result<usize>
where
    BufReader<T>: AsyncBufReadExt,
    T: Unpin,
{
    let mut content_length = 0;
    loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            bail!("Connection closed");
        }
        trace!("read line: {}", line);
        if let Some(content) = line.strip_prefix("Content-Length: ") {
            content_length = content
                .trim()
                .parse()
                .context("Failed to parse Content-Length")?;
        } else if line.strip_prefix("Content-Type: ").is_some() {
            // ignored.
        } else if line == "\r\n" {
            break;
        } else {
            bail!("Failed to get Content-Length from LSP data.")
        }
    }
    Ok(content_length)
}

async fn read_out_message<T>(reader: &mut BufReader<T>) -> Result<Value>
where
    BufReader<T>: AsyncBufReadExt,
    T: Unpin,
{
    let content_length = read_content_length(reader).await?;
    let mut body = vec![0u8; content_length];
    reader.read_exact(&mut body).await?;
    let body_str = String::from_utf8_lossy(&body);
    trace!("read body: {}", body_str);
    serde_json::from_slice(&body).context("Failed to parse input as LSP data")
}

async fn proxy_stdin(mut stdin: ChildStdin, mut input: broadcast::Receiver<String>) {
    while let Ok(message) = input.recv().await {
        let header = format!("Content-Length: {}\r\n\r\n", message.len());
        if let Err(e) = stdin.write_all(header.as_bytes()).await {
            debug!("Failed to write header to child stdin: {}", e);
            break;
        }
        if let Err(e) = stdin.write_all(message.as_bytes()).await {
            debug!("Failed to write body to child stdin: {}", e);
            break;
        }
        if let Err(e) = stdin.flush().await {
            debug!("Failed to flush child stdin: {}", e);
            break;
        }
    }
}

fn merge_values(responses: &Vec<Value>) -> Value {
    match responses.first() {
        Some(base) => responses
            .iter()
            .skip(1)
            .fold(base.clone(), |mut acc, patch| {
                merge(&mut acc, patch);
                acc
            }),
        None => Value::Null,
    }
}

async fn proxy_message(
    reply: Value,
    child_id: ChildId,
    tx: &mpsc::Sender<String>,
    manager: &CapabilityManager,
    registry: &RequestRegistry,
) {
    // Discover capabilities on initialization
    {
        let guard = manager.read().await;
        if !guard.contains_key(&child_id) {
            drop(guard);
            let mut guard = manager.write().await;
            if !guard.contains_key(&child_id) {
                if let Some(result) = reply.get("result") {
                    if let Some(capabilities) = result.get("capabilities") {
                        guard.insert(child_id, capabilities.clone());
                    }
                }
            }
        }
    }

    let id_val = reply.get("id").cloned().unwrap_or(Value::Null);
    if id_val.is_null() {
        // This is a notification, forward it
        let _ = tx.send(serde_json::to_string(&reply).unwrap()).await;
        return;
    }

    let id_str = id_val.to_string();
    let mut reg_guard = registry.write().await;

    if let Entry::Occupied(mut occupied_entry) = reg_guard.entry(id_str) {
        let req = occupied_entry.get_mut();
        req.collected_responses.push(reply);
        if req.collected_responses.len() >= req.expected_responses {
            let finished_req = occupied_entry.remove();
            let merged = merge_values(&finished_req.collected_responses);
            let _ = finished_req
                .response_tx
                .send(serde_json::to_string(&merged).unwrap());
        }
    } else {
        // Untracked response or a request from server to client
        let _ = tx.send(serde_json::to_string(&reply).unwrap()).await;
    }
}

/// Reads the child's stdout. Parses output during initialization to discover
/// new capabilities, and forwards messages to the main sender.
async fn proxy_stdout(
    child_id: ChildId,
    mut stdout: BufReader<ChildStdout>,
    tx: mpsc::Sender<String>,
    manager: CapabilityManager,
    registry: RequestRegistry,
) {
    loop {
        match read_out_message(&mut stdout).await {
            Ok(message) => {
                if message.get("id").is_some() {
                    proxy_message(message, child_id, &tx, &manager, &registry).await;
                } else {
                    let _ = tx.send(serde_json::to_string(&message).unwrap()).await;
                }
            }
            Err(e) => {
                debug!("Child {} stdout closed: {}", child_id, e);
                break;
            }
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

    let (tx, _rx) = broadcast::channel(100);
    let (merged_tx, merged_rx) = mpsc::channel(100);
    let mut child_processes = Vec::new();
    let mut child_rxs = Vec::with_capacity(config.languages.len());
    let cap_manager = Arc::new(RwLock::new(HashMap::new()));
    let req_reg = Arc::new(RwLock::new(HashMap::new()));
    let mut rng = rand::rng();

    for lang in &config.languages {
        let child_id: ChildId = rng.random();
        // spawn LSP server command
        let mut cmd = Command::new(&lang.command);
        cmd.args(&lang.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped());
        let mut child = cmd
            .spawn()
            .with_context(|| format!("Failed to spawn {} binary.", &lang.command.display()))?;
        info!("spawned {}", lang.command.display());

        let child_stdin = child.stdin.take().unwrap();
        let child_stdout = BufReader::new(child.stdout.take().unwrap());

        let (child_tx, child_rx) = mpsc::channel(100);
        child_rxs.push(child_rx);

        let rx = tx.subscribe();
        tokio::spawn(async move {
            proxy_stdin(child_stdin, rx).await;
        });
        let manager_clone = Arc::clone(&cap_manager);
        let req_reg_clone = Arc::clone(&req_reg);
        tokio::spawn(async move {
            proxy_stdout(child_id, child_stdout, child_tx, manager_clone, req_reg_clone).await
        });

        // Keep child process alive
        child_processes.push(child);
    }

    // Centralized task to write all messages to stdout
    let num_langs = config.languages.len();
    tokio::spawn(async move {
        let mut stdout = io::stdout();
        let mut map: StreamMap<usize, ReceiverStream<String>> = StreamMap::new();

        for (key, rx) in child_rxs.into_iter().enumerate() {
            map.insert(key, ReceiverStream::new(rx));
        }
        // Add the merged responses stream
        map.insert(num_langs, ReceiverStream::new(merged_rx));

        while let Some((_, message)) = map.next().await {
            debug!("sending to client: {}", message);
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
    });

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

        // If it's a request (has ID and Method), track it
        if let Some(id) = message.get("id") {
            if message.get("method").is_some() {
                let id_str = id.to_string();
                let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
                {
                    let mut reg_guard = req_reg.write().await;
                    reg_guard.insert(
                        id_str.clone(),
                        InFlightRequest {
                            expected_responses: config.languages.len(),
                            collected_responses: Vec::new(),
                            response_tx: resp_tx,
                        },
                    );
                }
                let req_reg_clone = req_reg.clone();
                let merged_tx_clone = merged_tx.clone();
                tokio::spawn(async move {
                    tokio::select! {
                        result = resp_rx => {
                            if let Ok(response_str) = result {
                                let _ = merged_tx_clone.send(response_str).await;
                            }
                        }
                        _ = tokio::time::sleep(Duration::from_secs(30)) => {
                            // Timeout logic
                            let mut reg_guard = req_reg_clone.write().await;
                            if let Some(req) = reg_guard.remove(&id_str) {
                                if !req.collected_responses.is_empty() {
                                    let merged = merge_values(&req.collected_responses);
                                    let _ = merged_tx_clone.send(serde_json::to_string(&merged).unwrap()).await;
                                }
                            }
                        }
                    }
                });
            }
        }

        // Broadcast the raw message to all children
        if let Err(e) = tx.send(raw) {
            debug!("Failed to broadcast message to children: {}", e);
        }
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
