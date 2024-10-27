use anyhow::{anyhow, bail, Context};
use log::{debug, info, trace, warn};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::fmt::Display;
use std::future::Future;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader, Lines, WriteHalf};
use tokio::sync::{mpsc, oneshot, watch, Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::{process, time};

#[cfg(target_os = "windows")]
mod mpv_platform {
    use std::path::PathBuf;
    use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeClient};
    pub type Stream = NamedPipeClient;
    pub async fn connect(path: &PathBuf) -> Result<Stream, ()> {
        let opts = ClientOptions::new();
        opts.open(path).or(Err(()))
    }
    pub fn default_ipc_path() -> PathBuf {
        "\\\\.\\pipe\\mpv_ipc".into()
    }
    pub fn default_mpv_bin() -> PathBuf {
        "mpv.exe".into()
    }
}
#[cfg(not(target_os = "windows"))]
mod mpv_platform {
    use std::path::PathBuf;
    use tokio::net::UnixStream;
    pub type Stream = UnixStream;
    pub async fn connect(path: &PathBuf) -> Result<Stream, ()> {
        UnixStream::connect(&path).await.or(Err(()))
    }
    pub fn default_ipc_path() -> PathBuf {
        let dir = std::env::temp_dir();
        dir.join("mpv_ipc.sock")
    }
    pub fn default_mpv_bin() -> PathBuf {
        "mpv".into()
    }
}

#[derive(Serialize, Deserialize)]
struct MpvCommand {
    request_id: usize,
    command: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug)]
struct MpvResponse {
    request_id: usize,
    data: MpvDataOption,
    error: String,
}

type LockedMpvIdMap<T> = Arc<Mutex<HashMap<usize, T>>>;
type MpvDataOption = Option<serde_json::Value>;

#[derive(Clone)]
pub struct MpvSpawnOptions {
    pub mpv_bin: PathBuf,
    pub ipc_path: PathBuf,
    pub config_dir: Option<PathBuf>,
    pub inherit_stdout: bool,
}
impl Default for MpvSpawnOptions {
    fn default() -> Self {
        Self {
            mpv_bin: mpv_platform::default_mpv_bin(),
            ipc_path: mpv_platform::default_ipc_path(),
            config_dir: None,
            inherit_stdout: false,
        }
    }
}

pub struct MpvIpc {
    valid: Arc<RwLock<bool>>,
    writer: WriteHalf<mpv_platform::Stream>,
    request_id: usize,
    requests: LockedMpvIdMap<oneshot::Sender<anyhow::Result<serde_json::Value>>>,
    event_handlers: Arc<Mutex<HashMap<String, Vec<mpsc::Sender<serde_json::Value>>>>>,
    observers: LockedMpvIdMap<mpsc::Sender<MpvDataOption>>,
    tasks: Vec<JoinHandle<()>>,
}
impl MpvIpc {
    pub async fn connect(ipc_path: &PathBuf) -> anyhow::Result<Self> {
        // Retry before giving up
        let (mut line_reader, writer): (Lines<_>, WriteHalf<_>) = async {
            for n in 0..10 {
                if n > 0 {
                    time::sleep(Duration::from_millis(100) * n).await;
                }
                if let Ok(stream) = mpv_platform::connect(ipc_path).await {
                    debug!("Connected to mpv socket");
                    let (reader, writer) = io::split(stream);
                    let line_reader = BufReader::new(reader).lines();
                    return Ok((line_reader, writer));
                }
            }
            bail!("failed to connect to mpv socket");
        }
        .await?;

        let requests = Arc::new(Mutex::new(HashMap::<
            usize,
            oneshot::Sender<anyhow::Result<serde_json::Value>>,
        >::new()));
        let observers = Arc::new(Mutex::new(HashMap::<usize, mpsc::Sender<MpvDataOption>>::new()));
        let event_handlers = Arc::new(Mutex::new(
            HashMap::<String, Vec<mpsc::Sender<serde_json::Value>>>::new(),
        ));
        let valid = Arc::new(RwLock::new(true));

        let valid_ref = valid.clone();
        let requests_ref = requests.clone();
        let observers_ref = observers.clone();
        let event_handlers_ref = event_handlers.clone();
        let mpv_ipc_task = tokio::spawn(async move {
            loop {
                if let Ok(Some(str)) = line_reader.next_line().await {
                    trace!("<-mpv: {}", str);
                    let json = serde_json::from_str::<serde_json::Value>(str.as_str()).unwrap();
                    if let Ok(mpv_resp) = MpvResponse::deserialize(&json) {
                        if let Some(tx) = requests_ref.lock().await.remove(&mpv_resp.request_id) {
                            if mpv_resp.error == "success" {
                                tx.send(Ok(mpv_resp.data.unwrap_or(serde_json::Value::Null))).unwrap();
                            } else {
                                tx.send(Err(anyhow!(mpv_resp.error))).unwrap();
                            }
                        } else {
                            warn!("Unhandled requests ID {}", mpv_resp.request_id);
                        }
                    } else if let Some(event) = json.as_object().and_then(|j| j.get("event")).and_then(|j| j.as_str()) {
                        trace!("Event '{}'", event);
                        if let Some(list) = event_handlers_ref.lock().await.get(event) {
                            for handler in list {
                                handler.send(json.clone()).await.unwrap();
                            }
                        }
                        if event == "property-change" {
                            let id = json.as_object().unwrap().get("id").unwrap().as_u64().unwrap() as usize;
                            if let Some(tx) = observers_ref.lock().await.get(&id) {
                                let data = json.as_object().unwrap().get("data").map(|d| d.to_owned());
                                tx.send(data).await.unwrap();
                            } else {
                                warn!("Unhandled observable ID {}", id);
                            }
                        }
                        if event == "shutdown" {
                            info!("Received mpv 'shutdown' event.");
                            *valid_ref.write().await = false;
                            break; // stop main loop
                        }
                    } else {
                        warn!("Unhandled mpv message: {}", str);
                    }
                } else {
                    warn!("Failed to read from mpv IPC. Assuming 'shutdown'.");
                    *valid_ref.write().await = false;

                    // Send faked shutdown event
                    if let Some(list) = event_handlers_ref.lock().await.get("shutdown") {
                        for handler in list {
                            handler.send(json!({"event": "shutdown"})).await.unwrap();
                        }
                    }
                    break; // stop main loop
                }
            }
        });

        Ok(Self {
            valid,
            writer,
            request_id: 0,
            requests,
            observers,
            event_handlers,
            tasks: vec![mpv_ipc_task],
        })
    }
    pub async fn spawn(opt: &MpvSpawnOptions) -> anyhow::Result<Self> {
        let mut args = vec![
            "--idle".to_owned(),
            "--input-ipc-server=".to_owned() + &opt.ipc_path.to_string_lossy(),
        ];
        if let Some(config_dir) = &opt.config_dir {
            args.push("--config-dir=".to_owned() + &config_dir.to_string_lossy());
        }
        let stdout_mode = || {
            if opt.inherit_stdout {
                Stdio::inherit()
            } else {
                Stdio::null()
            }
        };
        let child = process::Command::new(&opt.mpv_bin)
            .args(args)
            .stdin(Stdio::null())
            .stdout(stdout_mode())
            .stderr(stdout_mode())
            .spawn()
            .context("Failed to spawn mpv process")?;
        let child_pid = child.id().unwrap();
        info!("mpv spawned! pid: {}", child_pid);

        // Connect
        let mut sself = Self::connect(&opt.ipc_path).await?;

        // Sanity check
        let ipc_pid = sself.get_prop::<u32>("pid").await?;
        if ipc_pid != child_pid {
            warn!("mpv process pid and mpv ipc pid don't match");
        }

        Ok(sself)
    }
    pub async fn valid(&self) -> bool {
        *self.valid.read().await
    }
    pub async fn send_command(&mut self, cmd: serde_json::Value) -> anyhow::Result<serde_json::Value> {
        if !self.valid().await {
            bail!("mpv instance not valid");
        }
        let (tx, rx) = oneshot::channel::<anyhow::Result<serde_json::Value>>();
        self.request_id += 1;
        self.requests.lock().await.insert(self.request_id, tx);
        let str = serde_json::to_string(&MpvCommand {
            request_id: self.request_id,
            command: cmd,
        })
        .unwrap();
        trace!("->mpv: {}", str);
        self.writer.write_all((str + "\n").as_bytes()).await.unwrap();
        rx.await.unwrap()
    }
    pub async fn get_prop<T: DeserializeOwned>(&mut self, name: &str) -> anyhow::Result<T> {
        self.send_command(json!(["get_property", name]))
            .await
            .and_then(|json| T::deserialize(json).map_err(|_| anyhow!("failed to deserialize prop")))
    }
    pub async fn set_prop(&mut self, name: &str, value: impl Serialize) -> anyhow::Result<()> {
        self.send_command(json!(["set_property", name, value]))
            .await
            .map(|_| ())
    }
    pub async fn watch_event<A, F, Fut>(
        &mut self,
        name: impl AsRef<str> + 'static + Send + Sync + Serialize + Display,
        callback: F,
    ) where
        for<'a> Fut: Future<Output = A> + Send + 'a,
        for<'a> F: (Fn(serde_json::Value) -> Fut) + Send + 'a,
    {
        let (json_tx, mut json_rx) = mpsc::channel::<serde_json::Value>(1);
        let enable = {
            let mut event_handlers = self.borrow_mut().event_handlers.lock().await;
            if let Some(list) = event_handlers.get_mut(name.as_ref()) {
                list.push(json_tx);
                false
            } else {
                _ = event_handlers.insert(name.to_string(), vec![json_tx]);
                true
            }
        };
        if enable {
            self.send_command(json!(["enable_event", name])).await.unwrap();
        }
        self.tasks.push(tokio::spawn(async move {
            loop {
                let json = json_rx.recv().await.unwrap();
                trace!("Got watched event value '{}': {:?}", name, json);
                callback(json).await;
            }
        }));
    }
    pub async fn observe_prop<T: 'static + Send + Sync + Clone + DeserializeOwned>(
        &mut self,
        name: impl AsRef<str> + 'static + Send + Sync + Serialize + Display,
        default: T,
    ) -> watch::Receiver<T> {
        // Create observer
        self.request_id += 1;
        let id = self.request_id;
        let (json_tx, mut json_rx) = mpsc::channel::<MpvDataOption>(10);
        self.observers.lock().await.insert(id, json_tx);
        self.send_command(json!(["observe_property", id, name])).await.unwrap();

        // Create converter
        let init_val = self.get_prop(name.as_ref()).await.unwrap_or_else(|_| default.clone());
        let (t_tx, t_rx) = watch::channel::<T>(init_val);
        self.tasks.push(tokio::spawn(async move {
            loop {
                if let Some(json) = json_rx.recv().await.unwrap() {
                    trace!("Got observed value '{}': {}", name, json);
                    if let Ok(val) = T::deserialize(&json) {
                        _ = t_tx.send(val);
                    } else {
                        warn!("Failed to deserialize observable '{}'. Using default.", name);
                        _ = t_tx.send(default.clone());
                    }
                } else {
                    debug!("Observable '{}' updated without a value. Using default.", name);
                    _ = t_tx.send(default.clone());
                }
            }
        }));
        t_rx
    }
}
impl Drop for MpvIpc {
    fn drop(&mut self) {
        tokio::task::block_in_place(move || {
            tokio::runtime::Handle::current().block_on(async {
                _ = self.send_command(json!(["quit"])).await;
                for handle in &self.tasks {
                    handle.abort();
                }
                self.tasks.clear();
                _ = self.writer.shutdown().await;
            });
        });
    }
}
