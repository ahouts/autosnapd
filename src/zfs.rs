use std::path::PathBuf;
use std::process::Stdio;
use std::str::FromStr;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
#[cfg(test)]
use mockall::automock;
use tokio::io::{AsyncBufReadExt, AsyncReadExt};
use tokio::{io::BufReader, process::Command};

use crate::snapshot::Snapshot;
use futures::Future;
use std::pin::Pin;
use tokio::process::ChildStdout;

#[async_trait]
#[cfg_attr(test, automock)]
pub trait ZfsApi {
    async fn snapshots(&self, volume: &str) -> Result<Vec<Snapshot>>;

    async fn take_snapshot(&self, snapshot: &Snapshot) -> Result<()>;

    async fn remove_snapshot(&self, snapshot: &Snapshot) -> Result<()>;
}

pub struct ZfsApiImpl {
    zfs_path: PathBuf,
}

impl ZfsApiImpl {
    pub fn new(zfs_path: PathBuf) -> Self {
        ZfsApiImpl { zfs_path }
    }
}

#[async_trait]
impl ZfsApi for ZfsApiImpl {
    async fn snapshots(&self, volume: &str) -> Result<Vec<Snapshot>> {
        exec(
            &self.zfs_path,
            &["list", "-o", "name", "-t", "snapshot", volume],
            |stdout| {
                Box::pin(async {
                    let mut results = Vec::new();
                    let mut lines = BufReader::new(stdout).lines();
                    let mut first_line = true;

                    while let Some(line) = lines
                        .next_line()
                        .await
                        .with_context(|| "error reading output from zfs list")?
                    {
                        if first_line {
                            first_line = false;
                            continue;
                        }
                        match Snapshot::from_str(line.trim()) {
                            Ok(snapshot) => {
                                if snapshot.is_valid() {
                                    results.push(snapshot);
                                } else {
                                    log::warn!("snapshot is not valid, ignoring: {}", snapshot);
                                }
                            }
                            Err(e) => log::trace!("error parsing snapshot: {}", e),
                        }
                    }

                    Ok(results) as Result<Vec<Snapshot>>
                })
            },
        )
        .await?
    }

    async fn take_snapshot(&self, snapshot: &Snapshot) -> Result<()> {
        log::info!("taking snapshot: {}", snapshot);

        exec(
            &self.zfs_path,
            &["snapshot", format!("{}", snapshot).as_str()],
            |_| Box::pin(async {}),
        )
        .await
    }

    async fn remove_snapshot(&self, snapshot: &Snapshot) -> Result<()> {
        log::info!("removing snapshot: {}", snapshot);

        exec(
            &self.zfs_path,
            &["destroy", format!("{}", snapshot).as_str()],
            |_| Box::pin(async {}),
        )
        .await
    }
}

async fn exec<
    T: Send + 'static,
    F: FnOnce(ChildStdout) -> Pin<Box<dyn Future<Output = T> + Send>>,
>(
    zfs_path: &PathBuf,
    args: &[&str],
    f: F,
) -> Result<T> {
    let mut proc = Command::new(zfs_path)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("error spawning zfs {:?}", args))?;

    let mut proc_stdout = None;
    std::mem::swap(&mut proc.stdout, &mut proc_stdout);

    let handle = tokio::spawn(f(proc_stdout.unwrap()));

    let mut error_buff = Vec::new();
    proc.stderr
        .as_mut()
        .unwrap()
        .read_to_end(&mut error_buff)
        .await
        .with_context(|| format!("error reading error from zfs {:?}", args))?;

    let result = handle.await;

    let exit_status = proc
        .wait()
        .await
        .with_context(|| format!("error invoking zfs {:?}", args))?;
    if !exit_status.success() {
        let error_text = String::from_utf8_lossy(error_buff.as_slice());
        return Err(anyhow!(
            "bad exit code from zfs {:?}: {}\n{}",
            args,
            exit_status.code().unwrap_or(0),
            error_text
        ));
    }

    Ok(result.with_context(|| format!("error handling output from zfs {:?}", args))?)
}

pub struct DryZfsApi<A: ZfsApi>(pub A);

#[async_trait]
impl<A: ZfsApi + Send + Sync> ZfsApi for DryZfsApi<A> {
    async fn snapshots(&self, volume: &str) -> Result<Vec<Snapshot>> {
        self.0.snapshots(volume).await
    }

    async fn take_snapshot(&self, snapshot: &Snapshot) -> Result<()> {
        log::info!("not taking snapshot, dry run: {}", snapshot);
        Ok(())
    }

    async fn remove_snapshot(&self, snapshot: &Snapshot) -> Result<()> {
        log::info!("not removing snapshot, dry run: {}", snapshot);
        Ok(())
    }
}
