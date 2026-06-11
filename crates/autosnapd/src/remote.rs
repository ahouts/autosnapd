use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use autosnapd_core::{
    Snapshot, SourceConfig, remote_receive_args, remote_receive_resume_token_args,
    remote_snapshot_list_args,
};
use std::collections::HashSet;
use std::path::PathBuf;
use std::process::{ExitStatus, Stdio};
use std::str::FromStr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

#[cfg(test)]
use mockall::automock;

#[async_trait]
#[cfg_attr(test, automock)]
pub trait SourceApi {
    async fn run_source(&self, source: &SourceConfig) -> Result<()>;
}

#[async_trait]
#[cfg_attr(test, automock)]
pub trait ReplicationApi {
    async fn replicate_snapshots(
        &self,
        local_volume: &str,
        remote_host: &str,
        remote_dataset: &str,
        snapshots: &[Snapshot],
    ) -> Result<()>;
}

pub struct RemoteCommand {
    zfs_path: PathBuf,
}

impl RemoteCommand {
    pub fn new(zfs_path: PathBuf) -> Self {
        Self { zfs_path }
    }
}

impl RemoteCommand {
    async fn sync_remote(&self, host: &str, remote_path: &str, local_path: &str) -> Result<()> {
        log::debug!("syncing remote {}:{} to {}", host, remote_path, local_path);

        let source_path = if host.is_empty() {
            format!("{}/", remote_path)
        } else {
            format!("{}:{}/", host, remote_path)
        };

        let mut cmd = Command::new("rsync")
            .args([
                "-az",
                "--delete",
                "--chown=root:root",
                &source_path,
                &format!("{}/", local_path),
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("failed to execute rsync from {}:{}", host, remote_path))?;

        let mut stderr = String::new();
        cmd.stderr
            .as_mut()
            .unwrap()
            .read_to_string(&mut stderr)
            .await
            .with_context(|| "failed to read stderr")?;

        let status = cmd.wait().await?;

        if !status.success() {
            return Err(anyhow::anyhow!(
                "rsync failed with exit code {} and error:\n{}",
                status.code().unwrap_or(-1),
                stderr.trim()
            ));
        }

        Ok(())
    }

    async fn run_script(&self, path: &str, args: &[impl AsRef<str>]) -> Result<()> {
        log::debug!("running source script {} with {} args", path, args.len());

        let mut cmd = Command::new(path);
        for arg in args {
            cmd.arg(arg.as_ref());
        }

        let mut cmd = cmd
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("failed to execute source script {}", path))?;

        let mut stderr = String::new();
        cmd.stderr
            .as_mut()
            .unwrap()
            .read_to_string(&mut stderr)
            .await
            .with_context(|| "failed to read source script stderr")?;

        let status = cmd.wait().await?;

        if !status.success() {
            return Err(anyhow!(
                "source script {} failed with exit code {} and error:\n{}",
                path,
                status.code().unwrap_or(-1),
                stderr.trim()
            ));
        }

        Ok(())
    }
}

#[async_trait]
impl SourceApi for RemoteCommand {
    async fn run_source(&self, source: &SourceConfig) -> Result<()> {
        match source {
            SourceConfig::Remote(remote) => {
                self.sync_remote(&remote.host, &remote.remote_path, &remote.local_path)
                    .await
            }
            SourceConfig::Script(script) => self.run_script(&script.path, &script.args).await,
        }
    }
}

#[async_trait]
impl ReplicationApi for RemoteCommand {
    async fn replicate_snapshots(
        &self,
        local_volume: &str,
        remote_host: &str,
        remote_dataset: &str,
        snapshots: &[Snapshot],
    ) -> Result<()> {
        log::debug!(
            "replicating {} snapshots from {} to {}:{}",
            snapshots.len(),
            local_volume,
            remote_host,
            remote_dataset
        );

        if let Some(token) = self
            .remote_receive_resume_token(remote_host, remote_dataset)
            .await
            .with_context(|| {
                format!(
                    "error getting receive resume token for {}:{}",
                    remote_host, remote_dataset
                )
            })?
        {
            self.send_resume_token(remote_host, remote_dataset, &token)
                .await
                .with_context(|| {
                    format!(
                        "error resuming replication to {}:{}",
                        remote_host, remote_dataset
                    )
                })?;
        }

        let remote_snapshots = self
            .remote_snapshots(remote_host, remote_dataset)
            .await
            .with_context(|| {
                format!(
                    "error listing remote snapshots for {}:{}",
                    remote_host, remote_dataset
                )
            })?;

        for (parent, snapshot) in replication_plan(snapshots, &remote_snapshots) {
            self.send_snapshot(remote_host, remote_dataset, parent.as_ref(), &snapshot)
                .await
                .with_context(|| {
                    format!(
                        "error sending snapshot {} to {}:{}",
                        snapshot, remote_host, remote_dataset
                    )
                })?;
        }

        Ok(())
    }
}

impl RemoteCommand {
    async fn remote_snapshots(
        &self,
        remote_host: &str,
        remote_dataset: &str,
    ) -> Result<Vec<Snapshot>> {
        let remote_args = remote_snapshot_list_args(remote_dataset)?;
        let cmd = Command::new("ssh")
            .arg(remote_host)
            .args(remote_args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("failed to execute ssh {}", remote_host))?;

        let (status, stdout, stderr) = wait_with_piped_output(cmd)
            .await
            .with_context(|| "failed to read remote zfs list output")?;
        if !status.success() {
            if is_dataset_missing_error(remote_dataset, &stderr) {
                log::debug!(
                    "remote dataset {}:{} does not exist, treating remote snapshot list as empty",
                    remote_host,
                    remote_dataset
                );
                return Ok(Vec::new());
            }

            return Err(anyhow!(
                "remote zfs list failed with exit code {} and error:\n{}",
                status.code().unwrap_or(-1),
                stderr.trim()
            ));
        }

        Ok(stdout
            .lines()
            .filter_map(|line| match Snapshot::from_str(line.trim()) {
                Ok(snapshot) if snapshot.is_valid() => Some(snapshot),
                Ok(snapshot) => {
                    log::warn!("remote snapshot is not valid, ignoring: {}", snapshot);
                    None
                }
                Err(e) => {
                    log::trace!("error parsing remote snapshot: {}", e);
                    None
                }
            })
            .collect())
    }

    async fn remote_receive_resume_token(
        &self,
        remote_host: &str,
        remote_dataset: &str,
    ) -> Result<Option<String>> {
        let remote_args = remote_receive_resume_token_args(remote_dataset)?;
        let cmd = Command::new("ssh")
            .arg(remote_host)
            .args(remote_args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("failed to execute ssh {}", remote_host))?;

        let (status, stdout, stderr) = wait_with_piped_output(cmd)
            .await
            .with_context(|| "failed to read remote zfs receive_resume_token output")?;
        if !status.success() {
            if is_dataset_missing_error(remote_dataset, &stderr) {
                log::debug!(
                    "remote dataset {}:{} does not exist, treating receive resume token as absent",
                    remote_host,
                    remote_dataset
                );
                return Ok(None);
            }

            return Err(anyhow!(
                "remote zfs get receive_resume_token failed with exit code {} and error:\n{}",
                status.code().unwrap_or(-1),
                stderr.trim()
            ));
        }

        Ok(parse_receive_resume_token(&stdout))
    }

    async fn send_snapshot(
        &self,
        remote_host: &str,
        remote_dataset: &str,
        parent: Option<&Snapshot>,
        snapshot: &Snapshot,
    ) -> Result<()> {
        match parent {
            Some(parent) => log::info!(
                "incrementally sending snapshot {} from {} to {}:{}",
                snapshot,
                parent,
                remote_host,
                remote_dataset
            ),
            None => log::info!(
                "fully sending snapshot {} to {}:{}",
                snapshot,
                remote_host,
                remote_dataset
            ),
        }

        self.send_stream(
            remote_host,
            remote_dataset,
            SendRequest::Snapshot { parent, snapshot },
        )
        .await
    }

    async fn send_resume_token(
        &self,
        remote_host: &str,
        remote_dataset: &str,
        token: &str,
    ) -> Result<()> {
        log::info!(
            "resuming interrupted zfs receive to {}:{}",
            remote_host,
            remote_dataset
        );

        self.send_stream(remote_host, remote_dataset, SendRequest::ResumeToken(token))
            .await
    }

    async fn send_stream(
        &self,
        remote_host: &str,
        remote_dataset: &str,
        request: SendRequest<'_>,
    ) -> Result<()> {
        let send_args = zfs_send_args(request);
        let mut send_cmd = Command::new(&self.zfs_path);
        let mut send = send_cmd
            .args(&send_args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("failed to execute zfs {:?}", send_args))?;

        let remote_args = remote_receive_args(remote_dataset)?;
        let mut receive = Command::new("ssh")
            .arg(remote_host)
            .args(remote_args)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("failed to execute remote zfs receive on {}", remote_host))?;

        let mut send_stdout = send.stdout.take().unwrap();
        let mut send_err = send.stderr.take().unwrap();
        let mut receive_stdin = receive.stdin.take().unwrap();
        let mut receive_err = receive.stderr.take().unwrap();
        let mut send_stderr = String::new();
        let mut receive_stderr = String::new();
        let (result, _, _) = tokio::join!(
            async {
                let result = tokio::try_join!(
                    async {
                        let result = tokio::io::copy(&mut send_stdout, &mut receive_stdin).await;
                        receive_stdin.shutdown().await?;
                        result
                    },
                    async { send.wait().await },
                    async { receive.wait().await }
                )
                .context("piping snapshot over SSH");
                let _ = send.start_kill();
                let _ = receive.start_kill();
                result
            },
            async { send_err.read_to_string(&mut send_stderr).await },
            async { receive_err.read_to_string(&mut receive_stderr).await },
        );

        let (bytes_sent, send_status, receive_status) = result.with_context(|| {
            format!("send stderr: {send_stderr}\nrecv stderr: {receive_stderr}")
        })?;

        log::debug!("sent {bytes_sent} bytes");

        if !send_status.success() {
            return Err(anyhow!(
                "zfs send failed with exit code {} and error:\n{}",
                send_status.code().unwrap_or(-1),
                send_stderr.trim()
            ));
        }

        if !receive_status.success() {
            return Err(anyhow!(
                "remote zfs receive failed with exit code {} and error:\n{}",
                receive_status.code().unwrap_or(-1),
                receive_stderr.trim()
            ));
        }

        Ok(())
    }
}

async fn wait_with_piped_output(
    mut cmd: tokio::process::Child,
) -> Result<(ExitStatus, String, String)> {
    let mut stdout = cmd
        .stdout
        .take()
        .ok_or_else(|| anyhow!("child stdout was not piped"))?;
    let mut stderr = cmd
        .stderr
        .take()
        .ok_or_else(|| anyhow!("child stderr was not piped"))?;

    let mut stdout_buf = String::new();
    let mut stderr_buf = String::new();
    let (stdout_result, stderr_result) = tokio::join!(
        stdout.read_to_string(&mut stdout_buf),
        stderr.read_to_string(&mut stderr_buf)
    );
    stdout_result.with_context(|| "failed to read child stdout")?;
    stderr_result.with_context(|| "failed to read child stderr")?;

    let status = cmd.wait().await?;

    Ok((status, stdout_buf, stderr_buf))
}

enum SendRequest<'a> {
    Snapshot {
        parent: Option<&'a Snapshot>,
        snapshot: &'a Snapshot,
    },
    ResumeToken(&'a str),
}

fn zfs_send_args(request: SendRequest<'_>) -> Vec<String> {
    match request {
        SendRequest::Snapshot { parent, snapshot } => {
            let mut args = vec![String::from("send"), String::from("-w")];
            if let Some(parent) = parent {
                args.push(String::from("-i"));
                args.push(parent.to_string());
            }
            args.push(snapshot.to_string());
            args
        }
        SendRequest::ResumeToken(token) => {
            vec![String::from("send"), String::from("-t"), token.to_string()]
        }
    }
}

fn parse_receive_resume_token(output: &str) -> Option<String> {
    let token = output.trim();
    if token.is_empty() || token == "-" {
        None
    } else {
        Some(token.to_string())
    }
}

fn is_dataset_missing_error(dataset: &str, stderr: &str) -> bool {
    stderr
        .lines()
        .any(|line| line.trim() == format!("cannot open '{}': dataset does not exist", dataset))
}

fn snapshot_key(snapshot: &Snapshot) -> String {
    format!(
        "{}_{}_{}",
        snapshot.prefix, snapshot.date_time, snapshot.time_unit
    )
}

fn replication_plan(local: &[Snapshot], remote: &[Snapshot]) -> Vec<(Option<Snapshot>, Snapshot)> {
    let remote_snapshot_keys = remote.iter().map(snapshot_key).collect::<HashSet<_>>();
    let latest_common_index = local
        .iter()
        .rposition(|snapshot| remote_snapshot_keys.contains(&snapshot_key(snapshot)));

    let mut parent = latest_common_index.map(|index| local[index].clone());
    let start_index = latest_common_index.map(|index| index + 1).unwrap_or(0);
    let mut plan = Vec::new();

    for snapshot in local[start_index..]
        .iter()
        .filter(|snapshot| !remote_snapshot_keys.contains(&snapshot_key(snapshot)))
    {
        plan.push((parent.clone(), snapshot.clone()));
        parent = Some(snapshot.clone());
    }

    plan
}

pub struct DryReplicationApi<A: ReplicationApi>(pub A);

pub struct DrySourceApi<A: SourceApi>(pub A);

#[async_trait]
impl<A: SourceApi + Send + Sync> SourceApi for DrySourceApi<A> {
    async fn run_source(&self, source: &SourceConfig) -> Result<()> {
        log::info!("not running source, dry run: {:?}", source);
        Ok(())
    }
}

#[async_trait]
impl<A: ReplicationApi + Send + Sync> ReplicationApi for DryReplicationApi<A> {
    async fn replicate_snapshots(
        &self,
        local_volume: &str,
        remote_host: &str,
        remote_dataset: &str,
        snapshots: &[Snapshot],
    ) -> Result<()> {
        log::info!(
            "not replicating {} snapshots from {} to {}:{}, dry run",
            snapshots.len(),
            local_volume,
            remote_host,
            remote_dataset
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use autosnapd_core::TimeUnit;
    use chrono::{NaiveDate, TimeZone, Utc};
    use std::fs;
    use std::path::PathBuf;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_sync_remote_success() {
        // Create test directories
        let local_dir = TempDir::new().unwrap();
        let remote_dir = TempDir::new().unwrap();

        // Create a test file in remote dir
        fs::write(remote_dir.path().join("test.txt"), "test content").unwrap();

        let remote = RemoteCommand::new(PathBuf::from("zfs"));
        let result = remote
            .sync_remote(
                "", // Empty host for local sync
                remote_dir.path().to_str().unwrap(),
                local_dir.path().to_str().unwrap(),
            )
            .await;

        assert!(result.is_ok());
        assert!(local_dir.path().join("test.txt").exists());

        // Verify content
        let content = fs::read_to_string(local_dir.path().join("test.txt")).unwrap();
        assert_eq!(content, "test content");
    }

    #[tokio::test]
    async fn test_sync_remote_failure() {
        let remote = RemoteCommand::new(PathBuf::from("zfs"));
        let result = remote
            .sync_remote("nonexistent-host", "/nonexistent/path", "/tmp/nonexistent")
            .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("rsync failed"));
    }

    #[tokio::test]
    async fn test_run_script_success() {
        let remote = RemoteCommand::new(PathBuf::from("zfs"));
        let result = remote
            .run_script(
                "sh",
                &["-c", "test \"$1\" = expected", "script-name", "expected"],
            )
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_run_script_failure() {
        let remote = RemoteCommand::new(PathBuf::from("zfs"));
        let result = remote
            .run_script("sh", &["-c", "echo script failed >&2; exit 7"])
            .await;

        let error = result.unwrap_err().to_string();
        assert!(error.contains("source script sh failed with exit code 7"));
        assert!(error.contains("script failed"));
    }

    #[test]
    fn snapshot_key_ignores_volume() {
        let local = Snapshot::from_str("tank/data@autosnap_2021-06-14T03:21:01Z_hourly").unwrap();
        let remote =
            Snapshot::from_str("backup/tank/data@autosnap_2021-06-14T03:21:01Z_hourly").unwrap();

        assert_eq!(snapshot_key(&local), snapshot_key(&remote));
    }

    #[test]
    fn replication_plan_sends_full_then_incremental_when_no_common_parent_exists() {
        let first = test_snapshot("tank/data", 1);
        let second = test_snapshot("tank/data", 2);

        assert_eq!(
            vec![(None, first.clone()), (Some(first), second.clone())],
            replication_plan(&[test_snapshot("tank/data", 1), second], &[])
        );
    }

    #[test]
    fn replication_plan_sends_only_after_latest_common_parent() {
        let first = test_snapshot("tank/data", 1);
        let second = test_snapshot("tank/data", 2);
        let third = test_snapshot("tank/data", 3);
        let remote_first = test_snapshot("backup/tank/data", 1);
        let remote_third = test_snapshot("backup/tank/data", 3);

        assert_eq!(
            Vec::<(Option<Snapshot>, Snapshot)>::new(),
            replication_plan(&[first, second, third], &[remote_first, remote_third])
        );
    }

    #[test]
    fn replication_plan_uses_latest_common_parent_for_new_snapshots() {
        let first = test_snapshot("tank/data", 1);
        let second = test_snapshot("tank/data", 2);
        let third = test_snapshot("tank/data", 3);
        let remote_first = test_snapshot("backup/tank/data", 1);

        assert_eq!(
            vec![
                (Some(first), second.clone()),
                (Some(second.clone()), third.clone())
            ],
            replication_plan(
                &[test_snapshot("tank/data", 1), second, third],
                &[remote_first]
            )
        );
    }

    #[test]
    fn replication_plan_preserves_input_order_for_tied_snapshot_times() {
        let first = test_snapshot_with_unit("tank/data", 1, TimeUnit::Day);
        let second = test_snapshot_with_unit("tank/data", 1, TimeUnit::Hour);

        assert_eq!(
            vec![(None, first.clone()), (Some(first), second.clone())],
            replication_plan(
                &[
                    test_snapshot_with_unit("tank/data", 1, TimeUnit::Day),
                    second
                ],
                &[]
            )
        );
    }

    #[test]
    fn zfs_send_args_builds_full_send() {
        let snapshot = test_snapshot("tank/data", 1);

        assert_eq!(
            vec![
                "send".to_string(),
                "-w".to_string(),
                "tank/data@autosnap_2021-06-14T03:01:01Z_hourly".to_string()
            ],
            zfs_send_args(SendRequest::Snapshot {
                parent: None,
                snapshot: &snapshot
            })
        );
    }

    #[test]
    fn zfs_send_args_builds_incremental_send() {
        let parent = test_snapshot("tank/data", 1);
        let snapshot = test_snapshot("tank/data", 2);

        assert_eq!(
            vec![
                "send".to_string(),
                "-w".to_string(),
                "-i".to_string(),
                "tank/data@autosnap_2021-06-14T03:01:01Z_hourly".to_string(),
                "tank/data@autosnap_2021-06-14T03:02:01Z_hourly".to_string()
            ],
            zfs_send_args(SendRequest::Snapshot {
                parent: Some(&parent),
                snapshot: &snapshot
            })
        );
    }

    #[test]
    fn zfs_send_args_builds_resume_send() {
        // Resume tokens encode the interrupted stream; resumability is created
        // by zfs receive -s and continued with zfs send -t <token>.
        assert_eq!(
            vec![
                "send".to_string(),
                "-t".to_string(),
                "resume-token".to_string()
            ],
            zfs_send_args(SendRequest::ResumeToken("resume-token"))
        );
    }

    #[test]
    fn zfs_receive_args_uses_resumable_unmounted_receive() {
        assert_eq!(
            vec![
                "zfs".to_string(),
                "receive".to_string(),
                "-s".to_string(),
                "-u".to_string(),
                "backup/tank/data".to_string()
            ],
            remote_receive_args("backup/tank/data").unwrap()
        );
    }

    #[test]
    fn parse_receive_resume_token_handles_missing_tokens() {
        assert_eq!(None, parse_receive_resume_token("-\n"));
        assert_eq!(None, parse_receive_resume_token("\n"));
        assert_eq!(None, parse_receive_resume_token("   \n"));
    }

    #[test]
    fn parse_receive_resume_token_returns_token() {
        assert_eq!(
            Some("1-token-value".to_string()),
            parse_receive_resume_token(" 1-token-value\n")
        );
    }

    #[test]
    fn is_dataset_missing_error_matches_requested_dataset() {
        assert!(is_dataset_missing_error(
            "vol/abc",
            "cannot open 'vol/abc': dataset does not exist\n"
        ));
    }

    #[test]
    fn is_dataset_missing_error_ignores_other_datasets() {
        assert!(!is_dataset_missing_error(
            "vol/abc",
            "cannot open 'vol/def': dataset does not exist\n"
        ));
    }

    #[test]
    fn is_dataset_missing_error_ignores_unrelated_errors() {
        assert!(!is_dataset_missing_error(
            "vol/abc",
            "cannot open 'vol/abc': permission denied\n"
        ));
    }

    fn test_snapshot(volume: &str, minute: u32) -> Snapshot {
        test_snapshot_with_unit(volume, minute, TimeUnit::Hour)
    }

    fn test_snapshot_with_unit(volume: &str, minute: u32, time_unit: TimeUnit) -> Snapshot {
        Snapshot {
            volume: volume.into(),
            prefix: "autosnap".into(),
            date_time: Utc.from_utc_datetime(
                &NaiveDate::from_ymd_opt(2021, 6, 14)
                    .unwrap()
                    .and_hms_opt(3, minute, 1)
                    .unwrap(),
            ),
            time_unit,
        }
    }
}
