use anyhow::{Context, Result};
use std::process::Stdio;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use async_trait::async_trait;

#[cfg(test)]
use mockall::automock;

#[async_trait]
#[cfg_attr(test, automock)]
pub trait RemoteApi {
    async fn sync_remote(&self, host: &str, remote_path: &str, local_path: &str) -> Result<()>;
}

pub struct RemoteCommand;

impl RemoteCommand {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl RemoteApi for RemoteCommand {
    async fn sync_remote(&self, host: &str, remote_path: &str, local_path: &str) -> Result<()> {
        log::debug!("syncing remote {}:{} to {}", host, remote_path, local_path);

        let source_path = if host.is_empty() {
            format!("{}/", remote_path)
        } else {
            format!("{}:{}/", host, remote_path)
        };

        let mut cmd = Command::new("rsync")
            .args(["-az", "--delete", "--chown=root:root", &source_path, &format!("{}/", local_path)])
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_sync_remote_success() {
        // Create test directories
        let local_dir = TempDir::new().unwrap();
        let remote_dir = TempDir::new().unwrap();

        // Create a test file in remote dir
        fs::write(remote_dir.path().join("test.txt"), "test content").unwrap();

        let remote = RemoteCommand::new();
        let result = remote.sync_remote(
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
        let remote = RemoteCommand::new();
        let result = remote.sync_remote("nonexistent-host", "/nonexistent/path", "/tmp/nonexistent").await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("rsync failed"));
    }
}
