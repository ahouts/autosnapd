use anyhow::{Context, Result, anyhow};
use autosnapd_core::{RemoteZfsCommand, parse_forced_remote_zfs_command};
use std::env;
use std::process::{Command, ExitCode, Stdio};

const ZFS_PATH: &str = "/usr/bin/zfs";

fn main() -> ExitCode {
    match run_from_env() {
        Ok(code) => ExitCode::from(code),
        Err(e) => {
            eprintln!("autosnapd-ssh-command: {e}");
            ExitCode::from(2)
        }
    }
}

fn run_from_env() -> Result<u8> {
    let command = env::var("SSH_ORIGINAL_COMMAND").context("SSH_ORIGINAL_COMMAND is not set")?;
    let command = parse_forced_remote_zfs_command(&command)?;
    run_command(&command)
}

fn run_command(command: &RemoteZfsCommand) -> Result<u8> {
    let mut child = Command::new("sudo")
        .args(sudo_args(command))
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .context("failed to execute sudo")?;

    let status = child.wait().context("failed to wait for sudo")?;
    Ok(status.code().unwrap_or(1).try_into().unwrap_or(1))
}

fn sudo_args(command: &RemoteZfsCommand) -> Vec<String> {
    let mut args = vec![
        String::from("-n"),
        String::from("--"),
        String::from(ZFS_PATH),
    ];
    args.extend(command.zfs_args());
    args
}

#[allow(dead_code)]
fn command_from_original(original: Option<&str>) -> Result<RemoteZfsCommand> {
    let original = original.ok_or_else(|| anyhow!("SSH_ORIGINAL_COMMAND is not set"))?;
    parse_forced_remote_zfs_command(original)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_list_command() {
        let command =
            command_from_original(Some("zfs list -H -o name -t snapshot backup/tank/data"))
                .unwrap();

        assert_eq!(
            sudo_args(&command),
            vec![
                "-n",
                "--",
                "/usr/bin/zfs",
                "list",
                "-H",
                "-o",
                "name",
                "-t",
                "snapshot",
                "backup/tank/data"
            ]
        );
    }

    #[test]
    fn parses_valid_receive_command() {
        let command = command_from_original(Some("zfs receive -s -u backup/tank/data")).unwrap();

        assert_eq!(
            sudo_args(&command),
            vec![
                "-n",
                "--",
                "/usr/bin/zfs",
                "receive",
                "-s",
                "-u",
                "backup/tank/data"
            ]
        );
    }

    #[test]
    fn parses_valid_destroy_command() {
        let command = command_from_original(Some(
            "zfs destroy backup/tank/data@autosnap_2021-06-14T03:21:01Z_hourly",
        ))
        .unwrap();

        assert_eq!(
            sudo_args(&command),
            vec![
                "-n",
                "--",
                "/usr/bin/zfs",
                "destroy",
                "backup/tank/data@autosnap_2021-06-14T03:21:01Z_hourly"
            ]
        );
    }

    #[test]
    fn rejects_missing_env() {
        assert!(command_from_original(None).is_err());
    }

    #[test]
    fn rejects_invalid_command() {
        assert!(command_from_original(Some("zfs destroy tank/data@manual")).is_err());
        assert!(command_from_original(Some("sh -c true")).is_err());
    }
}
