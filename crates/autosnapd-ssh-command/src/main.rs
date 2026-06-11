use anyhow::{Context, Result, anyhow};
use autosnapd_core::{
    Config, RemoteZfsCommand, Snapshot, is_dataset_missing_error, load_config,
    parse_forced_remote_zfs_command, parse_receive_resume_token, validate_replica_target,
    validate_snapshot_spacing, validate_snapshot_timestamp,
};
use chrono::Utc;
use std::env;
use std::path::PathBuf;
use std::process::{Command, ExitCode, Output, Stdio};
use std::str::FromStr;
use structopt::StructOpt;

const ZFS_PATH: &str = "/usr/bin/zfs";

#[derive(StructOpt)]
#[structopt(
    name = "autosnapd-ssh-command",
    about = "forced ssh command for autosnapd replication"
)]
struct Opt {
    #[structopt(short, long, default_value = "/etc/autosnapd.toml", parse(from_os_str))]
    config: PathBuf,
}

fn main() -> ExitCode {
    match run_from_env() {
        Ok(code) => ExitCode::from(code),
        Err(e) => {
            eprintln!("autosnapd-ssh-command: {e:#}");
            ExitCode::from(2)
        }
    }
}

fn run_from_env() -> Result<u8> {
    let opt: Opt = Opt::from_args();
    let original = env::var("SSH_ORIGINAL_COMMAND").context("SSH_ORIGINAL_COMMAND is not set")?;
    let command = parse_forced_remote_zfs_command(&original)?;

    let config_data = std::fs::read_to_string(&opt.config)
        .with_context(|| format!("error reading configuration file {}", opt.config.display()))?;
    let config = load_config(&config_data)?;

    validate_command(&command, &config)?;
    run_command(&command)
}

fn validate_command(command: &RemoteZfsCommand, config: &Config) -> Result<()> {
    validate_replica_target(config, command.dataset())?;

    if let RemoteZfsCommand::Receive { dataset, snapshot } = command {
        match snapshot {
            Some(snapshot) => {
                validate_snapshot_timestamp(snapshot, Utc::now())?;
                let existing = list_existing_snapshots(dataset)?;
                validate_snapshot_spacing(snapshot, &existing)?;
            }
            None => {
                // A receive without a snapshot name can only resume an
                // interrupted stream that was validated when it started. There
                // is a window between this check and the receive where the
                // token could be consumed by a concurrent receive, but a fresh
                // stream pushed through it is still constrained by zfs itself
                // (full streams need -F, incrementals need a matching parent).
                if query_resume_token(dataset)?.is_none() {
                    return Err(anyhow!(
                        "receive without a snapshot name is only permitted to resume an interrupted receive into {}",
                        dataset
                    ));
                }
            }
        }
    }

    Ok(())
}

fn list_existing_snapshots(dataset: &str) -> Result<Vec<Snapshot>> {
    let list = RemoteZfsCommand::ListSnapshots {
        dataset: dataset.to_string(),
    };
    let output = run_captured(list.zfs_args())?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if is_dataset_missing_error(dataset, &stderr) {
            return Ok(Vec::new());
        }

        return Err(anyhow!(
            "zfs list failed with exit code {} and error:\n{}",
            output.status.code().unwrap_or(-1),
            stderr.trim()
        ));
    }

    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| Snapshot::from_str(line.trim()).ok())
        .filter(|snapshot| snapshot.is_valid())
        .collect())
}

fn query_resume_token(dataset: &str) -> Result<Option<String>> {
    let get = RemoteZfsCommand::ReceiveResumeToken {
        dataset: dataset.to_string(),
    };
    let output = run_captured(get.zfs_args())?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if is_dataset_missing_error(dataset, &stderr) {
            return Ok(None);
        }

        return Err(anyhow!(
            "zfs get receive_resume_token failed with exit code {} and error:\n{}",
            output.status.code().unwrap_or(-1),
            stderr.trim()
        ));
    }

    Ok(parse_receive_resume_token(&String::from_utf8_lossy(
        &output.stdout,
    )))
}

fn run_captured(zfs_args: Vec<String>) -> Result<Output> {
    // stdin must not be inherited: the session stdin carries the zfs send
    // stream for the receive command that runs after validation.
    Command::new("sudo")
        .args(sudo_args_for(zfs_args))
        .stdin(Stdio::null())
        .output()
        .context("failed to execute sudo")
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
    sudo_args_for(command.zfs_args())
}

fn sudo_args_for(zfs_args: Vec<String>) -> Vec<String> {
    let mut args = vec![
        String::from("-n"),
        String::from("--"),
        String::from(ZFS_PATH),
    ];
    args.extend(zfs_args);
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
    fn parses_valid_named_receive_command() {
        let command = command_from_original(Some(
            "zfs receive -s -u backup/tank/data@autosnap_2021-06-14T03:21:01Z_hourly",
        ))
        .unwrap();

        assert_eq!(
            sudo_args(&command),
            vec![
                "-n",
                "--",
                "/usr/bin/zfs",
                "receive",
                "-s",
                "-u",
                "backup/tank/data@autosnap_2021-06-14T03:21:01Z_hourly"
            ]
        );
    }

    #[test]
    fn parses_plain_receive_command_for_resume() {
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
    fn pre_flight_commands_use_the_same_sudo_invocation() {
        let list = RemoteZfsCommand::ListSnapshots {
            dataset: String::from("backup/tank/data"),
        };

        assert_eq!(
            sudo_args_for(list.zfs_args()),
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
    fn rejects_destroy_command() {
        assert!(
            command_from_original(Some(
                "zfs destroy backup/tank/data@autosnap_2021-06-14T03:21:01Z_hourly",
            ))
            .is_err()
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

    #[test]
    fn validate_command_rejects_unconfigured_datasets() {
        let config = load_config("").unwrap();
        let command =
            command_from_original(Some("zfs list -H -o name -t snapshot backup/tank/data"))
                .unwrap();

        assert!(
            validate_command(&command, &config)
                .unwrap_err()
                .to_string()
                .contains("not a configured volume")
        );
    }

    #[test]
    fn validate_command_rejects_non_prune_only_datasets() {
        let config = load_config(
            r#"
["backup/tank/data"]
daily = 7
        "#,
        )
        .unwrap();
        let command = command_from_original(Some(
            "zfs receive -s -u backup/tank/data@autosnap_2021-06-14T03:21:01Z_hourly",
        ))
        .unwrap();

        assert!(
            validate_command(&command, &config)
                .unwrap_err()
                .to_string()
                .contains("not configured as a prune_only volume")
        );
    }
}
