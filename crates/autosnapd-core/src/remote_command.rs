use crate::CompactString;
use crate::snapshot::Snapshot;
use anyhow::{Result, anyhow};
use std::str::FromStr;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RemoteZfsCommand {
    ListSnapshots {
        dataset: String,
    },
    ReceiveResumeToken {
        dataset: String,
    },
    /// `snapshot: Some(_)` receives a fresh stream into a named snapshot so the
    /// name can be validated before any data lands; `snapshot.volume` always
    /// equals `dataset`. `snapshot: None` is only used to resume an interrupted
    /// (already validated) receive.
    Receive {
        dataset: String,
        snapshot: Option<Snapshot>,
    },
}

impl RemoteZfsCommand {
    pub fn dataset(&self) -> &str {
        match self {
            RemoteZfsCommand::ListSnapshots { dataset } => dataset,
            RemoteZfsCommand::ReceiveResumeToken { dataset } => dataset,
            RemoteZfsCommand::Receive { dataset, .. } => dataset,
        }
    }

    pub fn zfs_args(&self) -> Vec<String> {
        match self {
            RemoteZfsCommand::ListSnapshots { dataset } => {
                vec!["list", "-H", "-o", "name", "-t", "snapshot", dataset]
                    .into_iter()
                    .map(String::from)
                    .collect()
            }
            RemoteZfsCommand::ReceiveResumeToken { dataset } => {
                vec!["get", "-H", "-o", "value", "receive_resume_token", dataset]
                    .into_iter()
                    .map(String::from)
                    .collect()
            }
            RemoteZfsCommand::Receive { dataset, snapshot } => {
                let target = snapshot
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| dataset.clone());
                vec![
                    String::from("receive"),
                    String::from("-s"),
                    String::from("-u"),
                    target,
                ]
            }
        }
    }

    pub fn ssh_args(&self) -> Vec<String> {
        let mut args = vec![String::from("zfs")];
        args.extend(self.zfs_args());
        args
    }
}

pub fn validate_dataset_name(dataset: &str) -> Result<()> {
    if dataset.is_empty() {
        return Err(anyhow!("dataset name is empty"));
    }

    if dataset.starts_with('-') {
        return Err(anyhow!("dataset name starts with '-'"));
    }

    if dataset.contains('@') {
        return Err(anyhow!("dataset name contains '@'"));
    }

    if dataset.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err(anyhow!(
            "dataset name contains whitespace or control characters"
        ));
    }

    Ok(())
}

pub fn remote_snapshot_list_args(dataset: &str) -> Result<Vec<String>> {
    validate_dataset_name(dataset)?;
    Ok(RemoteZfsCommand::ListSnapshots {
        dataset: dataset.to_string(),
    }
    .ssh_args())
}

pub fn remote_receive_resume_token_args(dataset: &str) -> Result<Vec<String>> {
    validate_dataset_name(dataset)?;
    Ok(RemoteZfsCommand::ReceiveResumeToken {
        dataset: dataset.to_string(),
    }
    .ssh_args())
}

pub fn remote_receive_args(dataset: &str) -> Result<Vec<String>> {
    validate_dataset_name(dataset)?;
    Ok(RemoteZfsCommand::Receive {
        dataset: dataset.to_string(),
        snapshot: None,
    }
    .ssh_args())
}

pub fn remote_receive_snapshot_args(remote_dataset: &str, snapshot: &Snapshot) -> Result<Vec<String>> {
    validate_dataset_name(remote_dataset)?;
    let remote_snapshot = Snapshot {
        volume: CompactString::from(remote_dataset),
        ..snapshot.clone()
    };
    if !remote_snapshot.is_valid() {
        return Err(anyhow!(
            "snapshot does not render canonically: {}",
            remote_snapshot
        ));
    }
    Ok(RemoteZfsCommand::Receive {
        dataset: remote_dataset.to_string(),
        snapshot: Some(remote_snapshot),
    }
    .ssh_args())
}

pub fn parse_forced_remote_zfs_command(command: &str) -> Result<RemoteZfsCommand> {
    let argv = command.split_whitespace().collect::<Vec<_>>();
    if argv.is_empty() {
        return Err(anyhow!("empty command"));
    }

    if argv[0] != "zfs" {
        return Err(anyhow!("unsupported command"));
    }

    match argv.as_slice() {
        ["zfs", "list", "-H", "-o", "name", "-t", "snapshot", dataset] => {
            validate_dataset_name(dataset)?;
            Ok(RemoteZfsCommand::ListSnapshots {
                dataset: (*dataset).to_string(),
            })
        }
        [
            "zfs",
            "get",
            "-H",
            "-o",
            "value",
            "receive_resume_token",
            dataset,
        ] => {
            validate_dataset_name(dataset)?;
            Ok(RemoteZfsCommand::ReceiveResumeToken {
                dataset: (*dataset).to_string(),
            })
        }
        ["zfs", "receive", "-s", "-u", target] => {
            if target.contains('@') {
                let snapshot = Snapshot::from_str(target)?;
                if snapshot.to_string() != *target {
                    return Err(anyhow!("snapshot name is not canonical: {}", target));
                }
                Ok(RemoteZfsCommand::Receive {
                    dataset: snapshot.volume.to_string(),
                    snapshot: Some(snapshot),
                })
            } else {
                validate_dataset_name(target)?;
                Ok(RemoteZfsCommand::Receive {
                    dataset: (*target).to_string(),
                    snapshot: None,
                })
            }
        }
        _ => Err(anyhow!("unsupported zfs command")),
    }
}

pub fn parse_receive_resume_token(output: &str) -> Option<String> {
    let token = output.trim();
    if token.is_empty() || token == "-" {
        None
    } else {
        Some(token.to_string())
    }
}

pub fn is_dataset_missing_error(dataset: &str, stderr: &str) -> bool {
    stderr
        .lines()
        .any(|line| line.trim() == format!("cannot open '{}': dataset does not exist", dataset))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dataset_validation_rejects_whitespace_and_control_chars() {
        assert!(validate_dataset_name("tank/data").is_ok());
        assert!(validate_dataset_name("tank/data set").is_err());
        assert!(validate_dataset_name("tank/data\nset").is_err());
        assert!(validate_dataset_name("tank/data\tset").is_err());
    }

    #[test]
    fn dataset_validation_rejects_empty_option_like_and_snapshots() {
        assert!(validate_dataset_name("").is_err());
        assert!(validate_dataset_name("-x").is_err());
        assert!(validate_dataset_name("tank/data@snap").is_err());
    }

    #[test]
    fn builders_match_forced_command_shapes() {
        let snapshot =
            Snapshot::from_str("tank/data@autosnap_2021-06-14T03:21:01Z_hourly").unwrap();
        for args in [
            remote_snapshot_list_args("backup/tank/data").unwrap(),
            remote_receive_resume_token_args("backup/tank/data").unwrap(),
            remote_receive_args("backup/tank/data").unwrap(),
            remote_receive_snapshot_args("backup/tank/data", &snapshot).unwrap(),
        ] {
            let parsed = parse_forced_remote_zfs_command(&args.join(" ")).unwrap();
            assert_eq!(args, parsed.ssh_args());
        }
    }

    #[test]
    fn forced_command_parses_named_receive() {
        let parsed = parse_forced_remote_zfs_command(
            "zfs receive -s -u backup/tank/data@autosnap_2021-06-14T03:21:01Z_hourly",
        )
        .unwrap();

        match parsed {
            RemoteZfsCommand::Receive { dataset, snapshot } => {
                assert_eq!(dataset, "backup/tank/data");
                let snapshot = snapshot.unwrap();
                assert_eq!(snapshot.volume, dataset.as_str());
                assert_eq!(
                    snapshot.to_string(),
                    "backup/tank/data@autosnap_2021-06-14T03:21:01Z_hourly"
                );
            }
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn forced_command_parses_plain_receive_without_snapshot() {
        assert_eq!(
            RemoteZfsCommand::Receive {
                dataset: String::from("backup/tank/data"),
                snapshot: None,
            },
            parse_forced_remote_zfs_command("zfs receive -s -u backup/tank/data").unwrap()
        );
    }

    #[test]
    fn forced_command_rejects_non_canonical_receive_snapshots() {
        for target in [
            // non-UTC offset
            "backup/tank/data@autosnap_2021-06-14T03:21:01-01:00_hourly",
            // explicit +00:00 instead of Z
            "backup/tank/data@autosnap_2021-06-14T03:21:01+00:00_hourly",
            // fractional seconds
            "backup/tank/data@autosnap_2021-06-14T03:21:01.500Z_hourly",
            // unknown time unit
            "backup/tank/data@autosnap_2021-06-14T03:21:01Z_weekly",
            // invalid prefix charset
            "backup/tank/data@auto.snap_2021-06-14T03:21:01Z_hourly",
            // empty snapshot part
            "backup/tank/data@",
        ] {
            assert!(
                parse_forced_remote_zfs_command(&format!("zfs receive -s -u {}", target)).is_err(),
                "expected rejection of {}",
                target
            );
        }
    }

    #[test]
    fn remote_receive_snapshot_args_rewrites_volume_to_remote_dataset() {
        let snapshot =
            Snapshot::from_str("tank/data@autosnap_2021-06-14T03:21:01Z_hourly").unwrap();

        assert_eq!(
            vec![
                "zfs",
                "receive",
                "-s",
                "-u",
                "backup/tank/data@autosnap_2021-06-14T03:21:01Z_hourly"
            ],
            remote_receive_snapshot_args("backup/tank/data", &snapshot).unwrap()
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

    #[test]
    fn forced_command_rejects_destroy() {
        assert!(
            parse_forced_remote_zfs_command(
                "zfs destroy backup/tank/data@autosnap_2021-06-14T03:21:01Z_hourly"
            )
            .is_err()
        );
    }

    #[test]
    fn forced_command_rejects_unknown_or_extra_args() {
        assert!(parse_forced_remote_zfs_command("true").is_err());
        assert!(parse_forced_remote_zfs_command("zfs list").is_err());
        assert!(
            parse_forced_remote_zfs_command(
                "zfs list -H -o name -t snapshot backup/tank/data extra"
            )
            .is_err()
        );
    }

    #[test]
    fn forced_command_rejects_bad_dynamic_targets() {
        assert!(parse_forced_remote_zfs_command("zfs receive -s -u backup/tank/data set").is_err());
        assert!(parse_forced_remote_zfs_command("zfs receive -s -u -x").is_err());
    }
}
