use anyhow::{Result, anyhow};

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RemoteZfsCommand {
    ListSnapshots { dataset: String },
    ReceiveResumeToken { dataset: String },
    Receive { dataset: String },
}

impl RemoteZfsCommand {
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
            RemoteZfsCommand::Receive { dataset } => vec!["receive", "-s", "-u", dataset]
                .into_iter()
                .map(String::from)
                .collect(),
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
        ["zfs", "receive", "-s", "-u", dataset] => {
            validate_dataset_name(dataset)?;
            Ok(RemoteZfsCommand::Receive {
                dataset: (*dataset).to_string(),
            })
        }
        _ => Err(anyhow!("unsupported zfs command")),
    }
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
        for args in [
            remote_snapshot_list_args("backup/tank/data").unwrap(),
            remote_receive_resume_token_args("backup/tank/data").unwrap(),
            remote_receive_args("backup/tank/data").unwrap(),
        ] {
            let parsed = parse_forced_remote_zfs_command(&args.join(" ")).unwrap();
            assert_eq!(args, parsed.ssh_args());
        }
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
