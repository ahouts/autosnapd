use crate::CompactString;
use crate::remote_command::validate_dataset_name;
use crate::time_unit::TimeUnit;
use anyhow::{Context, Error, Result, anyhow};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::de::Error as SerdeError;
use serde::{Deserialize, Deserializer};
use std::collections::HashMap;
use std::ops::Index;
use std::str::FromStr;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct SnapshotPrefix(pub CompactString);

pub const SNAPSHOT_PREFIX_REGEX: &str = r"[a-zA-Z0-9]+";
static SNAPSHOT_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(format!("^{}$", SNAPSHOT_PREFIX_REGEX).as_str()).unwrap());

impl SnapshotPrefix {
    fn new(s: CompactString) -> Result<Self> {
        if !SNAPSHOT_REGEX.is_match(s.as_str()) {
            Err(anyhow!("invalid snapshot prefix: {}", s))
        } else {
            Ok(SnapshotPrefix(s))
        }
    }
}

impl Default for SnapshotPrefix {
    fn default() -> Self {
        SnapshotPrefix(CompactString::from("autosnap"))
    }
}

impl FromStr for SnapshotPrefix {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        SnapshotPrefix::new(CompactString::from(s))
    }
}

impl<'de> Deserialize<'de> for SnapshotPrefix {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        let s = CompactString::deserialize(deserializer)?;
        SnapshotPrefix::new(s).map_err(|e| D::Error::custom(format!("{}", e)))
    }
}

#[derive(Debug, Eq, PartialEq, Deserialize, Clone)]
pub struct RemoteConfig {
    pub host: CompactString,
    pub remote_path: CompactString,
    pub local_path: CompactString,
}

#[derive(Debug, Eq, PartialEq, Deserialize, Clone)]
pub struct ScriptConfig {
    pub path: CompactString,
    #[serde(default)]
    pub args: Vec<CompactString>,
}

#[derive(Debug, Eq, PartialEq, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SourceConfig {
    Remote(RemoteConfig),
    Script(ScriptConfig),
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct ReplicationConfig {
    pub host: CompactString,
    pub dataset: CompactString,
}

#[derive(Deserialize)]
struct RawConfig {
    #[serde(default)]
    snapshot_prefix: SnapshotPrefix,
    #[serde(default)]
    templates: Templates,
    #[serde(flatten)]
    configs: HashMap<CompactString, RawVolumeConfig>,
}

#[derive(Default, Deserialize)]
struct Templates(HashMap<CompactString, RawBaseVolumeConfig>);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawVolumeConfig {
    template: Option<CompactString>,
    #[serde(flatten)]
    base: RawBaseVolumeConfig,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawBaseVolumeConfig {
    source: Option<SourceConfig>,
    replication: Option<RawReplicationConfig>,
    host: Option<CompactString>,
    dataset: Option<CompactString>,
    prune_only: Option<bool>,
    minutely: Option<u16>,
    hourly: Option<u16>,
    daily: Option<u16>,
    monthly: Option<u16>,
    yearly: Option<u16>,
}

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct RawReplicationConfig {
    template: Option<CompactString>,
    host: Option<CompactString>,
    dataset: Option<CompactString>,
}

#[derive(Debug, Eq, PartialEq, Default, Clone)]
pub struct VolumeConfig {
    pub minutely: u16,
    pub hourly: u16,
    pub daily: u16,
    pub monthly: u16,
    pub yearly: u16,
    pub prune_only: bool,
    pub source: Option<SourceConfig>,
    pub replication: Option<ReplicationConfig>,
}

impl Index<TimeUnit> for VolumeConfig {
    type Output = u16;

    fn index(&self, index: TimeUnit) -> &Self::Output {
        match index {
            TimeUnit::Minute => &self.minutely,
            TimeUnit::Hour => &self.hourly,
            TimeUnit::Day => &self.daily,
            TimeUnit::Month => &self.monthly,
            TimeUnit::Year => &self.yearly,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Config {
    pub snapshot_prefix: SnapshotPrefix,
    pub volume_config: HashMap<CompactString, VolumeConfig>,
}

pub fn load_config(data: &str) -> Result<Config> {
    let raw_cfg: RawConfig =
        toml::from_str(data).with_context(|| "error parsing configuration file")?;

    fn apply_defaults(
        cfg: &RawBaseVolumeConfig,
        defaults: &VolumeConfig,
        templates: &HashMap<CompactString, RawBaseVolumeConfig>,
    ) -> Result<VolumeConfig> {
        let mut resolved = VolumeConfig {
            minutely: cfg.minutely.unwrap_or(defaults.minutely),
            hourly: cfg.hourly.unwrap_or(defaults.hourly),
            daily: cfg.daily.unwrap_or(defaults.daily),
            monthly: cfg.monthly.unwrap_or(defaults.monthly),
            yearly: cfg.yearly.unwrap_or(defaults.yearly),
            prune_only: cfg.prune_only.unwrap_or(defaults.prune_only),
            source: cfg.source.clone().or_else(|| defaults.source.clone()),
            replication: None,
        };

        resolved.replication = match &cfg.replication {
            Some(replication) => Some(resolve_replication(replication, templates)?),
            None => defaults.replication.clone(),
        };

        Ok(resolved)
    }

    fn resolve_replication(
        cfg: &RawReplicationConfig,
        templates: &HashMap<CompactString, RawBaseVolumeConfig>,
    ) -> Result<ReplicationConfig> {
        let template = match &cfg.template {
            Some(template_name) => {
                let template = templates
                    .get(template_name)
                    .with_context(|| format!("unknown replication template: {}", template_name))?;
                Some(template)
            }
            None => None,
        };

        let host = cfg
            .host
            .clone()
            .or_else(|| template.and_then(|template| template.host.clone()))
            .with_context(|| "replication config missing host")?;
        let dataset = cfg
            .dataset
            .clone()
            .or_else(|| template.and_then(|template| template.dataset.clone()))
            .with_context(|| "replication config missing dataset")?;
        validate_dataset_name(&dataset)
            .with_context(|| format!("invalid replication dataset: {}", dataset))?;

        Ok(ReplicationConfig { host, dataset })
    }

    fn validate_prune_only(volume: &str, config: &VolumeConfig) -> Result<()> {
        if config.prune_only && config.source.is_some() {
            return Err(anyhow!(
                "volume {} is prune_only but has a source configured",
                volume
            ));
        }

        Ok(())
    }

    let raw_templates = raw_cfg.templates.0;

    let templates: HashMap<CompactString, VolumeConfig> = raw_templates
        .iter()
        .map(|(key, config)| {
            Ok((
                key.clone(),
                apply_defaults(config, &VolumeConfig::default(), &raw_templates)?,
            ))
        })
        .collect::<Result<HashMap<CompactString, VolumeConfig>>>()?;

    let volume_config = raw_cfg
        .configs
        .into_iter()
        .map(|(key, config)| {
            validate_dataset_name(&key)
                .with_context(|| format!("invalid volume dataset: {}", key))?;
            let resolved_config = if let Some(template_name) = &config.template {
                if let Some(template) = templates.get(template_name) {
                    apply_defaults(&config.base, template, &raw_templates)?
                } else {
                    return Err(anyhow!("unknown template: {}", template_name));
                }
            } else {
                apply_defaults(&config.base, &VolumeConfig::default(), &raw_templates)?
            };

            validate_prune_only(&key, &resolved_config)?;
            Ok((key, resolved_config))
        })
        .collect::<Result<HashMap<CompactString, VolumeConfig>>>()?;

    Ok(Config {
        snapshot_prefix: raw_cfg.snapshot_prefix,
        volume_config,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_config_works() {
        const TEST_CONFIG: &str = r#"
snapshot_prefix = "asdf"

[templates.t1]
minutely = 1
hourly = 2

[templates.t2]
minutely = 3

["volume1/f1"]
template = "t1"
hourly = 4
daily = 5

["volume1/f2"]
template = "t2"
daily = 6

["volume1/f3"]
daily = 7
        "#;
        assert_eq!(
            Config {
                snapshot_prefix: SnapshotPrefix::from_str("asdf").unwrap(),
                volume_config: vec![
                    (
                        CompactString::from("volume1/f1"),
                        VolumeConfig {
                            minutely: 1,
                            hourly: 4,
                            daily: 5,
                            ..VolumeConfig::default()
                        }
                    ),
                    (
                        CompactString::from("volume1/f2"),
                        VolumeConfig {
                            minutely: 3,
                            daily: 6,
                            ..VolumeConfig::default()
                        }
                    ),
                    (
                        CompactString::from("volume1/f3"),
                        VolumeConfig {
                            daily: 7,
                            ..VolumeConfig::default()
                        }
                    )
                ]
                .into_iter()
                .collect::<HashMap<CompactString, VolumeConfig>>()
            },
            load_config(TEST_CONFIG).unwrap()
        );
    }

    #[test]
    fn load_empty_config() {
        assert_eq!(
            Config {
                snapshot_prefix: Default::default(),
                volume_config: Default::default()
            },
            load_config("").unwrap()
        );
    }

    #[test]
    fn invalid_snapshot_prefix() {
        assert_eq!(
            "invalid snapshot prefix: a!b",
            SnapshotPrefix::from_str("a!b")
                .unwrap_err()
                .to_string()
                .as_str()
        );
    }

    #[test]
    fn load_config_rejects_volume_dataset_with_whitespace() {
        const TEST_CONFIG: &str = r#"
["tank/data set"]
daily = 1
        "#;

        assert!(
            load_config(TEST_CONFIG)
                .unwrap_err()
                .to_string()
                .contains("invalid volume dataset")
        );
    }

    #[test]
    fn load_config_rejects_replication_dataset_with_whitespace() {
        const TEST_CONFIG: &str = r#"
["tank/data"]
replication = { host = "backup.example.com", dataset = "backup/tank/data set" }
        "#;

        assert!(
            load_config(TEST_CONFIG)
                .unwrap_err()
                .to_string()
                .contains("invalid replication dataset")
        );
    }

    #[test]
    fn load_config_with_sources() {
        const TEST_CONFIG: &str = r#"
snapshot_prefix = "asdf"

[templates.remote_backup]
minutely = 1
hourly = 2
source = { type = "remote", host = "server1.example.com", remote_path = "/data/backup", local_path = "/mnt/backup" }

["volume1/remote"]
template = "remote_backup"
hourly = 4

["volume2/remote"]
source = { type = "remote", host = "server2.example.com", remote_path = "/var/lib/data", local_path = "/mnt/data2" }
daily = 7

["volume3/script"]
source = { type = "script", path = "/usr/local/bin/build-volume", args = ["--target", "/mnt/generated"] }
monthly = 1
        "#;

        let config = load_config(TEST_CONFIG).unwrap();

        let vol1_cfg = config.volume_config.get("volume1/remote").unwrap();
        assert_eq!(
            vol1_cfg.source,
            Some(SourceConfig::Remote(RemoteConfig {
                host: "server1.example.com".into(),
                remote_path: "/data/backup".into(),
                local_path: "/mnt/backup".into(),
            }))
        );
        assert_eq!(vol1_cfg.hourly, 4);
        assert_eq!(vol1_cfg.minutely, 1);

        let vol2_cfg = config.volume_config.get("volume2/remote").unwrap();
        assert_eq!(
            vol2_cfg.source,
            Some(SourceConfig::Remote(RemoteConfig {
                host: "server2.example.com".into(),
                remote_path: "/var/lib/data".into(),
                local_path: "/mnt/data2".into(),
            }))
        );
        assert_eq!(vol2_cfg.daily, 7);

        let vol3_cfg = config.volume_config.get("volume3/script").unwrap();
        assert_eq!(
            vol3_cfg.source,
            Some(SourceConfig::Script(ScriptConfig {
                path: "/usr/local/bin/build-volume".into(),
                args: vec!["--target".into(), "/mnt/generated".into()],
            }))
        );
        assert_eq!(vol3_cfg.monthly, 1);
    }

    #[test]
    fn load_config_with_script_source_default_args() {
        const TEST_CONFIG: &str = r#"
["volume/script"]
source = { type = "script", path = "/usr/local/bin/build-volume" }
daily = 1
        "#;

        let config = load_config(TEST_CONFIG).unwrap();
        let volume = config.volume_config.get("volume/script").unwrap();

        assert_eq!(
            volume.source,
            Some(SourceConfig::Script(ScriptConfig {
                path: "/usr/local/bin/build-volume".into(),
                args: Vec::new(),
            }))
        );
    }

    #[test]
    fn legacy_remote_config_is_rejected() {
        const TEST_CONFIG: &str = r#"
["volume/remote"]
remote = { host = "server.example.com", remote_path = "/data", local_path = "/mnt/data" }
daily = 1
        "#;

        assert!(load_config(TEST_CONFIG).is_err());
    }

    #[test]
    fn load_config_with_replication() {
        const TEST_CONFIG: &str = r#"
[templates.replicated]
hourly = 2
replication = { host = "backup1.example.com", dataset = "backup/tank/data" }

["tank/data"]
template = "replicated"
daily = 7

["tank/logs"]
replication = { host = "backup2.example.com", dataset = "backup/tank/logs" }
monthly = 3
        "#;

        let config = load_config(TEST_CONFIG).unwrap();

        let data_cfg = config.volume_config.get("tank/data").unwrap();
        assert_eq!(data_cfg.hourly, 2);
        assert_eq!(data_cfg.daily, 7);
        assert_eq!(
            data_cfg.replication.as_ref().unwrap().host,
            "backup1.example.com"
        );
        assert_eq!(
            data_cfg.replication.as_ref().unwrap().dataset,
            "backup/tank/data"
        );

        let logs_cfg = config.volume_config.get("tank/logs").unwrap();
        assert_eq!(logs_cfg.monthly, 3);
        assert_eq!(
            logs_cfg.replication.as_ref().unwrap().host,
            "backup2.example.com"
        );
        assert_eq!(
            logs_cfg.replication.as_ref().unwrap().dataset,
            "backup/tank/logs"
        );
    }

    #[test]
    fn load_config_with_replication_template() {
        const TEST_CONFIG: &str = r#"
[templates.backup]
host = "backup.example.com"

["tank/data"]
hourly = 1
daily = 1
replication = { template = "backup", dataset = "backup/tank/data" }

["tank/logs"]
hourly = 1
daily = 1
monthly = 1
replication = { template = "backup", dataset = "backup/tank/logs" }
        "#;

        let config = load_config(TEST_CONFIG).unwrap();

        let data_replication = config
            .volume_config
            .get("tank/data")
            .unwrap()
            .replication
            .as_ref()
            .unwrap();
        assert_eq!(data_replication.host, "backup.example.com");
        assert_eq!(data_replication.dataset, "backup/tank/data");

        let logs_replication = config
            .volume_config
            .get("tank/logs")
            .unwrap()
            .replication
            .as_ref()
            .unwrap();
        assert_eq!(logs_replication.host, "backup.example.com");
        assert_eq!(logs_replication.dataset, "backup/tank/logs");
    }

    #[test]
    fn load_config_with_matching_volume_and_replication_template() {
        const TEST_CONFIG: &str = r#"
[templates.backup]
host = "backup.example.com"
hourly = 24
daily = 7

["tank/data"]
template = "backup"
replication = { template = "backup", dataset = "backup/tank/data" }
        "#;

        let config = load_config(TEST_CONFIG).unwrap();
        let volume = config.volume_config.get("tank/data").unwrap();

        assert_eq!(volume.hourly, 24);
        assert_eq!(volume.daily, 7);
        assert_eq!(
            volume.replication.as_ref().unwrap().host,
            "backup.example.com"
        );
        assert_eq!(
            volume.replication.as_ref().unwrap().dataset,
            "backup/tank/data"
        );
    }

    #[test]
    fn load_config_allows_replication_with_zero_local_retention() {
        const TEST_CONFIG: &str = r#"
["tank/data"]
replication = { host = "backup.example.com", dataset = "backup/tank/data" }
        "#;

        let config = load_config(TEST_CONFIG).unwrap();
        let volume = config.volume_config.get("tank/data").unwrap();

        assert_eq!(volume.daily, 0);
        assert_eq!(
            volume.replication.as_ref().unwrap().host,
            "backup.example.com"
        );
    }

    #[test]
    fn replication_retention_fields_are_rejected() {
        const TEST_CONFIG: &str = r#"
["tank/data"]
daily = 7
replication = { host = "backup.example.com", dataset = "backup/tank/data", daily = 30 }
        "#;

        assert!(load_config(TEST_CONFIG).is_err());
    }

    #[test]
    fn prune_only_volume_parses() {
        const TEST_CONFIG: &str = r#"
["backup/tank/data"]
prune_only = true
hourly = 72
daily = 30
        "#;

        let config = load_config(TEST_CONFIG).unwrap();
        let volume = config.volume_config.get("backup/tank/data").unwrap();

        assert!(volume.prune_only);
        assert_eq!(volume.hourly, 72);
        assert_eq!(volume.daily, 30);
    }

    #[test]
    fn prune_only_defaults_to_false() {
        const TEST_CONFIG: &str = r#"
["tank/data"]
daily = 7
        "#;

        let config = load_config(TEST_CONFIG).unwrap();
        let volume = config.volume_config.get("tank/data").unwrap();

        assert!(!volume.prune_only);
    }

    #[test]
    fn prune_only_inherited_from_template() {
        const TEST_CONFIG: &str = r#"
[templates.replica]
prune_only = true
hourly = 72

["backup/tank/data"]
template = "replica"
        "#;

        let config = load_config(TEST_CONFIG).unwrap();
        let volume = config.volume_config.get("backup/tank/data").unwrap();

        assert!(volume.prune_only);
        assert_eq!(volume.hourly, 72);
    }

    #[test]
    fn prune_only_disabled_by_volume_override() {
        const TEST_CONFIG: &str = r#"
[templates.replica]
prune_only = true
hourly = 72

["tank/data"]
template = "replica"
prune_only = false
        "#;

        let config = load_config(TEST_CONFIG).unwrap();
        let volume = config.volume_config.get("tank/data").unwrap();

        assert!(!volume.prune_only);
    }

    #[test]
    fn prune_only_with_source_is_rejected() {
        const TEST_CONFIG: &str = r#"
["backup/tank/data"]
prune_only = true
daily = 7
source = { type = "script", path = "/usr/local/bin/build-volume" }
        "#;

        assert_eq!(
            "volume backup/tank/data is prune_only but has a source configured",
            load_config(TEST_CONFIG).unwrap_err().to_string()
        );
    }

    #[test]
    fn prune_only_with_template_source_is_rejected() {
        const TEST_CONFIG: &str = r#"
[templates.sourced]
source = { type = "script", path = "/usr/local/bin/build-volume" }
daily = 7

["backup/tank/data"]
template = "sourced"
prune_only = true
        "#;

        assert_eq!(
            "volume backup/tank/data is prune_only but has a source configured",
            load_config(TEST_CONFIG).unwrap_err().to_string()
        );
    }

    #[test]
    fn prune_only_with_replication_is_allowed() {
        const TEST_CONFIG: &str = r#"
["backup/tank/data"]
prune_only = true
daily = 7
replication = { host = "offsite.example.com", dataset = "offsite/tank/data" }
        "#;

        let config = load_config(TEST_CONFIG).unwrap();
        let volume = config.volume_config.get("backup/tank/data").unwrap();

        assert!(volume.prune_only);
        assert_eq!(
            volume.replication.as_ref().unwrap().host,
            "offsite.example.com"
        );
    }

    #[test]
    fn direct_template_replication_fields_do_not_create_replication_for_volume_template() {
        const TEST_CONFIG: &str = r#"
[templates.backup]
host = "backup.example.com"
hourly = 24

["tank/data"]
template = "backup"
        "#;

        let config = load_config(TEST_CONFIG).unwrap();
        let volume = config.volume_config.get("tank/data").unwrap();

        assert_eq!(volume.hourly, 24);
        assert_eq!(volume.replication, None);
    }

    #[test]
    fn unknown_replication_template_fails() {
        const TEST_CONFIG: &str = r#"
["tank/data"]
replication = { template = "missing", dataset = "backup/tank/data" }
        "#;

        assert_eq!(
            "unknown replication template: missing",
            load_config(TEST_CONFIG).unwrap_err().to_string()
        );
    }

    #[test]
    fn replication_template_without_host_fails() {
        const TEST_CONFIG: &str = r#"
[templates.backup]
hourly = 24

["tank/data"]
replication = { template = "backup", dataset = "backup/tank/data" }
        "#;

        assert_eq!(
            "replication config missing host",
            load_config(TEST_CONFIG).unwrap_err().to_string()
        );
    }

    #[test]
    fn replication_template_without_dataset_fails() {
        const TEST_CONFIG: &str = r#"
[templates.backup]
host = "backup.example.com"
hourly = 24

["tank/data"]
replication = { template = "backup" }
        "#;

        assert_eq!(
            "replication config missing dataset",
            load_config(TEST_CONFIG).unwrap_err().to_string()
        );
    }
}
