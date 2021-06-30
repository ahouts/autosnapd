use crate::time_unit::TimeUnit;
use crate::CompactString;
use anyhow::{anyhow, Context, Error, Result};
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
    minutely: Option<u16>,
    hourly: Option<u16>,
    daily: Option<u16>,
    monthly: Option<u16>,
    yearly: Option<u16>,
}

#[derive(Debug, Eq, PartialEq, Default, Clone)]
pub struct VolumeConfig {
    pub minutely: u16,
    pub hourly: u16,
    pub daily: u16,
    pub monthly: u16,
    pub yearly: u16,
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

pub fn load_config(data: &[u8]) -> Result<Config> {
    let raw_cfg: RawConfig =
        toml::from_slice(data).with_context(|| "error parsing configuration file")?;

    fn apply_defaults(cfg: &RawBaseVolumeConfig, defaults: &VolumeConfig) -> VolumeConfig {
        VolumeConfig {
            minutely: cfg.minutely.unwrap_or(defaults.minutely),
            hourly: cfg.hourly.unwrap_or(defaults.hourly),
            daily: cfg.daily.unwrap_or(defaults.daily),
            monthly: cfg.monthly.unwrap_or(defaults.monthly),
            yearly: cfg.yearly.unwrap_or(defaults.yearly),
        }
    }

    let templates: HashMap<CompactString, VolumeConfig> = raw_cfg
        .templates
        .0
        .into_iter()
        .map(|(key, config)| (key, apply_defaults(&config, &VolumeConfig::default())))
        .collect();

    Ok(Config {
        snapshot_prefix: raw_cfg.snapshot_prefix,
        volume_config: raw_cfg
            .configs
            .into_iter()
            .map(|(key, config)| {
                if let Some(template_name) = &config.template {
                    if let Some(template) = templates.get(template_name) {
                        Ok((key, apply_defaults(&config.base, template)))
                    } else {
                        Err(anyhow!("unknown template: {}", template_name))
                    }
                } else {
                    Ok((key, apply_defaults(&config.base, &VolumeConfig::default())))
                }
            })
            .collect::<Result<HashMap<CompactString, VolumeConfig>>>()?,
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
            load_config(TEST_CONFIG.as_bytes()).unwrap()
        );
    }

    #[test]
    fn load_empty_config() {
        assert_eq!(
            Config {
                snapshot_prefix: Default::default(),
                volume_config: Default::default()
            },
            load_config(&[]).unwrap()
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
}
