use crate::cfg::SNAPSHOT_PREFIX_REGEX;
use crate::time_unit::TimeUnit;
use crate::CompactString;
use anyhow::{Context, Error};
use chrono::{DateTime, SecondsFormat, Utc};
use once_cell::sync::Lazy;
use regex::Regex;
use std::fmt::Write;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Snapshot {
    pub volume: CompactString,
    pub prefix: CompactString,
    pub date_time: DateTime<Utc>,
    pub time_unit: TimeUnit,
}

impl Snapshot {
    pub fn is_valid(&self) -> bool {
        let mut buff = CompactString::new();
        if let Err(e) = write!(&mut buff, "{}", self) {
            log::error!("error writing snapshot to string: {}", e);
            return false;
        }
        match Snapshot::from_str(buff.as_str()) {
            Ok(parsed) => {
                let res = parsed == *self;
                if !res {
                    log::warn!(
                        "parsed snapshot {:?} does not equal original snapshot {:?}: {}",
                        parsed,
                        self,
                        self,
                    );
                }
                res
            }
            Err(e) => {
                log::warn!(
                    "unable to parse snapshot {:?} from snapshot info: {}",
                    self,
                    e
                );
                false
            }
        }
    }
}

static SNAPSHOT_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        format!(
            r"^([-a-zA-Z0-9 ,_.:/]+)@({})_([-0-9T:.TZ+]+)_([a-z]+)$",
            SNAPSHOT_PREFIX_REGEX
        )
        .as_str(),
    )
    .unwrap()
});

impl FromStr for Snapshot {
    type Err = Error;

    fn from_str(s: &str) -> anyhow::Result<Self, Self::Err> {
        let captures = SNAPSHOT_REGEX
            .captures(s)
            .with_context(|| format!("error matching snapshot to pattern: {}", s))?;
        let volume = captures.get(1).unwrap().as_str();
        let prefix = captures.get(2).unwrap().as_str();
        let date_time = captures.get(3).unwrap().as_str();
        let time_unit = captures.get(4).unwrap().as_str();
        Ok(Snapshot {
            volume: CompactString::from(volume),
            prefix: CompactString::from(prefix),
            date_time: DateTime::parse_from_rfc3339(date_time)
                .with_context(|| format!("invalid date time: {}", date_time))?
                .with_timezone(&Utc),
            time_unit: TimeUnit::from_str(time_unit).with_context(|| "error parsing time unit")?,
        })
    }
}

impl Display for Snapshot {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}@{}_{}_{}",
            self.volume,
            self.prefix,
            self.date_time.to_rfc3339_opts(SecondsFormat::Secs, true),
            self.time_unit
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, TimeZone, Utc};

    #[test]
    fn from_str() {
        assert_eq!(
            Snapshot {
                volume: CompactString::from("zroot/test::dt,1._- /volume123"),
                prefix: CompactString::from("autosnap"),
                date_time: Utc
                    .from_utc_datetime(
                        &NaiveDate::from_ymd_opt(2021, 6, 14)
                            .unwrap()
                            .and_hms_opt(3, 21, 1)
                            .unwrap()
                    )
                    .with_timezone(&Utc),
                time_unit: TimeUnit::Hour
            },
            Snapshot::from_str(
                "zroot/test::dt,1._- /volume123@autosnap_2021-06-14T03:21:01Z_hourly"
            )
            .unwrap()
        )
    }

    #[test]
    fn from_str_with_timezone() {
        assert_eq!(
            Snapshot {
                volume: CompactString::from("zroot"),
                prefix: CompactString::from("autosnap"),
                date_time: Utc
                    .from_utc_datetime(
                        &NaiveDate::from_ymd_opt(2021, 6, 14)
                            .unwrap()
                            .and_hms_opt(4, 21, 1)
                            .unwrap()
                    )
                    .with_timezone(&Utc),
                time_unit: TimeUnit::Year
            },
            Snapshot::from_str("zroot@autosnap_2021-06-14T03:21:01-01:00_yearly").unwrap()
        )
    }

    #[test]
    fn round_trip() {
        let snapshot = Snapshot {
            volume: CompactString::from("zroot/test::dt,1._ /volume123"),
            prefix: CompactString::from("autosnap"),
            date_time: Utc
                .from_utc_datetime(
                    &NaiveDate::from_ymd_opt(2021, 6, 14)
                        .unwrap()
                        .and_hms_opt(3, 21, 1)
                        .unwrap(),
                )
                .with_timezone(&Utc),
            time_unit: TimeUnit::Minute,
        };
        assert!(snapshot.is_valid());
        assert_eq!(
            snapshot,
            Snapshot::from_str(format!("{}", &snapshot).as_str()).unwrap()
        )
    }
}
