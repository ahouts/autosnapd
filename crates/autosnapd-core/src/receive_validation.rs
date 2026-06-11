use crate::cfg::Config;
use crate::snapshot::Snapshot;
use anyhow::{Result, anyhow};
use chrono::{DateTime, Duration, Utc};

pub fn max_clock_skew() -> Duration {
    Duration::minutes(5)
}

pub fn validate_snapshot_timestamp(snapshot: &Snapshot, now: DateTime<Utc>) -> Result<()> {
    let latest_allowed = now + max_clock_skew();
    if snapshot.date_time > latest_allowed {
        return Err(anyhow!(
            "snapshot {} is timestamped in the future: {} is after {} ({} plus {} minutes of clock skew tolerance)",
            snapshot,
            snapshot.date_time,
            latest_allowed,
            now,
            max_clock_skew().num_minutes(),
        ));
    }

    Ok(())
}

pub fn validate_snapshot_spacing(snapshot: &Snapshot, existing: &[Snapshot]) -> Result<()> {
    let newest_of_unit = existing
        .iter()
        .filter(|existing| existing.time_unit == snapshot.time_unit)
        .max_by_key(|existing| existing.date_time);

    if let Some(newest) = newest_of_unit {
        let earliest_allowed = newest.date_time + snapshot.time_unit;
        if snapshot.date_time < earliest_allowed {
            return Err(anyhow!(
                "snapshot {} is not at least one {} interval after the newest existing {} snapshot {}: {} is before {}",
                snapshot,
                snapshot.time_unit,
                snapshot.time_unit,
                newest,
                snapshot.date_time,
                earliest_allowed,
            ));
        }
    }

    Ok(())
}

pub fn validate_replica_target(config: &Config, dataset: &str) -> Result<()> {
    match config.volume_config.get(dataset) {
        None => Err(anyhow!(
            "dataset {} is not a configured volume on this host",
            dataset
        )),
        Some(volume) if !volume.prune_only => Err(anyhow!(
            "dataset {} is not configured as a prune_only volume",
            dataset
        )),
        Some(_) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::load_config;
    use crate::time_unit::TimeUnit;
    use crate::CompactString;
    use chrono::{NaiveDate, TimeZone};
    use std::str::FromStr;

    fn snapshot_at(time_unit: TimeUnit, date_time: DateTime<Utc>) -> Snapshot {
        Snapshot {
            volume: CompactString::from("backup/tank/data"),
            prefix: CompactString::from("autosnap"),
            date_time,
            time_unit,
        }
    }

    fn utc(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> DateTime<Utc> {
        Utc.from_utc_datetime(
            &NaiveDate::from_ymd_opt(y, mo, d)
                .unwrap()
                .and_hms_opt(h, mi, s)
                .unwrap(),
        )
    }

    #[test]
    fn timestamp_in_past_is_accepted() {
        let now = utc(2021, 6, 14, 12, 0, 0);
        let snapshot = snapshot_at(TimeUnit::Hour, utc(2021, 6, 14, 3, 21, 1));

        assert!(validate_snapshot_timestamp(&snapshot, now).is_ok());
    }

    #[test]
    fn timestamp_within_clock_skew_is_accepted() {
        let now = utc(2021, 6, 14, 12, 0, 0);

        for date_time in [utc(2021, 6, 14, 12, 4, 59), utc(2021, 6, 14, 12, 5, 0)] {
            let snapshot = snapshot_at(TimeUnit::Hour, date_time);
            assert!(validate_snapshot_timestamp(&snapshot, now).is_ok());
        }
    }

    #[test]
    fn timestamp_beyond_clock_skew_is_rejected() {
        let now = utc(2021, 6, 14, 12, 0, 0);
        let snapshot = snapshot_at(TimeUnit::Hour, utc(2021, 6, 14, 12, 5, 1));

        assert!(
            validate_snapshot_timestamp(&snapshot, now)
                .unwrap_err()
                .to_string()
                .contains("timestamped in the future")
        );
    }

    #[test]
    fn spacing_with_no_existing_snapshots_is_accepted() {
        let snapshot = snapshot_at(TimeUnit::Hour, utc(2021, 6, 14, 3, 0, 0));

        assert!(validate_snapshot_spacing(&snapshot, &[]).is_ok());
    }

    #[test]
    fn spacing_of_exactly_one_unit_is_accepted() {
        let existing = [snapshot_at(TimeUnit::Hour, utc(2021, 6, 14, 3, 0, 0))];
        let snapshot = snapshot_at(TimeUnit::Hour, utc(2021, 6, 14, 4, 0, 0));

        assert!(validate_snapshot_spacing(&snapshot, &existing).is_ok());
    }

    #[test]
    fn spacing_one_second_short_is_rejected() {
        let existing = [snapshot_at(TimeUnit::Hour, utc(2021, 6, 14, 3, 0, 0))];
        let snapshot = snapshot_at(TimeUnit::Hour, utc(2021, 6, 14, 3, 59, 59));

        assert!(validate_snapshot_spacing(&snapshot, &existing).is_err());
    }

    #[test]
    fn duplicate_timestamp_is_rejected() {
        let existing = [snapshot_at(TimeUnit::Hour, utc(2021, 6, 14, 3, 0, 0))];
        let snapshot = snapshot_at(TimeUnit::Hour, utc(2021, 6, 14, 3, 0, 0));

        assert!(validate_snapshot_spacing(&snapshot, &existing).is_err());
    }

    #[test]
    fn backfill_older_than_newest_is_rejected() {
        let existing = [snapshot_at(TimeUnit::Hour, utc(2021, 6, 14, 3, 0, 0))];
        let snapshot = snapshot_at(TimeUnit::Hour, utc(2021, 6, 13, 3, 0, 0));

        assert!(validate_snapshot_spacing(&snapshot, &existing).is_err());
    }

    #[test]
    fn spacing_ignores_other_time_units() {
        let existing = [snapshot_at(TimeUnit::Day, utc(2021, 6, 14, 3, 0, 0))];
        let snapshot = snapshot_at(TimeUnit::Hour, utc(2021, 6, 14, 3, 0, 0));

        assert!(validate_snapshot_spacing(&snapshot, &existing).is_ok());
    }

    #[test]
    fn spacing_compares_across_prefixes() {
        let existing = [Snapshot {
            prefix: CompactString::from("othersnap"),
            ..snapshot_at(TimeUnit::Hour, utc(2021, 6, 14, 3, 0, 0))
        }];
        let snapshot = snapshot_at(TimeUnit::Hour, utc(2021, 6, 14, 3, 30, 0));

        assert!(validate_snapshot_spacing(&snapshot, &existing).is_err());
    }

    #[test]
    fn spacing_uses_newest_of_unit_in_unsorted_list() {
        let existing = [
            snapshot_at(TimeUnit::Hour, utc(2021, 6, 14, 5, 0, 0)),
            snapshot_at(TimeUnit::Hour, utc(2021, 6, 14, 3, 0, 0)),
        ];
        let snapshot = snapshot_at(TimeUnit::Hour, utc(2021, 6, 14, 4, 0, 0));

        assert!(validate_snapshot_spacing(&snapshot, &existing).is_err());
    }

    #[test]
    fn month_spacing_is_computed_from_previous_snapshot_date() {
        // February 2021 has 28 days, so the next monthly snapshot is allowed
        // exactly 28 days after the previous one.
        let existing = [snapshot_at(TimeUnit::Month, utc(2021, 2, 1, 0, 0, 0))];

        let on_time = snapshot_at(TimeUnit::Month, utc(2021, 3, 1, 0, 0, 0));
        assert!(validate_snapshot_spacing(&on_time, &existing).is_ok());

        let early = snapshot_at(TimeUnit::Month, utc(2021, 2, 28, 23, 59, 59));
        assert!(validate_snapshot_spacing(&early, &existing).is_err());
    }

    #[test]
    fn year_spacing_accounts_for_leap_years() {
        // 2024 is a leap year: 366 days until the next yearly snapshot.
        let existing = [snapshot_at(TimeUnit::Year, utc(2024, 1, 1, 0, 0, 0))];

        let early = snapshot_at(TimeUnit::Year, utc(2024, 12, 31, 0, 0, 0));
        assert!(validate_snapshot_spacing(&early, &existing).is_err());

        let on_time = snapshot_at(TimeUnit::Year, utc(2025, 1, 1, 0, 0, 0));
        assert!(validate_snapshot_spacing(&on_time, &existing).is_ok());
    }

    #[test]
    fn replica_target_must_be_configured() {
        let config = load_config("").unwrap();

        assert!(
            validate_replica_target(&config, "backup/tank/data")
                .unwrap_err()
                .to_string()
                .contains("not a configured volume")
        );
    }

    #[test]
    fn replica_target_must_be_prune_only() {
        let config = load_config(
            r#"
["backup/tank/data"]
daily = 7
        "#,
        )
        .unwrap();

        assert!(
            validate_replica_target(&config, "backup/tank/data")
                .unwrap_err()
                .to_string()
                .contains("not configured as a prune_only volume")
        );
    }

    #[test]
    fn prune_only_replica_target_is_accepted() {
        let config = load_config(
            r#"
["backup/tank/data"]
prune_only = true
hourly = 72
daily = 30
        "#,
        )
        .unwrap();

        assert!(validate_replica_target(&config, "backup/tank/data").is_ok());
    }

    #[test]
    fn validation_round_trips_with_parsed_snapshot_names() {
        let existing =
            Snapshot::from_str("backup/tank/data@autosnap_2021-06-14T03:21:01Z_hourly").unwrap();
        let snapshot =
            Snapshot::from_str("backup/tank/data@autosnap_2021-06-14T04:21:01Z_hourly").unwrap();

        assert!(validate_snapshot_spacing(&snapshot, &[existing]).is_ok());
    }
}
