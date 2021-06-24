use crate::cfg::{Config, VolumeConfig};
use crate::clock::{Clock, ClockImpl};
use crate::time_unit::TimeUnit;
use crate::zfs::{DryZfsApi, ZfsApi, ZfsApiImpl};
use anyhow::{Context, Result};
use futures::lock::Mutex;
use futures::select;
use futures::FutureExt;
use slice_group_by::GroupBy;
use smartstring::{LazyCompact, SmartString};
use snapshot::Snapshot;
use std::path::PathBuf;
use std::time::Duration;
use structopt::StructOpt;
use strum::IntoEnumIterator;

type CompactString = SmartString<LazyCompact>;

mod cfg;
mod clock;
mod snapshot;
mod time_unit;
mod zfs;

#[derive(StructOpt)]
#[structopt(name = "zsnapd", about = "zfs snapshot daemon")]
struct Opt {
    #[structopt(short, long)]
    config: PathBuf,
    #[structopt(short = "P", long)]
    zfs_path: PathBuf,
    #[structopt(short, long)]
    dry: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::try_init().with_context(|| "error initializing logging")?;
    let opt: Opt = Opt::from_args();

    let cfg_file_data = tokio::fs::read(opt.config)
        .await
        .with_context(|| "error reading configuration file")?;

    let cfg = cfg::load_config(cfg_file_data.as_slice())?;
    log::debug!("config loaded: {:?}", cfg);
    let zfs_api = ZfsApiImpl::new(opt.zfs_path);

    if opt.dry {
        main_loop(&cfg, &DryZfsApi(zfs_api)).await?;
    } else {
        main_loop(&cfg, &zfs_api).await?;
    }

    Ok(())
}

async fn main_loop<A: ZfsApi>(cfg: &Config, zfs_api: &A) -> Result<()> {
    let mut interval = tokio::time::interval(Duration::new(60, 0));
    let mut interrupt = Box::pin(tokio::signal::ctrl_c().fuse());
    let mutex = Mutex::new(());

    loop {
        select! {
            _ = interval.tick().fuse() => handle_snapshots_for_all_volumes(cfg, zfs_api, &ClockImpl, &mutex).await?,
            _ = interrupt => break,
        }
    }

    Ok(())
}

async fn handle_snapshots_for_all_volumes<A: ZfsApi, C: Clock>(
    cfg: &Config,
    zfs_api: &A,
    clock: &C,
    mutex: &Mutex<()>,
) -> Result<()> {
    let guard = if let Some(guard) = mutex.try_lock() {
        guard
    } else {
        log::warn!("not taking snapshots as a previous instance is still active");
        return Ok(());
    };

    log::debug!("handling snapshots for all volumes");
    for (volume, config) in cfg.volume_config.iter() {
        log::debug!("handling snapshots for volume {}: {:?}", volume, config);
        handle_snapshots_for_volume(zfs_api, clock, &volume, config, cfg).await?;
    }

    drop(guard);
    Ok(())
}

async fn handle_snapshots_for_volume<A: ZfsApi, C: Clock>(
    zfs_api: &A,
    clock: &C,
    volume: &str,
    volume_config: &VolumeConfig,
    config: &Config,
) -> Result<()> {
    let mut snapshots = zfs_api.snapshots(volume).await?;
    snapshots.sort_by_key(|snapshot| snapshot.date_time);
    snapshots.sort_by_key(|snapshot| snapshot.time_unit);

    let mut snapshot_groups = snapshots
        .linear_group_by_key(|snapshot| snapshot.time_unit)
        .peekable();

    for time_unit in TimeUnit::iter() {
        let group = match snapshot_groups.peek() {
            Some(snapshots) if snapshots[0].time_unit == time_unit => {
                snapshot_groups.next().unwrap()
            }
            _ => &[],
        };
        let desired_count = volume_config[time_unit];

        let snapshot_taken = if desired_count != 0 {
            take_snapshot_if_required(
                zfs_api,
                clock,
                &volume,
                group,
                time_unit,
                config.snapshot_prefix.0.as_str(),
            )
            .await?
        } else {
            false
        };

        maybe_remove_snapshots(zfs_api, group, desired_count, snapshot_taken).await?;
    }

    Ok(())
}

async fn maybe_remove_snapshots<A: ZfsApi>(
    zfs_api: &A,
    group: &[Snapshot],
    desired_count: u16,
    snapshot_taken: bool,
) -> Result<()> {
    let total_snapshots = group.len() + if snapshot_taken { 1 } else { 0 };

    if total_snapshots > desired_count as usize {
        let to_remove = group.split_at(total_snapshots - desired_count as usize).0;
        for snapshot in to_remove {
            zfs_api
                .remove_snapshot(snapshot)
                .await
                .with_context(|| "error removing snapshot")?;
        }
    }

    Ok(())
}

async fn take_snapshot_if_required<A: ZfsApi, C: Clock>(
    zfs_api: &A,
    clock: &C,
    volume: &str,
    group: &[Snapshot],
    time_unit: TimeUnit,
    snapshot_prefix: &str,
) -> Result<bool> {
    let now = clock.current();
    let should_take_snapshot = group
        .last()
        .map(|most_recent_snapshot| {
            let should_snapshot_starting_time = most_recent_snapshot.date_time + time_unit;
            now >= should_snapshot_starting_time
        })
        .unwrap_or(true);
    if should_take_snapshot {
        zfs_api
            .take_snapshot(&Snapshot {
                volume: SmartString::from(volume),
                prefix: SmartString::from(snapshot_prefix),
                date_time: now,
                time_unit,
            })
            .await
            .with_context(|| "error taking snapshot")?;
        Ok(true)
    } else {
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::MockClock;
    use crate::zfs::MockZfsApi;
    use chrono::{Local, NaiveDate, TimeZone};
    use mockall::predicate::eq;

    #[tokio::test]
    async fn take_snapshot_if_required_takes_a_snapshot() {
        let mut mock_api = MockZfsApi::new();
        let mut mock_clock = MockClock::new();

        mock_clock.expect_current().times(1).returning(move || {
            Local
                .from_local_datetime(&NaiveDate::from_ymd(2021, 5, 6).and_hms(7, 3, 1))
                .unwrap()
        });

        mock_api
            .expect_take_snapshot()
            .times(1)
            .with(eq(Snapshot {
                volume: CompactString::from("zvol/abc/a"),
                prefix: CompactString::from("abc123"),
                date_time: Local
                    .from_local_datetime(&NaiveDate::from_ymd(2021, 5, 6).and_hms(7, 3, 1))
                    .unwrap(),
                time_unit: TimeUnit::Minute,
            }))
            .returning(|_| Box::pin(async { Ok(()) }));

        assert!(take_snapshot_if_required(
            &mock_api,
            &mock_clock,
            "zvol/abc/a",
            &[
                Snapshot {
                    volume: CompactString::from("zvol/abc/a"),
                    prefix: CompactString::from("abc312"),
                    date_time: Local
                        .from_local_datetime(&NaiveDate::from_ymd(2021, 5, 6).and_hms(7, 1, 1))
                        .unwrap(),
                    time_unit: TimeUnit::Minute,
                },
                Snapshot {
                    volume: CompactString::from("zvol/abc/a"),
                    prefix: CompactString::from("abc321"),
                    date_time: Local
                        .from_local_datetime(&NaiveDate::from_ymd(2021, 5, 6).and_hms(7, 2, 1))
                        .unwrap(),
                    time_unit: TimeUnit::Minute,
                },
            ],
            TimeUnit::Minute,
            "abc123",
        )
        .await
        .unwrap());
    }

    #[tokio::test]
    async fn take_snapshot_if_required_takes_a_snapshot_if_no_snapshots() {
        let mut mock_api = MockZfsApi::new();
        let mut mock_clock = MockClock::new();

        mock_clock.expect_current().times(1).returning(move || {
            Local
                .from_local_datetime(&NaiveDate::from_ymd(2021, 5, 6).and_hms(7, 3, 1))
                .unwrap()
        });

        mock_api
            .expect_take_snapshot()
            .times(1)
            .with(eq(Snapshot {
                volume: CompactString::from("zvol/abc/a"),
                prefix: CompactString::from("abc123"),
                date_time: Local
                    .from_local_datetime(&NaiveDate::from_ymd(2021, 5, 6).and_hms(7, 3, 1))
                    .unwrap(),
                time_unit: TimeUnit::Minute,
            }))
            .returning(|_| Box::pin(async { Ok(()) }));

        assert!(take_snapshot_if_required(
            &mock_api,
            &mock_clock,
            "zvol/abc/a",
            &[],
            TimeUnit::Minute,
            "abc123",
        )
        .await
        .unwrap());
    }

    #[tokio::test]
    async fn take_snapshot_if_required_passes() {
        let mock_api = MockZfsApi::new();
        let mut mock_clock = MockClock::new();

        mock_clock.expect_current().times(1).returning(move || {
            Local
                .from_local_datetime(&NaiveDate::from_ymd(2021, 5, 6).and_hms(7, 2, 30))
                .unwrap()
        });

        assert!(!take_snapshot_if_required(
            &mock_api,
            &mock_clock,
            "zvol/abc/a",
            &[
                Snapshot {
                    volume: CompactString::from("zvol/abc/a"),
                    prefix: CompactString::from("abc123"),
                    date_time: Local
                        .from_local_datetime(&NaiveDate::from_ymd(2021, 5, 6).and_hms(7, 1, 1))
                        .unwrap(),
                    time_unit: TimeUnit::Minute,
                },
                Snapshot {
                    volume: CompactString::from("zvol/abc/a"),
                    prefix: CompactString::from("abc123"),
                    date_time: Local
                        .from_local_datetime(&NaiveDate::from_ymd(2021, 5, 6).and_hms(7, 2, 1))
                        .unwrap(),
                    time_unit: TimeUnit::Minute,
                },
            ],
            TimeUnit::Minute,
            "abc123",
        )
        .await
        .unwrap());
    }
}
