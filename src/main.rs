use crate::cfg::{Config, VolumeConfig};
use crate::clock::{Clock, ClockImpl};
use crate::time_unit::TimeUnit;
use crate::zfs::{DryZfsApi, ZfsApi, ZfsApiImpl};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
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
use crate::remote::{RemoteApi};
use crate::remote::RemoteCommand;

type CompactString = SmartString<LazyCompact>;

mod cfg;
mod clock;
mod remote;
mod snapshot;
mod time_unit;
mod zfs;

#[derive(StructOpt)]
#[structopt(name = "autosnapd", about = "zfs snapshot daemon")]
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

    let cfg_file_data = tokio::fs::read_to_string(opt.config)
        .await
        .with_context(|| "error reading configuration file")?;

    let cfg = cfg::load_config(cfg_file_data.as_str())?;
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
    let remote_api = RemoteCommand::new();

    loop {
        select! {
            _ = interval.tick().fuse() => handle_snapshots_for_all_volumes(cfg, zfs_api, &ClockImpl, &remote_api, &mutex).await?,
            _ = interrupt => break,
        }
    }

    Ok(())
}

async fn handle_snapshots_for_all_volumes<A: ZfsApi, C: Clock, R: RemoteApi>(
    cfg: &Config,
    zfs_api: &A,
    clock: &C,
    remote_api: &R,
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
        handle_snapshots_for_volume(zfs_api, clock, remote_api, volume, config, cfg).await?;
    }

    drop(guard);
    Ok(())
}

async fn handle_snapshots_for_volume<A: ZfsApi, C: Clock, R: RemoteApi>(
    zfs_api: &A,
    clock: &C,
    remote_api: &R,
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

        if desired_count == 0 {
            continue;
        }

        if !is_snapshot_required(clock, group, time_unit).await {
            continue;
        }

        if let Some(remote) = &volume_config.remote {
            match remote_api.sync_remote(&remote.host, &remote.remote_path, &remote.local_path).await {
                Ok(_) => log::debug!(
                    "successfully synced remote {}:{} to {}",
                    remote.host,
                    remote.remote_path,
                    &remote.local_path
                ),
                Err(e) => {
                    log::error!(
                        "failed to sync remote {}:{} to {}: {}",
                        remote.host,
                        remote.remote_path,
                        &remote.local_path,
                        e
                    );
                    return Ok(());
                }
            }
        }

        let snapshot_taken = take_snapshot(
            zfs_api,
            volume,
            config.snapshot_prefix.0.as_str(),
            time_unit,
            clock.current(),
        )
        .await?;

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
        let to_remove = group
            .split_at(usize::min(
                total_snapshots - desired_count as usize,
                group.len(),
            ))
            .0;
        for snapshot in to_remove {
            zfs_api
                .remove_snapshot(snapshot)
                .await
                .with_context(|| "error removing snapshot")?;
        }
    }

    Ok(())
}

async fn is_snapshot_required<C: Clock>(
    clock: &C,
    group: &[Snapshot],
    time_unit: TimeUnit,
) -> bool {
    let now = clock.current();
    group
        .last()
        .map(|most_recent_snapshot| {
            let should_snapshot_starting_time = most_recent_snapshot.date_time + time_unit;
            now >= should_snapshot_starting_time
        })
        .unwrap_or(true)
}

async fn take_snapshot<A: ZfsApi>(
    zfs_api: &A,
    volume: &str,
    snapshot_prefix: &str,
    time_unit: TimeUnit,
    date_time: DateTime<Utc>,
) -> Result<bool> {
    let snapshot = Snapshot {
        volume: SmartString::from(volume),
        prefix: SmartString::from(snapshot_prefix),
        date_time,
        time_unit,
    };
    if !snapshot.is_valid() {
        log::warn!("not taking snapshot, snapshot is invalid: {}", snapshot);
        return Ok(false);
    }
    zfs_api
        .take_snapshot(&snapshot)
        .await
        .with_context(|| "error taking snapshot")?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::{RemoteConfig, SnapshotPrefix};
    use crate::clock::MockClock;
    use crate::remote::MockRemoteApi;
    use crate::zfs::MockZfsApi;
    use chrono::{NaiveDate, TimeZone, Utc};
    use mockall::predicate::eq;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_is_snapshot_required_returns_true_for_empty_group() {
        let mut mock_clock = MockClock::new();
        mock_clock.expect_current().times(1).returning(move || {
            Utc.from_utc_datetime(
                &NaiveDate::from_ymd_opt(2021, 5, 6)
                    .unwrap()
                    .and_hms_opt(7, 3, 1)
                    .unwrap(),
            )
        });

        assert!(is_snapshot_required(&mock_clock, &[], TimeUnit::Minute).await);
    }

    #[tokio::test]
    async fn test_is_snapshot_required_returns_true_when_time_elapsed() {
        let mut mock_clock = MockClock::new();
        mock_clock.expect_current().times(1).returning(move || {
            Utc.from_utc_datetime(
                &NaiveDate::from_ymd_opt(2021, 5, 6)
                    .unwrap()
                    .and_hms_opt(7, 3, 1)
                    .unwrap(),
            )
        });

        assert!(
            is_snapshot_required(
                &mock_clock,
                &[Snapshot {
                    volume: CompactString::from("zvol/abc/a"),
                    prefix: CompactString::from("abc123"),
                    date_time: Utc.from_utc_datetime(
                        &NaiveDate::from_ymd_opt(2021, 5, 6)
                            .unwrap()
                            .and_hms_opt(7, 2, 0)
                            .unwrap()
                    ),
                    time_unit: TimeUnit::Minute,
                }],
                TimeUnit::Minute
            )
            .await
        );
    }

    #[tokio::test]
    async fn test_is_snapshot_required_returns_false_when_time_not_elapsed() {
        let mut mock_clock = MockClock::new();
        mock_clock.expect_current().times(1).returning(move || {
            Utc.from_utc_datetime(
                &NaiveDate::from_ymd_opt(2021, 5, 6)
                    .unwrap()
                    .and_hms_opt(7, 2, 30)
                    .unwrap(),
            )
        });

        assert!(
            !is_snapshot_required(
                &mock_clock,
                &[Snapshot {
                    volume: CompactString::from("zvol/abc/a"),
                    prefix: CompactString::from("abc123"),
                    date_time: Utc.from_utc_datetime(
                        &NaiveDate::from_ymd_opt(2021, 5, 6)
                            .unwrap()
                            .and_hms_opt(7, 2, 0)
                            .unwrap()
                    ),
                    time_unit: TimeUnit::Minute,
                }],
                TimeUnit::Minute
            )
            .await
        );
    }

    #[tokio::test]
    async fn test_take_snapshot_success() {
        let mut mock_api = MockZfsApi::new();
        let now = Utc.from_utc_datetime(
            &NaiveDate::from_ymd_opt(2021, 5, 6)
                .unwrap()
                .and_hms_opt(7, 3, 1)
                .unwrap(),
        );

        mock_api
            .expect_take_snapshot()
            .times(1)
            .with(eq(Snapshot {
                volume: CompactString::from("zvol/abc/a"),
                prefix: CompactString::from("abc123"),
                date_time: now,
                time_unit: TimeUnit::Minute,
            }))
            .returning(|_| Box::pin(async { Ok(()) }));

        assert!(
            take_snapshot(&mock_api, "zvol/abc/a", "abc123", TimeUnit::Minute, now)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_handle_snapshots_for_volume_with_remote() {
        let mut mock_zfs = MockZfsApi::new();
        let mut mock_clock = MockClock::new();
        let mut mock_remote = MockRemoteApi::new();
        
        let now = Utc.from_utc_datetime(
            &NaiveDate::from_ymd_opt(2021, 5, 6)
                .unwrap()
                .and_hms_opt(7, 3, 1)
                .unwrap(),
        );

        mock_clock.expect_current().times(2).returning(move || now);

        mock_remote
            .expect_sync_remote()
            .with(
                eq("server1.example.com"),
                eq("/backup"),
                eq("/local/backup")
            )
            .times(1)
            .returning(|_, _, _| Box::pin(async { Ok(()) }));

        mock_zfs
            .expect_snapshots()
            .with(eq("tank/data"))
            .times(1)
            .returning(|_| Box::pin(async { Ok(vec![]) }));

        mock_zfs
            .expect_take_snapshot()
            .times(1)
            .returning(|_| Box::pin(async { Ok(()) }));

        let volume_config = VolumeConfig {
            minutely: 1,
            hourly: 0,
            daily: 0,
            monthly: 0,
            yearly: 0,
            remote: Some(RemoteConfig {
                host: CompactString::from("server1.example.com"),
                remote_path: CompactString::from("/backup"),
                local_path: CompactString::from("/local/backup"),
            }),
        };

        let config = Config {
            snapshot_prefix: SnapshotPrefix(CompactString::from("autosnap")),
            volume_config: HashMap::new(),
        };

        let result = handle_snapshots_for_volume(
            &mock_zfs,
            &mock_clock,
            &mock_remote,
            "tank/data",
            &volume_config,
            &config,
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_handle_snapshots_for_volume_with_remote_failure() {
        let mut mock_zfs = MockZfsApi::new();
        let mut mock_clock = MockClock::new();
        let mut mock_remote = MockRemoteApi::new();
        
        let now = Utc.from_utc_datetime(
            &NaiveDate::from_ymd_opt(2021, 5, 6)
                .unwrap()
                .and_hms_opt(7, 3, 1)
                .unwrap(),
        );

        mock_clock.expect_current().times(1).returning(move || now);

        mock_remote
            .expect_sync_remote()
            .times(1)
            .returning(|_, _, _| Box::pin(async { Err(anyhow::anyhow!("Remote sync failed")) }));

        mock_zfs
            .expect_snapshots()
            .with(eq("tank/data"))
            .times(1)
            .returning(|_| Box::pin(async { Ok(vec![]) }));

        mock_zfs.expect_take_snapshot().times(0);

        let volume_config = VolumeConfig {
            minutely: 1,
            hourly: 0,
            daily: 0,
            monthly: 0,
            yearly: 0,
            remote: Some(RemoteConfig {
                host: CompactString::from("server1.example.com"),
                remote_path: CompactString::from("/backup"),
                local_path: CompactString::from("/local/backup"),
            }),
        };

        let config = Config {
            snapshot_prefix: SnapshotPrefix(CompactString::from("autosnap")),
            volume_config: HashMap::new(),
        };

        let result = handle_snapshots_for_volume(
            &mock_zfs,
            &mock_clock,
            &mock_remote,
            "tank/data",
            &volume_config,
            &config,
        )
        .await;

        assert!(result.is_ok());
    }
}
