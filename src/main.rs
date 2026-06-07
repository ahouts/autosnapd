use crate::cfg::{Config, VolumeConfig};
use crate::clock::{Clock, ClockImpl};
use crate::remote::{DryReplicationApi, DrySourceApi, RemoteCommand, ReplicationApi, SourceApi};
use crate::time_unit::TimeUnit;
use crate::zfs::{DryZfsApi, ZfsApi, ZfsApiImpl};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures::FutureExt;
use futures::lock::Mutex;
use futures::select;
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
    let zfs_api = ZfsApiImpl::new(opt.zfs_path.clone());

    if opt.dry {
        let source_api = DrySourceApi(RemoteCommand::new(opt.zfs_path.clone()));
        let replication_api = DryReplicationApi(RemoteCommand::new(opt.zfs_path));
        main_loop(&cfg, &DryZfsApi(zfs_api), &source_api, &replication_api).await?;
    } else {
        let source_api = RemoteCommand::new(opt.zfs_path);
        main_loop(&cfg, &zfs_api, &source_api, &source_api).await?;
    }

    Ok(())
}

async fn main_loop<A: ZfsApi, S: SourceApi, P: ReplicationApi>(
    cfg: &Config,
    zfs_api: &A,
    source_api: &S,
    replication_api: &P,
) -> Result<()> {
    let mut interval = tokio::time::interval(Duration::new(60, 0));
    let mut interrupt = Box::pin(tokio::signal::ctrl_c().fuse());
    let mutex = Mutex::new(());

    loop {
        select! {
            _ = interval.tick().fuse() => handle_snapshots_for_all_volumes(cfg, zfs_api, &ClockImpl, source_api, replication_api, &mutex).await?,
            _ = interrupt => break,
        }
    }

    Ok(())
}

async fn handle_snapshots_for_all_volumes<A: ZfsApi, C: Clock, S: SourceApi, P: ReplicationApi>(
    cfg: &Config,
    zfs_api: &A,
    clock: &C,
    source_api: &S,
    replication_api: &P,
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
        handle_snapshots_for_volume(
            zfs_api,
            clock,
            source_api,
            replication_api,
            volume,
            config,
            cfg,
        )
        .await?;
    }

    drop(guard);
    Ok(())
}

async fn handle_snapshots_for_volume<A: ZfsApi, C: Clock, S: SourceApi, P: ReplicationApi>(
    zfs_api: &A,
    clock: &C,
    source_api: &S,
    replication_api: &P,
    volume: &str,
    volume_config: &VolumeConfig,
    config: &Config,
) -> Result<()> {
    let mut snapshots = zfs_api.snapshots(volume).await?;
    let mut snapshots_for_replication = snapshots.clone();
    sort_snapshots(&mut snapshots);
    let mut prune_work = Vec::new();

    for time_unit in TimeUnit::iter() {
        let group = snapshots_for_time_unit(&snapshots, time_unit);
        let desired_count = volume_config[time_unit];

        if desired_count == 0 {
            continue;
        }

        if !is_snapshot_required(clock, &group, time_unit).await {
            continue;
        }

        if let Some(source) = &volume_config.source {
            match source_api.run_source(source).await {
                Ok(_) => log::debug!("successfully ran source for {}", volume),
                Err(e) => {
                    log::error!("failed to run source for {}", volume);
                    log_error_chain(&e);
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

        let snapshot_taken = if let Some(snapshot) = snapshot_taken {
            snapshots.push(snapshot.clone());
            snapshots_for_replication.push(snapshot);
            sort_snapshots(&mut snapshots);
            true
        } else {
            false
        };
        prune_work.push((group, desired_count, snapshot_taken));
    }

    if let Some(replication) = &volume_config.replication {
        match replication_api
            .replicate_snapshots(
                volume,
                &replication.host,
                &replication.dataset,
                snapshots_for_replication.as_slice(),
            )
            .await
        {
            Ok(_) => log::debug!(
                "successfully replicated snapshots for {} to {}:{}",
                volume,
                replication.host,
                replication.dataset
            ),
            Err(e) => {
                log::error!(
                    "failed to replicate snapshots for {} to {}:{}",
                    volume,
                    replication.host,
                    replication.dataset
                );
                log_error_chain(&e);
                return Ok(());
            }
        }

        if let Err(e) = replication_api.prune_snapshots(replication).await {
            log::error!(
                "failed to prune remote snapshots for {}:{}",
                replication.host,
                replication.dataset
            );
            log_error_chain(&e);
        }
    }

    for (group, desired_count, snapshot_taken) in prune_work {
        maybe_remove_snapshots(zfs_api, group.as_slice(), desired_count, snapshot_taken).await?;
    }

    Ok(())
}

fn log_error_chain(err: &anyhow::Error) {
    for (i, cause) in err.chain().enumerate() {
        if i == 0 {
            log::error!("Error: {}", cause);
        } else {
            log::error!("  Caused by: {}", cause);
        }
    }
}

fn sort_snapshots(snapshots: &mut [Snapshot]) {
    snapshots.sort_by_key(|snapshot| snapshot.date_time);
    snapshots.sort_by_key(|snapshot| snapshot.time_unit);
}

fn snapshots_for_time_unit(snapshots: &[Snapshot], time_unit: TimeUnit) -> Vec<Snapshot> {
    snapshots
        .linear_group_by_key(|snapshot| snapshot.time_unit)
        .find(|group| group[0].time_unit == time_unit)
        .unwrap_or(&[])
        .to_vec()
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
) -> Result<Option<Snapshot>> {
    let snapshot = Snapshot {
        volume: SmartString::from(volume),
        prefix: SmartString::from(snapshot_prefix),
        date_time,
        time_unit,
    };
    if !snapshot.is_valid() {
        log::warn!("not taking snapshot, snapshot is invalid: {}", snapshot);
        return Ok(None);
    }
    zfs_api
        .take_snapshot(&snapshot)
        .await
        .with_context(|| "error taking snapshot")?;
    Ok(Some(snapshot))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::{RemoteConfig, ReplicationConfig, ScriptConfig, SnapshotPrefix, SourceConfig};
    use crate::clock::MockClock;
    use crate::remote::{MockReplicationApi, MockSourceApi};
    use crate::zfs::MockZfsApi;
    use chrono::{NaiveDate, TimeZone, Utc};
    use mockall::Sequence;
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

        assert_eq!(
            Some(Snapshot {
                volume: CompactString::from("zvol/abc/a"),
                prefix: CompactString::from("abc123"),
                date_time: now,
                time_unit: TimeUnit::Minute,
            }),
            take_snapshot(&mock_api, "zvol/abc/a", "abc123", TimeUnit::Minute, now)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_handle_snapshots_for_volume_with_remote_source() {
        let mut mock_zfs = MockZfsApi::new();
        let mut mock_clock = MockClock::new();
        let mut mock_source = MockSourceApi::new();
        let mock_replication = MockReplicationApi::new();

        let now = Utc.from_utc_datetime(
            &NaiveDate::from_ymd_opt(2021, 5, 6)
                .unwrap()
                .and_hms_opt(7, 3, 1)
                .unwrap(),
        );

        mock_clock.expect_current().times(2).returning(move || now);

        let source = remote_source_config();
        mock_source
            .expect_run_source()
            .with(eq(source.clone()))
            .times(1)
            .returning(|_| Box::pin(async { Ok(()) }));

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
            source: Some(source),
            replication: None,
        };

        let config = Config {
            snapshot_prefix: SnapshotPrefix(CompactString::from("autosnap")),
            volume_config: HashMap::new(),
        };

        let result = handle_snapshots_for_volume(
            &mock_zfs,
            &mock_clock,
            &mock_source,
            &mock_replication,
            "tank/data",
            &volume_config,
            &config,
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_handle_snapshots_for_volume_with_source_failure() {
        let mut mock_zfs = MockZfsApi::new();
        let mut mock_clock = MockClock::new();
        let mut mock_source = MockSourceApi::new();
        let mock_replication = MockReplicationApi::new();

        let now = Utc.from_utc_datetime(
            &NaiveDate::from_ymd_opt(2021, 5, 6)
                .unwrap()
                .and_hms_opt(7, 3, 1)
                .unwrap(),
        );

        mock_clock.expect_current().times(1).returning(move || now);

        mock_source
            .expect_run_source()
            .times(1)
            .returning(|_| Box::pin(async { Err(anyhow::anyhow!("source failed")) }));

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
            source: Some(remote_source_config()),
            replication: None,
        };

        let config = Config {
            snapshot_prefix: SnapshotPrefix(CompactString::from("autosnap")),
            volume_config: HashMap::new(),
        };

        let result = handle_snapshots_for_volume(
            &mock_zfs,
            &mock_clock,
            &mock_source,
            &mock_replication,
            "tank/data",
            &volume_config,
            &config,
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_handle_snapshots_for_volume_with_script_source() {
        let mut mock_zfs = MockZfsApi::new();
        let mut mock_clock = MockClock::new();
        let mut mock_source = MockSourceApi::new();
        let mock_replication = MockReplicationApi::new();

        let now = Utc.from_utc_datetime(
            &NaiveDate::from_ymd_opt(2021, 5, 6)
                .unwrap()
                .and_hms_opt(7, 3, 1)
                .unwrap(),
        );

        mock_clock.expect_current().times(2).returning(move || now);

        let source = SourceConfig::Script(ScriptConfig {
            path: "/usr/local/bin/build-volume".into(),
            args: vec!["--target".into(), "/mnt/data".into()],
        });
        mock_source
            .expect_run_source()
            .with(eq(source.clone()))
            .times(1)
            .returning(|_| Box::pin(async { Ok(()) }));

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
            source: Some(source),
            ..VolumeConfig::default()
        };

        let config = Config {
            snapshot_prefix: SnapshotPrefix(CompactString::from("autosnap")),
            volume_config: HashMap::new(),
        };

        let result = handle_snapshots_for_volume(
            &mock_zfs,
            &mock_clock,
            &mock_source,
            &mock_replication,
            "tank/data",
            &volume_config,
            &config,
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_handle_snapshots_for_volume_replicates_before_pruning() {
        let mut mock_zfs = MockZfsApi::new();
        let mut mock_clock = MockClock::new();
        let mut mock_source = MockSourceApi::new();
        let mut mock_replication = MockReplicationApi::new();
        let mut sequence = Sequence::new();

        let old = test_snapshot("tank/data", 7, 2, 0, TimeUnit::Minute);
        let new = test_snapshot("tank/data", 7, 3, 1, TimeUnit::Minute);
        let now = new.date_time;

        mock_clock.expect_current().times(2).returning(move || now);
        mock_source.expect_run_source().times(0);
        mock_zfs
            .expect_snapshots()
            .with(eq("tank/data"))
            .times(1)
            .returning(move |_| {
                let snapshots = vec![old.clone()];
                Box::pin(async move { Ok(snapshots) })
            });
        mock_zfs
            .expect_take_snapshot()
            .with(eq(new.clone()))
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_| Box::pin(async { Ok(()) }));
        mock_replication
            .expect_replicate_snapshots()
            .withf(move |volume, host, dataset, snapshots| {
                volume == "tank/data"
                    && host == "backup.example.com"
                    && dataset == "backup/tank/data"
                    && snapshots.len() == 2
                    && snapshots.iter().any(|snapshot| snapshot == &new)
            })
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_, _, _, _| Box::pin(async { Ok(()) }));
        mock_replication
            .expect_prune_snapshots()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_| Box::pin(async { Ok(()) }));
        mock_zfs
            .expect_remove_snapshot()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_| Box::pin(async { Ok(()) }));

        let volume_config = VolumeConfig {
            minutely: 1,
            replication: Some(replication_config()),
            ..VolumeConfig::default()
        };

        let config = Config {
            snapshot_prefix: SnapshotPrefix(CompactString::from("autosnap")),
            volume_config: HashMap::new(),
        };

        let result = handle_snapshots_for_volume(
            &mock_zfs,
            &mock_clock,
            &mock_source,
            &mock_replication,
            "tank/data",
            &volume_config,
            &config,
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_handle_snapshots_for_volume_skips_prune_when_replication_fails() {
        let mut mock_zfs = MockZfsApi::new();
        let mut mock_clock = MockClock::new();
        let mut mock_source = MockSourceApi::new();
        let mut mock_replication = MockReplicationApi::new();

        let old = test_snapshot("tank/data", 7, 2, 0, TimeUnit::Minute);
        let new = test_snapshot("tank/data", 7, 3, 1, TimeUnit::Minute);
        let now = new.date_time;

        mock_clock.expect_current().times(2).returning(move || now);
        mock_source.expect_run_source().times(0);
        mock_zfs
            .expect_snapshots()
            .with(eq("tank/data"))
            .times(1)
            .returning(move |_| {
                let snapshots = vec![old.clone()];
                Box::pin(async move { Ok(snapshots) })
            });
        mock_zfs
            .expect_take_snapshot()
            .with(eq(new))
            .times(1)
            .returning(|_| Box::pin(async { Ok(()) }));
        mock_replication
            .expect_replicate_snapshots()
            .times(1)
            .returning(|_, _, _, _| Box::pin(async { Err(anyhow::anyhow!("replication failed")) }));
        mock_replication.expect_prune_snapshots().times(0);
        mock_zfs.expect_remove_snapshot().times(0);

        let volume_config = VolumeConfig {
            minutely: 1,
            replication: Some(replication_config()),
            ..VolumeConfig::default()
        };

        let config = Config {
            snapshot_prefix: SnapshotPrefix(CompactString::from("autosnap")),
            volume_config: HashMap::new(),
        };

        let result = handle_snapshots_for_volume(
            &mock_zfs,
            &mock_clock,
            &mock_source,
            &mock_replication,
            "tank/data",
            &volume_config,
            &config,
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_handle_snapshots_for_volume_retries_replication_when_no_snapshot_is_due() {
        let mut mock_zfs = MockZfsApi::new();
        let mut mock_clock = MockClock::new();
        let mut mock_source = MockSourceApi::new();
        let mut mock_replication = MockReplicationApi::new();

        let current = test_snapshot("tank/data", 7, 3, 0, TimeUnit::Minute);
        let now = Utc.from_utc_datetime(
            &NaiveDate::from_ymd_opt(2021, 5, 6)
                .unwrap()
                .and_hms_opt(7, 3, 30)
                .unwrap(),
        );

        mock_clock.expect_current().times(1).returning(move || now);
        mock_source.expect_run_source().times(0);
        mock_zfs
            .expect_snapshots()
            .with(eq("tank/data"))
            .times(1)
            .returning(move |_| {
                let snapshots = vec![current.clone()];
                Box::pin(async move { Ok(snapshots) })
            });
        mock_zfs.expect_take_snapshot().times(0);
        mock_zfs.expect_remove_snapshot().times(0);
        mock_replication
            .expect_replicate_snapshots()
            .withf(|volume, host, dataset, snapshots| {
                volume == "tank/data"
                    && host == "backup.example.com"
                    && dataset == "backup/tank/data"
                    && snapshots.len() == 1
            })
            .times(1)
            .returning(|_, _, _, _| Box::pin(async { Ok(()) }));
        mock_replication
            .expect_prune_snapshots()
            .times(1)
            .returning(|_| Box::pin(async { Ok(()) }));

        let volume_config = VolumeConfig {
            minutely: 1,
            replication: Some(replication_config()),
            ..VolumeConfig::default()
        };

        let config = Config {
            snapshot_prefix: SnapshotPrefix(CompactString::from("autosnap")),
            volume_config: HashMap::new(),
        };

        let result = handle_snapshots_for_volume(
            &mock_zfs,
            &mock_clock,
            &mock_source,
            &mock_replication,
            "tank/data",
            &volume_config,
            &config,
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_handle_snapshots_for_volume_preserves_zfs_order_for_replication() {
        let mut mock_zfs = MockZfsApi::new();
        let mut mock_clock = MockClock::new();
        let mut mock_source = MockSourceApi::new();
        let mut mock_replication = MockReplicationApi::new();

        let daily = test_snapshot("tank/data", 7, 3, 0, TimeUnit::Day);
        let hourly = test_snapshot("tank/data", 7, 3, 0, TimeUnit::Hour);
        let now = Utc.from_utc_datetime(
            &NaiveDate::from_ymd_opt(2021, 5, 6)
                .unwrap()
                .and_hms_opt(7, 3, 30)
                .unwrap(),
        );

        mock_clock.expect_current().times(2).returning(move || now);
        mock_source.expect_run_source().times(0);
        mock_zfs
            .expect_snapshots()
            .with(eq("tank/data"))
            .times(1)
            .returning(move |_| {
                let snapshots = vec![daily.clone(), hourly.clone()];
                Box::pin(async move { Ok(snapshots) })
            });
        mock_zfs.expect_take_snapshot().times(0);
        mock_zfs.expect_remove_snapshot().times(0);
        mock_replication
            .expect_replicate_snapshots()
            .withf(|volume, host, dataset, snapshots| {
                volume == "tank/data"
                    && host == "backup.example.com"
                    && dataset == "backup/tank/data"
                    && snapshots.len() == 2
                    && snapshots[0].time_unit == TimeUnit::Day
                    && snapshots[1].time_unit == TimeUnit::Hour
            })
            .times(1)
            .returning(|_, _, _, _| Box::pin(async { Ok(()) }));
        mock_replication
            .expect_prune_snapshots()
            .times(1)
            .returning(|_| Box::pin(async { Ok(()) }));

        let volume_config = VolumeConfig {
            hourly: 1,
            daily: 1,
            replication: Some(replication_config()),
            ..VolumeConfig::default()
        };

        let config = Config {
            snapshot_prefix: SnapshotPrefix(CompactString::from("autosnap")),
            volume_config: HashMap::new(),
        };

        let result = handle_snapshots_for_volume(
            &mock_zfs,
            &mock_clock,
            &mock_source,
            &mock_replication,
            "tank/data",
            &volume_config,
            &config,
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_handle_snapshots_for_volume_prunes_local_when_remote_prune_fails() {
        let mut mock_zfs = MockZfsApi::new();
        let mut mock_clock = MockClock::new();
        let mut mock_source = MockSourceApi::new();
        let mut mock_replication = MockReplicationApi::new();
        let mut sequence = Sequence::new();

        let old = test_snapshot("tank/data", 7, 2, 0, TimeUnit::Minute);
        let new = test_snapshot("tank/data", 7, 3, 1, TimeUnit::Minute);
        let now = new.date_time;

        mock_clock.expect_current().times(2).returning(move || now);
        mock_source.expect_run_source().times(0);
        mock_zfs
            .expect_snapshots()
            .with(eq("tank/data"))
            .times(1)
            .returning(move |_| {
                let snapshots = vec![old.clone()];
                Box::pin(async move { Ok(snapshots) })
            });
        mock_zfs
            .expect_take_snapshot()
            .with(eq(new))
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_| Box::pin(async { Ok(()) }));
        mock_replication
            .expect_replicate_snapshots()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_, _, _, _| Box::pin(async { Ok(()) }));
        mock_replication
            .expect_prune_snapshots()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_| Box::pin(async { Err(anyhow::anyhow!("remote prune failed")) }));
        mock_zfs
            .expect_remove_snapshot()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_| Box::pin(async { Ok(()) }));

        let volume_config = VolumeConfig {
            minutely: 1,
            replication: Some(replication_config()),
            ..VolumeConfig::default()
        };

        let config = Config {
            snapshot_prefix: SnapshotPrefix(CompactString::from("autosnap")),
            volume_config: HashMap::new(),
        };

        let result = handle_snapshots_for_volume(
            &mock_zfs,
            &mock_clock,
            &mock_source,
            &mock_replication,
            "tank/data",
            &volume_config,
            &config,
        )
        .await;

        assert!(result.is_ok());
    }

    fn replication_config() -> ReplicationConfig {
        ReplicationConfig {
            host: CompactString::from("backup.example.com"),
            dataset: CompactString::from("backup/tank/data"),
            minutely: 0,
            hourly: 0,
            daily: 0,
            monthly: 0,
            yearly: 0,
        }
    }

    fn remote_source_config() -> SourceConfig {
        SourceConfig::Remote(RemoteConfig {
            host: CompactString::from("server1.example.com"),
            remote_path: CompactString::from("/backup"),
            local_path: CompactString::from("/local/backup"),
        })
    }

    fn test_snapshot(
        volume: &str,
        hour: u32,
        minute: u32,
        second: u32,
        time_unit: TimeUnit,
    ) -> Snapshot {
        Snapshot {
            volume: CompactString::from(volume),
            prefix: CompactString::from("autosnap"),
            date_time: Utc.from_utc_datetime(
                &NaiveDate::from_ymd_opt(2021, 5, 6)
                    .unwrap()
                    .and_hms_opt(hour, minute, second)
                    .unwrap(),
            ),
            time_unit,
        }
    }
}
