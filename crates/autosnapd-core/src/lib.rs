pub type CompactString = smartstring::SmartString<smartstring::LazyCompact>;

pub mod cfg;
pub mod receive_validation;
pub mod remote_command;
pub mod snapshot;
pub mod time_unit;

pub use cfg::{
    Config, RemoteConfig, ReplicationConfig, ScriptConfig, SnapshotPrefix, SourceConfig,
    VolumeConfig, load_config,
};
pub use receive_validation::{
    max_clock_skew, validate_replica_target, validate_snapshot_spacing,
    validate_snapshot_timestamp,
};
pub use remote_command::{
    RemoteZfsCommand, is_dataset_missing_error, parse_forced_remote_zfs_command,
    parse_receive_resume_token, remote_receive_args, remote_receive_resume_token_args,
    remote_receive_snapshot_args, remote_snapshot_list_args, validate_dataset_name,
};
pub use snapshot::Snapshot;
pub use time_unit::TimeUnit;
