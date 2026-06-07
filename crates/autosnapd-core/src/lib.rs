pub type CompactString = smartstring::SmartString<smartstring::LazyCompact>;

pub mod cfg;
pub mod remote_command;
pub mod snapshot;
pub mod time_unit;

pub use cfg::{
    Config, RemoteConfig, ReplicationConfig, ScriptConfig, SnapshotPrefix, SourceConfig,
    VolumeConfig, load_config,
};
pub use remote_command::{
    RemoteZfsCommand, parse_forced_remote_zfs_command, remote_receive_args,
    remote_receive_resume_token_args, remote_snapshot_list_args, remote_snapshot_remove_args,
    validate_dataset_name,
};
pub use snapshot::Snapshot;
pub use time_unit::TimeUnit;
