# autosnapd

A ZFS snapshot daemon with automated retention, source execution, and replication support.

## Usage

`autosnapd` runs as a loop, periodically checking volumes and performing snapshots based on a configuration file.

```bash
# Run in dry-run mode to verify configuration without making changes
autosnapd --dry --config /etc/autosnpmd.toml --zfs-path /usr/bin/zfs

# Standard execution
autosnapd --config /etc/autosnapd.toml --zfs-path /usr/bin/zfs
```

### Configuration

The configuration is defined in TOML format. Volume names are top-level keys. Each volume can use a template, define its own retention policy, run a pre-snapshot source command, and/or specify a replication target.

**Example `autosnapd.toml`:**

```toml
# prefix to prepend to snapshot names, defaults to "autosnap"
snapshot_prefix = "autosnap"

# Templates define reusable base configurations for volumes or replication
[templates.replicated_policy]
hourly = 24
daily = 7

# A replication template only needs the connection details
[templates.backup_target]
host = "backup-server"

# A volume using a template, overriding the hourly retention
["tank/data"]
template = "replicated_policy"
hourly = 1

# A volume with its own specific rotation (no template)
["tank/logs"]
daily = 30
monthly = 12

# A volume that runs a script before snapshotting and then syncing
["tank/backup-prep"]
daily = 7
monthly = 1
source = { type = "script", path = "/usr/local/bin/prepare-backup.sh", args = ["--target", "/mnt/data"] }
# Replication only pushes snapshots; retention on the replica is managed by
# the replica host itself (see prune_only below)
replication = { template = "backup_target", dataset = "backup/tank/data" }

# A volume using a remote rsync source
["tank/remote-sync"]
template = "replicated_policy"
source = { type = "remote", host = "mirror.example.com", remote_path = "/mnt/mirror", local_path = "/mnt/local" }

# A received replica dataset (on the backup server): never snapshotted,
# only pruned down to the retention counts below
["backup/tank/data"]
prune_only = true
hourly = 72
daily = 30
```

### Scenarios

**Scenario 1: Local Retention Only**
Simply define the number of snapshots to keep for different time units (e.g., `daily = 7`). The daemon will automatically prune old local snapshots when the limit is exceeded.

**Scenario 2: Secure Replication Workflow**
Define a `source` that performs an `rsync` or `zfs send/receive` precursor, followed by a `replication` block. The daemon ensures the source command completes successfully before proceeding to take the snapshot and replicate it to the remote host. The sender only ever pushes snapshots; it never deletes anything on the remote.

**Scenario 3: Replica Host**
The backup server runs its own `autosnapd` to enforce retention on received datasets. A `prune_only` volume is never snapshotted and never runs a source; on every cycle the daemon prunes each configured time unit down to the newest N snapshots. Time units left unset (or set to 0) are never touched.

```toml
["backup/tank/data"]
prune_only = true
hourly = 72
daily = 30
```

Notes:

* `prune_only` cannot be combined with `source` (configuration error). It can be combined with `replication` to chain replicas (A → B → C).
* Configure the `prune_only` volume *before* the sender's first push: the SSH command filter only accepts operations on datasets configured as `prune_only` volumes, and the first `zfs receive` from the sender then creates the dataset. The daemon treats a configured-but-not-yet-created `prune_only` dataset as having no snapshots.
* Pruning matches any snapshot of the form `<dataset>@<prefix>_<timestamp>_<unit>` regardless of the replica's `snapshot_prefix`, so the replica config does not need to mirror the sender's prefix.

## autosnapd-ssh-command

A security utility designed to be used with `sshd` to allow strictly controlled ZFS operations via SSH. It only permits commands that match the format sent by the `autosnapd` replication engine, preventing arbitrary command execution.

The permitted commands are append-only: `zfs list` (snapshots), `zfs get receive_resume_token`, and `zfs receive -s -u`. `zfs destroy` is not permitted, so a compromised sender key cannot destroy data on the replica — snapshot retention on the replica is enforced locally by its own `autosnapd` instance using `prune_only` volumes.

Beyond the command allowlist, the filter validates every request against the replica's own `autosnapd` configuration (`--config`, default `/etc/autosnapd.toml`):

* All commands are restricted to datasets configured as `prune_only` volumes on the replica.
* A receive must name the snapshot it delivers (`zfs receive -s -u <dataset>@<snapshot>`), and the name must be canonical (`<prefix>_<UTC timestamp>_<unit>`).
* The snapshot's timestamp must not be in the future (5 minutes of clock skew is tolerated — keep both hosts NTP-synced).
* The snapshot must be at least one time unit newer than the newest existing snapshot of the same unit, so a compromised sender cannot flood the replica or push the retention window around.
* A receive without a snapshot name is only accepted while the dataset holds a `receive_resume_token`, i.e. to resume an interrupted (already validated) stream.

Notes for upgrades:

* 0.7.0 changes the replication protocol: senders name the received snapshot (`zfs receive -s -u <dataset>@<snapshot>`). Sender and replica must be upgraded together — older senders are rejected by a 0.7.0 filter, and older filters reject 0.7.0 senders. The replica must also list every replicated dataset as a `prune_only` volume before the first push.
* Senders running a version older than 0.6.0 prune replicas remotely via `zfs destroy`; pointed at a 0.6.0+ replica, that command is rejected (the sender logs a non-fatal prune error each cycle until it is upgraded).

### Secure SSH Configuration

To use this utility for a dedicated `zfs` user, add the following to your `/etc/ssh/sshd_config`:

```ssh
Match User zfs
    ForceCommand /usr/bin/autosnapd-ssh-command --config /etc/autosnapd.toml
    PermitTTY no
    X11Forwarding no
    AllowTcpForwarding no 
```

`/usr/bin/autosnapd-ssh-command` requires that the `zfs` user be a sudoer with no password prompt.

## Dependencies

* `zfs`
* `sudo` (for source execution and command enforcement)
* `rsync` (if using remote source paths)
