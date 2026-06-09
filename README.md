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

[templates.archive]
daily = 60
monthly = 60

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
source = { type = "script", path = "/usr/local/bin/prepare-backup.sh", args = ["--target", "/mnt/data"] }
# Snapshot retention is separately configured for the replica
replication = { host = "backup-server", dataset = "backup/tank/data", template = "archive", daily = 120 }

# A volume using a remote rsync source
["tank/remote-sync"]
template = "replicated_policy"
source = { type = "remote", host = "mirror.example.com", remote_path = "/mnt/mirror", local_path = "/mnt/local" }
```

### Scenarios

**Scenario 1: Local Retention Only**
Simply define the number of snapshots to keep for different time units (e.g., `daily = 7`). The daemon will automatically prune old local snapshots when the limit is exceeded.

**Scenario 2: Secure Replication Workflow**
Define a `source` that performs an `rsync` or `zfs send/receive` precursor, followed by a `replication` block. The daemon ensures the source command completes successfully before proceeding to take the snapshot and replicate it to the remote host.

## autosnapd-ssh-command

A security utility designed to be used with `sshd` to allow strictly controlled ZFS operations via SSH. It only permits commands that match the format sent by the `autosnapd` replication engine, preventing arbitrary command execution.

### Secure SSH Configuration

To use this utility for a dedicated `zfs` user, add the following to your `/etc/ssh/sshd_config`:

```ssh
Match User zfs
    ForceCommand /usr/bin/autosnapd-ssh-command
    PermitTTY no
    X11Forwarding no
    AllowTcpForwarding no 
```

`/usr/bin/autosnapd-ssh-command` requires that the `zfs` user be a sudoer with no password prompt.

## Dependencies

* `zfs`
* `sudo` (for source execution and command enforcement)
* `rsync` (if using remote source paths)
