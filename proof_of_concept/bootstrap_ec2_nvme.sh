#!/bin/bash
# bootstrap_ec2_nvme.sh
# -----------------------------------------------------------------------------
# Prepare an AWS EC2 instance-store NVMe disk to host the PostgreSQL 18 data
# directory for the io_method benchmark.
#
# WHY: The benchmark's single variable is io_method (sync/worker/io_uring). On
# EC2, instance-store NVMe is *physically attached* to the host (no network hop,
# unlike EBS), so it isolates the database's own I/O path instead of measuring
# AWS storage-fabric latency. Recommended instance: m6id.2xlarge (8 vCPU/32GB +
# 1x474GB local NVMe), which mirrors the original VM specs.
#
# IMPORTANT: instance-store NVMe is EPHEMERAL. Its contents are wiped on every
# stop/terminate (a plain reboot is fine). Run this script once per fresh
# instance start, then rebuild the TPC-H DB from dbgen via the normal scripts.
#
# ORDER OF OPERATIONS on a new instance:
#   1. ./install_dependencies.sh      # installs PG18, creates default cluster
#   2. sudo ./bootstrap_ec2_nvme.sh   # THIS script: move data dir onto NVMe
#   3. (edit postgresql.conf for SSD yourself: random_page_cost,
#      effective_io_concurrency, etc.)
#   4. sudo ./configure_pg_modes.sh   # set up sync/worker/io_uring modes
#   5. ./run_setup.sh / run_randomized_experiment.sh
#
# This script is idempotent within a single boot: if the NVMe is already
# mounted and the data dir already lives there, it exits cleanly.
# -----------------------------------------------------------------------------
set -euo pipefail

PG_VERSION=18
CLUSTER=main
MOUNT_POINT="/mnt/pgdata"
NEW_DATA_DIR="${MOUNT_POINT}/${PG_VERSION}/${CLUSTER}"
OLD_DATA_DIR="/var/lib/postgresql/${PG_VERSION}/${CLUSTER}"
CONF_FILE="/etc/postgresql/${PG_VERSION}/${CLUSTER}/postgresql.conf"
FS_LABEL="pgnvme"

log()   { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"; }
error() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $1" >&2; exit 1; }

if [[ $EUID -ne 0 ]]; then
    error "This script must be run as root (sudo)."
fi

# -----------------------------------------------------------------------------
# 1. Identify the instance-store NVMe device.
#
# On Nitro instances the root EBS volume and any instance-store volumes all show
# up as /dev/nvme*n1. We pick the local instance storage by model string
# ("Amazon EC2 NVMe Instance Storage") rather than by device name, which is not
# stable. The root EBS reports "Amazon Elastic Block Store".
# -----------------------------------------------------------------------------
detect_nvme() {
    local dev
    for dev in /dev/nvme*n1; do
        [[ -e "$dev" ]] || continue
        local model
        model=$(nvme id-ctrl "$dev" 2>/dev/null | awk -F: '/^mn /{print $2}' | xargs || true)
        if [[ "$model" == *"Instance Storage"* ]]; then
            echo "$dev"
            return 0
        fi
    done
    return 1
}

log "Detecting instance-store NVMe device..."
if ! command -v nvme >/dev/null 2>&1; then
    log "nvme-cli not found; installing..."
    apt-get update -qq && apt-get install -y nvme-cli >/dev/null
fi

NVME_DEV="$(detect_nvme || true)"
if [[ -z "${NVME_DEV}" ]]; then
    error "No instance-store NVMe found. Are you on an instance with local NVMe (e.g. m6id.2xlarge)? Check 'sudo nvme list'."
fi
log "Found instance-store NVMe: ${NVME_DEV}"

# -----------------------------------------------------------------------------
# 2. Format + mount the NVMe (only if not already our pg volume).
#
# We use the whole device (no partition table) and label it so we can recognise
# it on a later boot. We deliberately do NOT add it to /etc/fstab with a hard
# requirement, because the device is ephemeral and a missing/zeroed disk on next
# boot would otherwise block startup; instead we mount here each run.
# -----------------------------------------------------------------------------
CURRENT_LABEL="$(blkid -s LABEL -o value "${NVME_DEV}" 2>/dev/null || true)"

if mountpoint -q "${MOUNT_POINT}" && [[ "${CURRENT_LABEL}" == "${FS_LABEL}" ]]; then
    log "${MOUNT_POINT} is already mounted from our NVMe volume; skipping format/mount."
else
    if [[ "${CURRENT_LABEL}" == "${FS_LABEL}" ]]; then
        log "NVMe already formatted with label ${FS_LABEL}; mounting without reformat."
    else
        log "Formatting ${NVME_DEV} as ext4 (label=${FS_LABEL})..."
        # -F: force (no partition table); -m 0: no reserved blocks (data disk, not root)
        mkfs.ext4 -F -m 0 -L "${FS_LABEL}" "${NVME_DEV}"
    fi

    log "Mounting ${NVME_DEV} at ${MOUNT_POINT}..."
    mkdir -p "${MOUNT_POINT}"
    # noatime: benchmark disk, don't waste IOPS updating access times.
    mount -o noatime "${NVME_DEV}" "${MOUNT_POINT}"
fi

# -----------------------------------------------------------------------------
# 3. Stop PostgreSQL before touching the data directory.
# -----------------------------------------------------------------------------
log "Stopping PostgreSQL..."
systemctl stop postgresql 2>/dev/null || true
# pg_ctlcluster path is what Debian's systemd unit drives; make sure it's down.
pg_ctlcluster "${PG_VERSION}" "${CLUSTER}" stop --force 2>/dev/null || true
sleep 2

# -----------------------------------------------------------------------------
# 4. Provision the data directory on the NVMe.
#
# If the freshly-installed cluster data still lives on the root volume, move it
# once to seed correct ownership/layout. On subsequent fresh boots the NVMe is
# empty, so we initdb a clean cluster instead (the experiment rebuilds the DB
# from dbgen anyway, so a clean cluster is exactly what we want).
# -----------------------------------------------------------------------------
mkdir -p "${MOUNT_POINT}/${PG_VERSION}"

if [[ -f "${NEW_DATA_DIR}/PG_VERSION" ]]; then
    log "Existing cluster data already present at ${NEW_DATA_DIR}; reusing it."
elif [[ -f "${OLD_DATA_DIR}/PG_VERSION" ]]; then
    log "Seeding data dir on NVMe by copying the freshly-installed cluster..."
    # Copy rather than move so the original stays as a fallback this boot.
    cp -a "${OLD_DATA_DIR}" "${MOUNT_POINT}/${PG_VERSION}/"
    log "Copied ${OLD_DATA_DIR} -> ${NEW_DATA_DIR}"
else
    log "No existing cluster found; running initdb directly on the NVMe..."
    sudo -u postgres /usr/lib/postgresql/${PG_VERSION}/bin/initdb \
        --pgdata="${NEW_DATA_DIR}" \
        --encoding=UTF8 \
        --locale=es_CR.UTF-8
fi

log "Ensuring correct ownership/permissions on ${NEW_DATA_DIR}..."
chown -R postgres:postgres "${MOUNT_POINT}/${PG_VERSION}"
chmod 700 "${NEW_DATA_DIR}"

# -----------------------------------------------------------------------------
# 5. Point the cluster's config at the NVMe data directory.
#
# Debian's postgresql.conf carries an explicit `data_directory = ...` line (see
# the project's pgtune example). We rewrite that single line so all existing
# tooling that references /etc/postgresql/18/main/* keeps working unchanged.
# -----------------------------------------------------------------------------
if [[ ! -f "${CONF_FILE}" ]]; then
    error "Config file ${CONF_FILE} not found. Run install_dependencies.sh first."
fi

log "Pointing data_directory at ${NEW_DATA_DIR} in ${CONF_FILE}..."
if grep -qE "^[[:space:]]*#?[[:space:]]*data_directory" "${CONF_FILE}"; then
    sed -i "s|^[[:space:]]*#\?[[:space:]]*data_directory.*|data_directory = '${NEW_DATA_DIR}'\t\t# moved onto instance-store NVMe by bootstrap_ec2_nvme.sh|" "${CONF_FILE}"
else
    echo "data_directory = '${NEW_DATA_DIR}'  # set by bootstrap_ec2_nvme.sh" >> "${CONF_FILE}"
fi

# -----------------------------------------------------------------------------
# 6. Start PostgreSQL and verify it is serving from the NVMe.
# -----------------------------------------------------------------------------
log "Starting PostgreSQL..."
systemctl start postgresql

log "Waiting for PostgreSQL to accept connections..."
for i in {1..30}; do
    if sudo -u postgres psql -c "SELECT 1;" >/dev/null 2>&1; then
        break
    fi
    if [[ $i -eq 30 ]]; then
        journalctl -u postgresql -n 50 --no-pager || true
        error "PostgreSQL failed to start after 30s. See logs above."
    fi
    sleep 1
done

ACTIVE_DATA_DIR="$(sudo -u postgres psql -tAc 'SHOW data_directory;' | xargs)"
if [[ "${ACTIVE_DATA_DIR}" != "${NEW_DATA_DIR}" ]]; then
    error "PostgreSQL is running from ${ACTIVE_DATA_DIR}, expected ${NEW_DATA_DIR}."
fi

log "============================================================"
log "SUCCESS"
log "  NVMe device      : ${NVME_DEV}"
log "  Mounted at       : ${MOUNT_POINT} ($(df -h "${MOUNT_POINT}" | awk 'NR==2{print $2" total, "$4" free"}'))"
log "  PG data_directory: ${ACTIVE_DATA_DIR}"
log "============================================================"
log "Reminder: this NVMe is EPHEMERAL. Re-run this script after any"
log "instance stop/terminate, then rebuild the TPC-H DB from dbgen."
log "Next: edit ${CONF_FILE} for SSD tuning, then ./configure_pg_modes.sh"
