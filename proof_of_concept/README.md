# TPC-H Database Setup Scripts

## Usage

### On Raspberry Pi or Low-Memory Systems (<2GB RAM)

```bash
./run_test.sh --low-memory
```

This will configure PostgreSQL with minimal memory settings:
- **shared_buffers**: 128MB
- **work_mem**: 4MB  
- **maintenance_work_mem**: 64MB
- **effective_cache_size**: 256MB
- **max_connections**: 20

### On Powerful Machines (4GB+ RAM)

```bash
./run_test.sh
```

This will auto-detect your system memory and configure PostgreSQL optimally:

**For 4GB+ systems:**
- **shared_buffers**: 2GB
- **work_mem**: 256MB
- **maintenance_work_mem**: 1GB
- **effective_cache_size**: 4GB

**For 2-4GB systems:**
- **shared_buffers**: 512MB
- **work_mem**: 32MB
- **maintenance_work_mem**: 256MB
- **effective_cache_size**: 1GB

## Installing Dependencies Only

If you just want to install PostgreSQL and dependencies:

```bash
# Normal mode (auto-detect)
./install_dependencies.sh

# Low-memory mode (Raspberry Pi)
./install_dependencies.sh --low-memory
```

## System Requirements

### Minimum (Raspberry Pi 3B)
- **RAM**: 1GB
- **Disk**: 5GB free space
- **OS**: Debian 11+ (Bullseye/Bookworm/Trixie)

### Recommended (Production)
- **RAM**: 4GB+
- **Disk**: 50GB+ free space
- **CPU**: 4+ cores
- **OS**: Debian 11+ or Ubuntu 20.04+

## Database Details

After successful installation:
- **Database Name**: `tpch_db`
- **Database User**: `tpch_user`
- **Password**: `tpch_password_123`
- **Port**: 5432

## Notes

- The `--low-memory` flag forces minimal PostgreSQL settings regardless of auto-detection
- Auto-detection will still apply low-memory settings on systems with <2GB RAM even without the flag
- Use `--low-memory` on Raspberry Pi to ensure optimal settings
- The script creates detailed logs in `installation.log`