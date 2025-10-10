#!/bin/bash
# PostgreSQL 18 and Dependency Installer for Debian
set -e
LOG_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/installation.log"
PG_VERSION=18

# Parse command line arguments
LOW_MEMORY=false
if [[ "$1" == "--low-memory" ]]; then
    LOW_MEMORY=true
fi

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $1" | tee -a "$LOG_FILE"
    exit 1
}

# Clean up any existing PostgreSQL repository configurations
cleanup_old_repos() {
    log "Cleaning up old PostgreSQL repository configurations"
    
    # Remove old repository files
    sudo rm -f /etc/apt/sources.list.d/pgdg.list
    sudo rm -f /etc/apt/sources.list.d/pgdg.list.save
    sudo rm -f /etc/apt/trusted.gpg.d/postgresql.gpg
    
    log "Old repository configurations removed"
}

# Install system dependencies
install_dependencies() {
    log "Installing system dependencies"
    
    sudo apt-get update
    sudo apt-get install -y \
        build-essential \
        wget \
        curl \
        git \
        unzip \
        make \
        gcc \
        g++ \
        libreadline-dev \
        zlib1g-dev \
        flex \
        bison \
        libxml2-dev \
        libxslt-dev \
        libssl-dev \
        libjson-c-dev \
        cmake \
        pkg-config \
        gnupg \
        lsb-release
    
    log "System dependencies installed"
}

# Install PostgreSQL 18
install_postgresql() {
    log "Installing PostgreSQL ${PG_VERSION}"
    
    # Download and install the PostgreSQL GPG key
    log "Adding PostgreSQL GPG key..."
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | \
        sudo gpg --dearmor -o /usr/share/keyrings/postgresql-archive-keyring.gpg
    
    log "GPG key added successfully"
    
    # Add PostgreSQL official repository with signed-by parameter
    log "Adding PostgreSQL repository..."
    sudo sh -c 'echo "deb [signed-by=/usr/share/keyrings/postgresql-archive-keyring.gpg] http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
    
    # Update package list
    log "Updating package lists..."
    sudo apt-get update
    
    # Install PostgreSQL
    log "Installing PostgreSQL packages..."
    sudo apt-get install -y postgresql-${PG_VERSION} postgresql-client-${PG_VERSION} \
                          postgresql-contrib-${PG_VERSION} postgresql-server-dev-${PG_VERSION}
    
    log "PostgreSQL ${PG_VERSION} installed successfully"
}

# Configure PostgreSQL
configure_postgresql() {
    log "Configuring PostgreSQL"
    
    # Start PostgreSQL service
    sudo systemctl enable postgresql
    sudo systemctl start postgresql
    
    # Check PostgreSQL status
    log "Checking PostgreSQL status..."
    sudo systemctl status postgresql 2>&1 | tee -a "$LOG_FILE" || true
    
    # Wait for PostgreSQL to be ready
    log "Waiting for PostgreSQL to be ready..."
    for i in {1..30}; do
        if sudo -u postgres psql -c "SELECT 1;" >/dev/null 2>&1; then
            log "PostgreSQL is ready"
            break
        fi
        if [ $i -eq 30 ]; then
            log "PostgreSQL failed to start. Checking logs..."
            sudo tail -50 /var/log/postgresql/postgresql-${PG_VERSION}-main.log 2>&1 | tee -a "$LOG_FILE" || true
            sudo journalctl -u postgresql -n 50 --no-pager 2>&1 | tee -a "$LOG_FILE" || true
            error "PostgreSQL failed to start after 30 seconds. Check logs above."
        fi
        sleep 1
    done
    
    # Configure PostgreSQL to allow connections
    sudo sed -i "s/#listen_addresses = 'localhost'/listen_addresses = '*'/" /etc/postgresql/${PG_VERSION}/main/postgresql.conf
    
    # Backup original pg_hba.conf
    sudo cp /etc/postgresql/${PG_VERSION}/main/pg_hba.conf /etc/postgresql/${PG_VERSION}/main/pg_hba.conf.bak
    
    # Add password authentication (only if not already present)
    if ! sudo grep -q "host all all 0.0.0.0/0 md5" /etc/postgresql/${PG_VERSION}/main/pg_hba.conf; then
        echo "host all all 0.0.0.0/0 md5" | sudo tee -a /etc/postgresql/${PG_VERSION}/main/pg_hba.conf
    fi
    
    # Get available memory in KB
    total_mem=$(grep MemTotal /proc/meminfo | awk '{print $2}')
    total_mem_mb=$((total_mem / 1024))
    
    log "Detected ${total_mem_mb}MB total memory"
    
    # Apply memory settings based on flag or auto-detection
    if [ "$LOW_MEMORY" = true ]; then
        # Force low memory settings (for Raspberry Pi)
        log "LOW MEMORY MODE: Using minimal settings for systems with <2GB RAM"
        sudo sed -i "s/^#*shared_buffers = .*/shared_buffers = 128MB/" /etc/postgresql/${PG_VERSION}/main/postgresql.conf
        sudo sed -i "s/^#*work_mem = .*/work_mem = 4MB/" /etc/postgresql/${PG_VERSION}/main/postgresql.conf
        sudo sed -i "s/^#*maintenance_work_mem = .*/maintenance_work_mem = 64MB/" /etc/postgresql/${PG_VERSION}/main/postgresql.conf
        sudo sed -i "s/^#*effective_cache_size = .*/effective_cache_size = 256MB/" /etc/postgresql/${PG_VERSION}/main/postgresql.conf
        sudo sed -i "s/^#*max_connections = .*/max_connections = 20/" /etc/postgresql/${PG_VERSION}/main/postgresql.conf
    else
        # Auto-detect or use standard settings
        if [ $total_mem_mb -lt 2048 ]; then
            # Very low memory system (< 2GB) - Raspberry Pi
            log "Auto-detected very low memory system (<2GB), using minimal settings"
            log "TIP: You can force this with --low-memory flag"
            sudo sed -i "s/^#*shared_buffers = .*/shared_buffers = 128MB/" /etc/postgresql/${PG_VERSION}/main/postgresql.conf
            sudo sed -i "s/^#*work_mem = .*/work_mem = 4MB/" /etc/postgresql/${PG_VERSION}/main/postgresql.conf
            sudo sed -i "s/^#*maintenance_work_mem = .*/maintenance_work_mem = 64MB/" /etc/postgresql/${PG_VERSION}/main/postgresql.conf
            sudo sed -i "s/^#*effective_cache_size = .*/effective_cache_size = 256MB/" /etc/postgresql/${PG_VERSION}/main/postgresql.conf
            sudo sed -i "s/^#*max_connections = .*/max_connections = 20/" /etc/postgresql/${PG_VERSION}/main/postgresql.conf
        elif [ $total_mem_mb -lt 4096 ]; then
            # Low memory system (2-4GB)
            log "Auto-detected low memory system (2-4GB), using conservative settings"
            sudo sed -i "s/^#*shared_buffers = .*/shared_buffers = 512MB/" /etc/postgresql/${PG_VERSION}/main/postgresql.conf
            sudo sed -i "s/^#*work_mem = .*/work_mem = 32MB/" /etc/postgresql/${PG_VERSION}/main/postgresql.conf
            sudo sed -i "s/^#*maintenance_work_mem = .*/maintenance_work_mem = 256MB/" /etc/postgresql/${PG_VERSION}/main/postgresql.conf
            sudo sed -i "s/^#*effective_cache_size = .*/effective_cache_size = 1GB/" /etc/postgresql/${PG_VERSION}/main/postgresql.conf
        else
            # Standard memory system (4GB+)
            log "Standard memory system detected (4GB+), using optimized settings"
            sudo sed -i "s/^#*shared_buffers = .*/shared_buffers = 2GB/" /etc/postgresql/${PG_VERSION}/main/postgresql.conf
            sudo sed -i "s/^#*work_mem = .*/work_mem = 256MB/" /etc/postgresql/${PG_VERSION}/main/postgresql.conf
            sudo sed -i "s/^#*maintenance_work_mem = .*/maintenance_work_mem = 1GB/" /etc/postgresql/${PG_VERSION}/main/postgresql.conf
            sudo sed -i "s/^#*effective_cache_size = .*/effective_cache_size = 4GB/" /etc/postgresql/${PG_VERSION}/main/postgresql.conf
        fi
    fi
    
    # Restart PostgreSQL
    sudo systemctl restart postgresql
    
    # Wait for PostgreSQL to be ready again
    log "Waiting for PostgreSQL to restart..."
    for i in {1..30}; do
        if sudo -u postgres psql -c "SELECT 1;" >/dev/null 2>&1; then
            log "PostgreSQL restarted successfully"
            break
        fi
        if [ $i -eq 30 ]; then
            error "PostgreSQL failed to restart after 30 seconds"
        fi
        sleep 1
    done
    
    log "PostgreSQL configured and restarted"
}

# Set database user password
setup_database_user() {
    log "Setting up database user"
    
    # Create database_user in PostgreSQL if needed
    sudo -u postgres psql -c "CREATE USER database_user WITH PASSWORD 'database_password' CREATEDB;" 2>/dev/null || \
        log "User database_user already exists"
    
    sudo -u postgres psql -c "ALTER USER database_user WITH SUPERUSER;" 2>/dev/null || true
    
    log "Database user setup completed"
}

main() {
    log "Starting dependency installation"
    
    if [ "$LOW_MEMORY" = true ]; then
        log "========================================="
        log "RUNNING IN LOW MEMORY MODE"
        log "Suitable for Raspberry Pi and systems <2GB RAM"
        log "========================================="
    fi
    
    # Clean up old repos FIRST before any apt-get update
    cleanup_old_repos
    
    install_dependencies
    install_postgresql
    configure_postgresql
    setup_database_user
    
    log "All dependencies installed successfully"
    
    if [ "$LOW_MEMORY" = true ]; then
        log ""
        log "PostgreSQL configured with low-memory settings:"
        log "  - shared_buffers: 128MB"
        log "  - work_mem: 4MB"
        log "  - maintenance_work_mem: 64MB"
        log "  - effective_cache_size: 256MB"
        log "  - max_connections: 20"
    fi
}

main "$@"