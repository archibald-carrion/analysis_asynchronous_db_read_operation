#!/bin/bash

# PostgreSQL 18 and Dependency Installer for Debian

set -e

LOG_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/installation.log"
PG_VERSION=18

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $1" | tee -a "$LOG_FILE"
    exit 1
}

# Install PostgreSQL 18
install_postgresql() {
    log "Installing PostgreSQL ${PG_VERSION}"
    
    # Add PostgreSQL official repository
    sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
    
    # Import repository signing key
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
    
    # Update package list
    sudo apt-get update
    
    # Install PostgreSQL
    sudo apt-get install -y postgresql-${PG_VERSION} postgresql-client-${PG_VERSION} \
                          postgresql-contrib-${PG_VERSION} postgresql-server-dev-${PG_VERSION}
    
    log "PostgreSQL ${PG_VERSION} installed successfully"
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
        pkg-config
    
    log "System dependencies installed"
}

# Configure PostgreSQL
configure_postgresql() {
    log "Configuring PostgreSQL"
    
    # Start PostgreSQL service
    sudo systemctl enable postgresql
    sudo systemctl start postgresql
    
    # Configure PostgreSQL to allow connections
    sudo sed -i "s/#listen_addresses = 'localhost'/listen_addresses = '*'/" /etc/postgresql/${PG_VERSION}/main/postgresql.conf
    
    # Add password authentication
    echo "host all all 0.0.0.0/0 md5" | sudo tee -a /etc/postgresql/${PG_VERSION}/main/pg_hba.conf
    
    # Increase shared buffers for large database
    sudo sed -i "s/^#*shared_buffers = .*/shared_buffers = 2GB/" /etc/postgresql/${PG_VERSION}/main/postgresql.conf
    sudo sed -i "s/^#*work_mem = .*/work_mem = 256MB/" /etc/postgresql/${PG_VERSION}/main/postgresql.conf
    sudo sed -i "s/^#*maintenance_work_mem = .*/maintenance_work_mem = 1GB/" /etc/postgresql/${PG_VERSION}/main/postgresql.conf
    
    # Restart PostgreSQL
    sudo systemctl restart postgresql
    
    log "PostgreSQL configured and restarted"
}

# Set database user password
setup_database_user() {
    log "Setting up database user"
    
    # Create database_user in PostgreSQL if needed
    sudo -u postgres psql -c "CREATE USER database_user WITH PASSWORD 'database_password' CREATEDB;" || true
    sudo -u postgres psql -c "ALTER USER database_user WITH SUPERUSER;" || true
    
    log "Database user setup completed"
}

main() {
    log "Starting dependency installation"
    
    install_dependencies
    install_postgresql
    configure_postgresql
    setup_database_user
    
    log "All dependencies installed successfully"
}

main "$@"