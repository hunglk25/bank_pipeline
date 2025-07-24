#!/bin/bash

# Banking Data Pipeline Setup Script
# This script sets up and initializes the complete data pipeline

set -e  # Exit on any error

echo "Banking Data Pipeline Setup Starting..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Docker is running
check_docker() {
    print_status "Checking Docker..."
    if ! docker info > /dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker first."
        exit 1
    fi
    print_success "Docker is running"
}

# Check if docker-compose is available
check_docker_compose() {
    print_status "Checking Docker Compose..."
    if ! command -v docker-compose &> /dev/null; then
        print_error "docker-compose could not be found. Please install Docker Compose."
        exit 1
    fi
    print_success "Docker Compose is available"
}

# Stop existing containers
stop_containers() {
    print_status "Stopping existing containers..."
    docker-compose down --remove-orphans || true
    print_success "Stopped existing containers"
}

# Build and start containers
start_containers() {
    print_status "Building Docker images..."
    docker-compose build
    print_status "Starting containers..."
    docker-compose up -d
    
    # Wait for services to be ready
    print_status "Waiting for services to be ready..."
    sleep 30
    
    # Check if containers are running
    if docker-compose ps | grep -q "Up"; then
        print_success "Containers are running"
    else
        print_error "Some containers failed to start"
        docker-compose logs
        exit 1
    fi
}

# Wait for databases to be ready
wait_for_databases() {
    print_status "Waiting for databases to be ready..."
    
    # Wait for PostgreSQL (Airflow metadata)
    print_status "Checking Airflow database..."
    for i in {1..30}; do
        if docker-compose exec -T postgres pg_isready -U airflow -d airflow > /dev/null 2>&1; then
            break
        fi
        if [ $i -eq 30 ]; then
            print_error "Airflow database is not ready after 30 attempts"
            exit 1
        fi
        sleep 2
    done
    
    # Wait for PostgreSQL (Data database)
    print_status "Checking data database..."
    for i in {1..30}; do
        if docker-compose exec -T postgres_data pg_isready -U user -d mydata > /dev/null 2>&1; then
            break
        fi
        if [ $i -eq 30 ]; then
            print_error "Data database is not ready after 30 attempts"
            exit 1
        fi
        sleep 2
    done
    
    print_success "Databases are ready"
}



# Show final status and instructions
show_final_status() {
    echo ""
    echo "=============================================="
    print_success "Banking Data Pipeline Setup Complete!"
    echo "=============================================="
    echo ""
    echo "🌐 Access points:"
    echo "   • Airflow UI: http://localhost:8081"
    echo "     Username: admin"
    echo "     Password: admin"
    echo ""
    echo "   • pgAdmin: http://localhost:8082"
    echo "     Email: admin@admin.com" 
    echo "     Password: admin"
    echo ""
    echo "   • Streamlit App: http://localhost:8501"
    print_success "Happy data pipelining! 🚀"
}

# Error handling
trap 'print_error "Setup failed. Check the logs above for details."' ERR

# Main execution
main() {
    print_status "Starting Banking Data Pipeline Setup..."
    
    check_docker
    check_docker_compose
    stop_containers
    start_containers
    wait_for_databases
    show_final_status
}

# Run main function
main
