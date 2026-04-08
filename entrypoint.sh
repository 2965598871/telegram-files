#!/bin/sh

set -e  # Exit on error

# Default configuration
PUID=${PUID:-0}
PGID=${PGID:-0}

# Store PIDs in variables
BACKEND_PID=""
NGINX_PID=""

cleanup() {
    echo "Cleaning up processes..."

    # Check and kill each process individually
    if [ -n "$BACKEND_PID" ]; then
        kill -TERM "$BACKEND_PID" 2>/dev/null || true
    fi
    if [ -n "$NGINX_PID" ]; then
        kill -TERM "$NGINX_PID" 2>/dev/null || true
    fi

    wait || true
    echo "All processes have been terminated"
    exit 0
}

setup_permissions() {
    if [ "$(id -u)" = "0" ] && [ "$PUID" != "0" ]; then
        echo "Setting up directory permissions..."
        chown -R "${PUID}:${PGID}" /app /etc/nginx /var/lib/nginx /var/log/nginx /run/nginx.pid /etc/nginx/nginx.conf
    fi
}

start_services() {
    cmd_prefix=""
    if [ "$(id -u)" = "0" ] && [ "$PUID" != "0" ]; then
        cmd_prefix="su-exec ${PUID}:${PGID}"
    fi

    echo "Starting Python service..."
    if [ -n "$cmd_prefix" ]; then
        $cmd_prefix python -m uvicorn app.main:app --host 0.0.0.0 --port 8080 --app-dir /app/pyapi &
    else
        python -m uvicorn app.main:app --host 0.0.0.0 --port 8080 --app-dir /app/pyapi &
    fi
    BACKEND_PID=$!

    echo "Starting Nginx service..."
    if [ -n "$cmd_prefix" ]; then
        $cmd_prefix nginx -g 'daemon off;' &
    else
        nginx -g 'daemon off;' &
    fi
    NGINX_PID=$!
}

monitor_services() {
    while true; do
        if [ -n "$BACKEND_PID" ] && ! kill -0 "$BACKEND_PID" 2>/dev/null; then
            echo "Python service exited, stopping container..."
            kill -TERM "$NGINX_PID" 2>/dev/null || true
            wait "$BACKEND_PID"
            exit $?
        fi

        if [ -n "$NGINX_PID" ] && ! kill -0 "$NGINX_PID" 2>/dev/null; then
            echo "Nginx service exited, stopping container..."
            kill -TERM "$BACKEND_PID" 2>/dev/null || true
            wait "$NGINX_PID"
            exit $?
        fi

        sleep 1
    done
}

# Set up signal handlers
trap cleanup TERM INT

# Replace nginx.conf.template with environment variables
envsubst '$NGINX_PORT' < /etc/nginx/nginx.conf.template > /etc/nginx/nginx.conf

# Set up permissions
setup_permissions

# Start services
start_services

# Exit the container if either child process dies so restart policies can work.
monitor_services
