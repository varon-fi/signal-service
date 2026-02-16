#!/bin/bash
# Signal Service Deployment Script
# Run with: sudo bash deploy-signal-service.sh

set -e

echo "=== Signal Service Deployment ==="

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo "Please run as root or with sudo"
    exit 1
fi

echo "1. Reloading systemd..."
systemctl daemon-reload

echo "2. Enabling signal-service..."
systemctl enable signal-service

echo "3. Starting signal-service..."
systemctl start signal-service

echo "4. Checking status..."
sleep 2
systemctl status signal-service --no-pager

echo ""
echo "=== Deployment Complete ==="
echo "View logs: sudo journalctl -u signal-service -f"
