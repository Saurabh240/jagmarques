#!/bin/bash

echo "Starting Docker Compose services with --force-recreate and -d options..."
docker compose up --force-recreate -d

if [ $? -eq 0 ]; then
    echo "Services started successfully."
else
    echo "Failed to start services. Exiting."
    exit 1
fi

