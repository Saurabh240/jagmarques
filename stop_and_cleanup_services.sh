#!/bin/bash

echo "Stopping and removing Docker Compose services..."
docker compose down --volumes --remove-orphans

if [ $? -eq 0 ]; then
    echo "All services, volumes, and orphan containers have been removed successfully."
else
    echo "Failed to remove services and volumes. Please check for errors."
    exit 1
fi

read -p "Do you want to remove dangling Docker images as well? (y/n): " choice
if [[ "$choice" == "y" || "$choice" == "Y" ]]; then
    echo "Removing dangling Docker images..."
    docker image prune -f
    echo "Dangling images removed."
else
    echo "Skipping removal of dangling images."
fi

echo "Cleanup completed."

