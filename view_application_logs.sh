#!/bin/bash

applications=(
  "trips-application"
  "streams-application"
  "routes-application"
  "Exit"
)

echo "Select the application to view logs:"
select app in "${applications[@]}"; do
    case $app in
        "trips-application")
            echo "Showing logs for Trips-Application..."
            docker logs -f trips-application
            break
            ;;
        "streams-application")
            echo "Showing logs for Streams-Application..."
            docker logs -f streams-application
            break
            ;;
        "routes-application")
            echo "Showing logs for Routes-Application..."
            docker logs -f routes-application
            break
            ;;
        "Exit")
            echo "Exiting."
            break
            ;;
        *)
            echo "Invalid choice. Please select a valid application."
            ;;
    esac
done

