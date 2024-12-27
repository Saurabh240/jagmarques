#!/bin/bash

topics=("DBInfo" "Results" "Routes" "Trips" "Exit")

echo "Select a topic to view:"
select topic in "${topics[@]}"; do
    case $topic in
        "DBInfo"|"Results"|"Routes"|"Trips")
            echo "Fetching data for topic: $topic"
            docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic "$topic" --from-beginning
            break
            ;;
        "Exit")
            echo "Exiting."
            break
            ;;
        *)
            echo "Invalid choice. Please select a valid topic."
            ;;
    esac
done

