package com.majoozilla.streamsapplication.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TripUtils {

    private static final Logger logger = LoggerFactory.getLogger(TripUtils.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static String extractRouteId(String tripJson) {
        try {
            JsonNode jsonNode = objectMapper.readTree(tripJson);
            if (jsonNode.hasNonNull("route_id")) {
                return jsonNode.get("route_id").asText();
            }
            logger.error("Route ID is missing or null");
            return null;
        } catch (Exception e) {
            logger.error("Failed to parse route_id from JSON: {}", e.getMessage());
            return null;
        }
    }

    public static Double calculateOccupancyPercentage(String tripJson) {
        try {
            JsonNode jsonNode = objectMapper.readTree(tripJson);
            if (!jsonNode.hasNonNull("capacity") || !jsonNode.hasNonNull("currentPassengers")) {
                logger.error("Capacity or current passengers field is missing or null");
                return 0.0;
            }
            int capacity = jsonNode.get("capacity").asInt();
            int currentPassengers = jsonNode.get("currentPassengers").asInt();
            return capacity == 0 ? 0.0 : (double) currentPassengers / capacity * 100;
        } catch (Exception e) {
            logger.error("Failed to calculate occupancy percentage: {}", e.getMessage());
            return 0.0;
        }
    }

    public static String extractTransportType(String tripJson) {
        try {
            JsonNode jsonNode = objectMapper.readTree(tripJson);
            if (jsonNode.hasNonNull("transportType")) {
                return jsonNode.get("transportType").asText();
            }
            logger.error("Transport type is missing or null");
            return null;
        } catch (Exception e) {
            logger.error("Failed to parse transportType from JSON: {}", e.getMessage());
            return null;
        }
    }
}
