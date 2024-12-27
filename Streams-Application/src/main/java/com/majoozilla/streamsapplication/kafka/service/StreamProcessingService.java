package com.majoozilla.streamsapplication.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.majoozilla.streamsapplication.StreamsApplication;
import com.majoozilla.streamsapplication.database.Database;
import com.majoozilla.streamsapplication.kafka.KeyValueSerde;
import com.majoozilla.streamsapplication.kafka.KeyValueSerdeLong;
import com.majoozilla.streamsapplication.models.Route;
import com.majoozilla.streamsapplication.models.Trip;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.UUID;

import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.JsonSerde;

public class StreamProcessingService {


    private static final Logger logger = LoggerFactory.getLogger(StreamProcessingService.class);
    private static final String RESULTS_TOPIC = "Results";
    static Serde<Trip> tripSerde = new JsonSerde<>(Trip.class);
    static Serde<Route> routeSerde = new JsonSerde<>(Route.class);
    static Serde<KeyValue<String, Double>> keyValueSerde = KeyValueSerde.getKeyValueSerde();
    static Serde<KeyValue<String, Long>> keyValueSerdeLong = new KeyValueSerdeLong();


    public static void PassengerWithMostTrips(KStream<String, Trip> trips) {
        KStream<String, Trip> passengerTrips = trips.map((key, trip) -> {
            logger.info("Mapping trip to passenger: {} with Trip ID: {}", trip.getPassengerName(), trip.getTripId());
            return KeyValue.pair(trip.getPassengerName(), trip);
        });

        KTable<String, Long> tripCounts = passengerTrips
                .groupByKey(Grouped.with(Serdes.String(), tripSerde))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("trip-counts-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        tripCounts.toStream().foreach((passengerName, tripCount) -> {
            logger.info("Passenger: {} has made {} trips", passengerName, tripCount);
        });

        KStream<String, Long> tripCountStream = tripCounts.toStream();

        KStream<String, Long> passengerWithMostTrips = tripCountStream
                .transform(() -> new Transformer<String, Long, KeyValue<String, Long>>() {
                    private String maxPassenger = null;
                    private long maxTrips = 0;

                    @Override
                    public void init(ProcessorContext context) {}

                    @Override
                    public KeyValue<String, Long> transform(String passengerName, Long tripCount) {
                        if (tripCount > maxTrips) {
                            maxTrips = tripCount;
                            maxPassenger = passengerName;
                            logger.info("New max trips found: {} with {} trips", maxPassenger, maxTrips);
                        }
                        return KeyValue.pair(maxPassenger, maxTrips);
                    }

                    @Override
                    public void close() {}
                });

        passengerWithMostTrips.foreach((passengerName, tripCount) -> {
            logger.info("Passenger with most trips: {} with {} trips", passengerName, tripCount);

           
            try (Connection conn = Database.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(
                         "INSERT INTO metrics (metricID, metricType, value, timestamp, passengerName) " +
                                 "VALUES (?, ?, ?, ?, ?)")) {

                UUID metricID = UUID.randomUUID();
                String metricType = "PassengerWithMostTrips";
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());

                stmt.setObject(1, metricID);
                stmt.setString(2, metricType);
                stmt.setLong(3, tripCount);
                stmt.setTimestamp(4, timestamp);
                stmt.setString(5, passengerName);

                stmt.executeUpdate();
                logger.info("Successfully stored passenger with most trips: {}", passengerName);
            } catch (SQLException e) {
                logger.error("Failed to write passenger with most trips to database", e);
            }
        });

        passengerWithMostTrips.to(RESULTS_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

    }

    public static void OperatorWithMaxOccupancy(KStream<String, Trip> trips) {
        KStream<String, Double> occupancyPercentageByOperator = trips
                .map((key, trip) -> {
                    double occupancy = 100.0 * trip.getCurrentPassengers() / trip.getCapacity();
                    logger.info("Calculated occupancy for operator {} (Trip ID: {}): {}%", trip.getOperatorName(), trip.getTripId(), occupancy);
                    return KeyValue.pair(trip.getOperatorName(), occupancy);
                });

        KGroupedStream<String, Double> groupedByOperator = occupancyPercentageByOperator
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()));

        KTable<String, Double> maxOccupancyPerOperator = groupedByOperator
                .reduce(
                        Double::max,
                        Materialized.<String, Double, KeyValueStore<Bytes, byte[]>>as("max-occupancy-per-operator-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Double())
                );

        maxOccupancyPerOperator.toStream().foreach((operator, occupancy) -> {
            logger.info("Max occupancy for operator {}: {}%", operator, occupancy);
        });

        KStream<String, KeyValue<String, Double>> maxOccupancyStream = maxOccupancyPerOperator
                .toStream()
                .map((key, value) -> KeyValue.pair("global", KeyValue.pair(key, value)));
        KTable<String, KeyValue<String, Double>> operatorWithMaxOccupancy = maxOccupancyPerOperator
                .toStream()
                .groupBy(
                        (operator, occupancy) -> "global",
                        Grouped.with(Serdes.String(), Serdes.Double())
                )
                .aggregate(
                        () -> new KeyValue<>("", 0.0),
                        (aggKey, newValue, aggValue) -> {
                            if (newValue > aggValue.value) {
                                logger.info("New maximum occupancy found: {} vs. {}", newValue, aggValue.value);
                                return new KeyValue<>(aggKey, newValue);
                            } else {
                                return aggValue;
                            }
                        },
                        Materialized.<String, KeyValue<String, Double>, KeyValueStore<Bytes, byte[]>>as("global-max-occupancy-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(KeyValueSerde.getKeyValueSerde())
                );

        operatorWithMaxOccupancy.toStream().foreach((key, value) -> {
            logger.info("Operator with Max Occupancy - Operator: {}, Occupancy: {}", value.key, value.value);

            try (Connection conn = Database.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(
                         "INSERT INTO metrics (metricID, metricType, value, timestamp, operatorName) " +
                                 "VALUES (?, ?, ?, ?, ?)")) {

                UUID metricID = UUID.randomUUID();
                String metricType = "OperatorWithMaxOccupancy";
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());

                stmt.setObject(1, metricID);
                stmt.setString(2, metricType);
                stmt.setDouble(3, value.value);
                stmt.setTimestamp(4, timestamp);
                stmt.setString(5, value.key);

                stmt.executeUpdate();
                logger.info("Successfully stored operator with max occupancy: {}", value.key);
            } catch (SQLException e) {
                logger.error("Failed to write operator with max occupancy to database", e);
            }
        });

        operatorWithMaxOccupancy.toStream().to(RESULTS_TOPIC, Produced.with(Serdes.String(), KeyValueSerde.getKeyValueSerde()));
    }

    public static void RoutesWithLeastOccupancyPerTransportType(KStream<String, Trip> trips, KTable<String, Route> routesTable) {
        KStream<String, Double> occupancyStream = trips.mapValues(trip -> {
            double occupancy = 100.0 * trip.getCurrentPassengers() / trip.getCapacity();
            logger.info("Calculated occupancy for trip (Trip ID: {}): {}%", trip.getTripId(), occupancy);
            return occupancy;
        });

        KStream<String, KeyValue<String, Double>> enrichedTrips = occupancyStream.join(routesTable,
                (occupancy, route) -> {
                    logger.info("Joining occupancy {} with route info (Route ID: {})", occupancy, route.getId());
                    return new KeyValue<>(route.getTransportType(), occupancy);
                },
                Joined.with(Serdes.String(), Serdes.Double(), routeSerde));


        KTable<String, KeyValue<String, Double>> minOccupancyByTransport = enrichedTrips
                .map((key, value) -> {
                    logger.info("Mapping to (TransportType: {}, (RouteID: {}, Occupancy: {}))", value.key, key, value.value);
                    return new KeyValue<>(value.key, new KeyValue<>(key, value.value));
                })
                .groupBy(
                        (key, value) -> key,
                        Grouped.with(Serdes.String(), keyValueSerde)
                )
                .aggregate(
                        () -> new KeyValue<>("", Double.MAX_VALUE),
                        (aggKey, newValue, aggValue) -> {
                            logger.info("Comparing occupancy values: {} and {}", newValue.value, aggValue.value);
                            return newValue.value < aggValue.value ? newValue : aggValue;
                        },
                        Materialized.<String, KeyValue<String, Double>, KeyValueStore<Bytes, byte[]>>as("min-occupancy-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(keyValueSerde)
                );


        minOccupancyByTransport.toStream().foreach((transportType, routeAndOccupancy) -> {
            logger.info("Least Occupied Route - Transport Type: {}, Route ID: {}, Occupancy: {}",
                    transportType, routeAndOccupancy.key, routeAndOccupancy.value);

            try (Connection conn = Database.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(
                         "INSERT INTO metrics (metricID, metricType, value, timestamp, routeID, transportType) " +
                                 "VALUES (?, ?, ?, ?, ?, ?)")) {

                UUID metricID = UUID.randomUUID();
                String metricType = "RoutesWithLeastOccupancyPerTransportType";
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());

                stmt.setObject(1, metricID);
                stmt.setString(2, metricType);
                stmt.setDouble(3, routeAndOccupancy.value);
                stmt.setTimestamp(4, timestamp);
                stmt.setInt(5, Integer.parseInt(routeAndOccupancy.key));
                stmt.setString(6, transportType); 

                stmt.executeUpdate();
                logger.info("Successfully stored least occupied route for transport type: {}", transportType);
            } catch (SQLException e) {
                logger.error("Failed to write least occupied route to database", e);
            }
        });

        minOccupancyByTransport.toStream().to(RESULTS_TOPIC, Produced.with(Serdes.String(), keyValueSerde));

    }

    public static void TransportTypeWithMaxPassengers(KStream<String, Trip> trips) {
        KGroupedStream<String, Integer> passengersPerTransportTypeMax = trips
                .map((key, trip) -> {
                    logger.info("Processing trip for transport type: {} with current passengers: {}", trip.getTransportType(), trip.getCurrentPassengers());
                    return KeyValue.pair(trip.getTransportType(), trip.getCurrentPassengers()); 
                })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()));  

        KTable<String, Integer> totalPassengersPerTransportTypeMax = passengersPerTransportTypeMax
                .reduce(Integer::sum, Materialized.as("total-passengers-per-transport-type-store-max"));

        totalPassengersPerTransportTypeMax.toStream().foreach((key, value) -> {
            logger.info("Total passengers for transport type {}: {}", key, value);
        });

        KTable<String, Integer> maxTransportType = totalPassengersPerTransportTypeMax
                .toStream()
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
                .reduce(
                        (currentMax, newValue) -> {
                            logger.info("Comparing current max passengers: {} with new value: {}", currentMax, newValue);
                            if (newValue > currentMax) {
                                logger.info("New max found: {}", newValue);
                                return newValue;
                            } else {
                                logger.info("Current max remains: {}", currentMax);
                                return currentMax;
                            }
                        },
                        Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("max-transport-type-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Integer())
                );

        maxTransportType.toStream().foreach((key, value) -> {
            logger.info("Transport Type with Max Passengers: {}, Passengers: {}", key, value);

           
            try (Connection conn = Database.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(
                         "INSERT INTO metrics (metricID, metricType, value, timestamp, transportType) " +
                                 "VALUES (?, ?, ?, ?, ?)")) {

                UUID metricID = UUID.randomUUID();  
                String metricType = "TransportTypeWithMaxPassengers";
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());

                stmt.setObject(1, metricID);
                stmt.setString(2, metricType);
                stmt.setInt(3, value);
                stmt.setTimestamp(4, timestamp);
                stmt.setString(5, key);  

                stmt.executeUpdate();
                logger.info("Successfully stored max passengers transport type: {}", key);
            } catch (SQLException e) {
                logger.error("Failed to write max passengers transport type to database", e);
            }
        });


        maxTransportType.toStream().to(RESULTS_TOPIC, Produced.with(Serdes.String(), Serdes.Integer()));
    }

    public static void AveragePassengersPerTransportType(KStream<String, Trip> trips) {
        KGroupedStream<String, Integer> passengersPerTransportType = trips
                .map((key, trip) -> {
                    logger.info("Processing trip for transport type: {} with current passengers: {}", trip.getTransportType(), trip.getCurrentPassengers());
                    return KeyValue.pair(trip.getTransportType(), trip.getCurrentPassengers());  
                })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()));

        KTable<String, Integer> totalPassengersPerTransportType = passengersPerTransportType
                .reduce(Integer::sum, Materialized.as("total-passengers-per-transport-type-store"));

        totalPassengersPerTransportType.toStream().foreach((key, value) -> {
            logger.info("Total passengers for transport type {}: {}", key, value);
        });

        KTable<String, Long> tripCountsPerTransportType = trips
                .map((key, trip) -> {
                    logger.info("Counting trip for transport type: {}", trip.getTransportType());
                    return KeyValue.pair(trip.getTransportType(), 1L);
                })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .reduce(Long::sum, Materialized.as("trip-counts-per-transport-type-store"));


        tripCountsPerTransportType.toStream().foreach((key, value) -> {
            logger.info("Total trips for transport type {}: {}", key, value);
        });

        KTable<String, Double> averagePassengersPerTransportType = totalPassengersPerTransportType
                .join(tripCountsPerTransportType,
                        (totalPassengersForTransportType, tripCount) -> {

                            logger.info("Calculating average passengers for transport type {}: total passengers = {}, trip count = {}",
                                    totalPassengersForTransportType, tripCount);
                            if (tripCount != null && tripCount > 0) {
                                return (double) totalPassengersForTransportType / tripCount;
                            } else {
                                logger.warn("Division by zero for transport type: {}. Returning 0.0.", totalPassengersForTransportType);
                                return 0.0;
                            }
                        },
                        Materialized.<String, Double, KeyValueStore<Bytes, byte[]>>as("average-passengers-per-transport-type-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Double())
                );

        averagePassengersPerTransportType.toStream().foreach((key, value) -> {
            logger.info("Average Passengers Per Transport Type - Type: {}, Average: {}", key, value);

            try (Connection conn = Database.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(
                         "INSERT INTO metrics (metricID, metricType, value, timestamp, transportType) " +
                                 "VALUES (?, ?, ?, ?, ?)")) {

                UUID metricID = UUID.randomUUID();
                String metricType = "AveragePassengersPerTransportType";
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());

                stmt.setObject(1, metricID);
                stmt.setString(2, metricType);
                stmt.setDouble(3, value);
                stmt.setTimestamp(4, timestamp);
                stmt.setString(5, key);  

                stmt.executeUpdate();
                logger.info("Successfully stored average passengers for transport type: {}", key);
            } catch (SQLException e) {
                logger.error("Failed to write average passengers for transport type to database", e);
            }
        });

        averagePassengersPerTransportType.toStream().to(RESULTS_TOPIC, Produced.with(Serdes.String(), Serdes.Double()));
    }

    public static void TotalOccupancyPercentageForAllRoutes(KStream<String, Trip> trips, KTable<String, Route> routesTable) {
        KTable<String, Integer> totalPassengersOcc = trips
                .map((key, trip) -> {
                    logger.info("Processing trip for passenger: {} with current passengers: {}", trip.getPassengerName(), trip.getCurrentPassengers());
                    return KeyValue.pair("global", trip.getCurrentPassengers());
                })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
                .reduce(
                        Integer::sum,
                        Materialized.as("total-passengers-store-occ")
                );


        totalPassengersOcc.toStream().foreach((key, value) -> {
            logger.info("Total Passengers Occ - Key: {}, Value: {}", key, value);
        });

        KTable<String, Integer> totalCapacityOcc = routesTable
                .toStream()
                .map((key, route) -> {

                    logger.info("Processing route for ID: {} with capacity: {}", route.getId(), route.getCapacity());
                    return KeyValue.pair("global", route.getCapacity());
                })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
                .reduce(
                        Integer::sum,
                        Materialized.as("total-capacity-store-occ")
                );


        totalCapacityOcc.toStream().foreach((key, value) -> {
            logger.info("Total Capacity Occ - Key: {}, Value: {}", key, value);
        });

        KTable<String, Double> totalOccupancyPercentageOcc = totalPassengersOcc.join(
                totalCapacityOcc,
                (totalPass, totalCap) -> {
                    if (totalCap != null && totalCap > 0) {
                        double occupancyPercentage = 100.0 * totalPass / totalCap;
                        logger.info("Calculated occupancy percentage: {} for total passengers: {} and total capacity: {}", occupancyPercentage, totalPass, totalCap);
                        return occupancyPercentage;
                    } else {
                        logger.warn("Zero or null capacity encountered, returning 0 occupancy percentage");
                        return 0.0;
                    }
                },
                Materialized.<String, Double, KeyValueStore<Bytes, byte[]>>as("total-occupancy-percentage-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Double())
        );

        totalOccupancyPercentageOcc.toStream().foreach((key, value) -> {
            logger.info("Total Occupancy Percentage: {}", value);

            try (Connection conn = Database.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(
                         "INSERT INTO metrics (metricID, metricType, value, timestamp) " +
                                 "VALUES (?, ?, ?, ?)")) {

                UUID metricID = UUID.randomUUID();
                String metricType = "TotalOccupancyPercentageForAllRoutes";
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());

                stmt.setObject(1, metricID);
                stmt.setString(2, metricType);
                stmt.setDouble(3, value);
                stmt.setTimestamp(4, timestamp);

                stmt.executeUpdate();
                logger.info("Successfully stored total occupancy percentage: {}", value);
            } catch (SQLException e) {
                logger.error("Failed to write total occupancy percentage to database", e);
            }
        });

         totalOccupancyPercentageOcc.toStream().to(RESULTS_TOPIC, Produced.with(Serdes.String(), Serdes.Double()));
    }

    public static void TotalSeatingCapacityForAllRoutes(KTable<String, Route> routesTable) {
        KStream<String, Integer> capacities = routesTable.toStream()
                .map((key, route) -> KeyValue.pair("totalCapacity", route.getCapacity()));

        KTable<String, Integer> totalCapacity = capacities
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
                .reduce(Integer::sum, Materialized.as("total-capacity-store"));


        totalCapacity.toStream().foreach((key, value) -> {
            try (Connection conn = Database.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(
                         "INSERT INTO metrics (metricID, metricType, value, timestamp) " +
                                 "VALUES (?, ?, ?, ?)")) {

                UUID metricID = UUID.randomUUID();
                String metricType = "TotalSeatingCapacityForAllRoutes";
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());

                stmt.setObject(1, metricID);
                stmt.setString(2, metricType);
                stmt.setInt(3, value);
                stmt.setTimestamp(4, timestamp);

                stmt.executeUpdate();
                logger.info("Successfully updated total seating capacity: {}", value);
            } catch (SQLException e) {
                logger.error("Failed to write total seating capacity to database", e);
            }
        });

        totalCapacity.toStream().to(RESULTS_TOPIC, Produced.with(Serdes.String(), Serdes.Integer()));

    }

    public static void TotalPassengersForAllTrips(KStream<String, Trip> trips) {
        KStream<String, Integer> passengerCounts = trips.mapValues(trip -> {
            logger.info("Processing trip for passenger: {} with current passengers: {}", trip.getPassengerName(), trip.getCurrentPassengers());
            return trip.getCurrentPassengers();
        });

        KGroupedStream<String, Integer> groupedStream = passengerCounts.groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()));

        logger.info("Starting aggregation of total passengers.");

        KTable<String, Integer> totalPassengers = groupedStream
                .reduce(Integer::sum, Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("total-passengers-store"));

        totalPassengers.toStream().foreach((key, value) -> {
            logger.info("Updated total passengers: {}. Current total: {}", key, value);

            try (Connection conn = Database.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(
                         "INSERT INTO metrics (metricID, metricType, value, timestamp) " +
                                 "VALUES (?, ?, ?, ?)")) {

                UUID metricID = UUID.randomUUID();  
                String metricType = "TotalPassengersForAllTrips";
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());

                logger.info("Preparing to insert total passengers into the database: metricID = {}, metricType = {}, value = {}, timestamp = {}",
                        metricID, metricType, value, timestamp);

                stmt.setObject(1, metricID);
                stmt.setString(2, metricType);
                stmt.setInt(3, value);
                stmt.setTimestamp(4, timestamp);

                stmt.executeUpdate();

                logger.info("Successfully updated total passengers in database with value: {}", value);
            } catch (SQLException e) {
                logger.error("Failed to write total passengers to database. Error: {}", e.getMessage(), e);
            }
        });

        totalPassengers.toStream().to(RESULTS_TOPIC, Produced.with(Serdes.String(), Serdes.Integer()));

    }

    public static void OccupancyPercentagePerTrip(KStream<String, Trip> trips) {
        KStream<String, String> occupancyMetrics = trips
                .mapValues(trip -> {
                    logger.info("Processing trip: {} for route: {}, current passengers: {} of capacity: {}",
                            trip.getTripId(), trip.getRouteId(), trip.getCurrentPassengers(), trip.getCapacity());

                    if (trip.getCapacity() > 0) {
                        double occupancyPercentage = 100.0 * trip.getCurrentPassengers() / trip.getCapacity();
                        logger.info("Calculated occupancy for tripId {}: {}%", trip.getTripId(), occupancyPercentage);
                        return String.format(
                                "{" +
                                        "\"tripId\": \"%s\", " +
                                        "\"routeId\": \"%s\", " +
                                        "\"origin\": \"%s\", " +
                                        "\"destination\": \"%s\", " +
                                        "\"transportType\": \"%s\", " +
                                        "\"operatorName\": \"%s\", " +
                                        "\"capacity\": %d, " +
                                        "\"currentPassengers\": %d, " +
                                        "\"occupancyPercentage\": %.2f" +
                                        "}",
                                trip.getTripId(),
                                trip.getRouteId(),
                                trip.getOrigin(),
                                trip.getDestination(),
                                trip.getTransportType(),
                                trip.getOperatorName(),
                                trip.getCapacity(),
                                trip.getCurrentPassengers(),
                                occupancyPercentage
                        );
                    } else {
                        logger.warn("TripId {} has zero capacity, occupancy cannot be calculated.", trip.getTripId());
                        return String.format(
                                "{" +
                                        "\"tripId\": \"%s\", " +
                                        "\"routeId\": \"%s\", " +
                                        "\"origin\": \"%s\", " +
                                        "\"destination\": \"%s\", " +
                                        "\"transportType\": \"%s\", " +
                                        "\"operatorName\": \"%s\", " +
                                        "\"capacity\": %d, " +
                                        "\"currentPassengers\": %d, " +
                                        "\"occupancyPercentage\": \"N/A\"" +
                                        "}",
                                trip.getTripId(),
                                trip.getRouteId(),
                                trip.getOrigin(),
                                trip.getDestination(),
                                trip.getTransportType(),
                                trip.getOperatorName(),
                                trip.getCapacity(),
                                trip.getCurrentPassengers()
                        );
                    }
                });

        occupancyMetrics.foreach((key, value) -> logger.info("Enriched Metric: {}", value));

        occupancyMetrics.foreach((key, value) -> {
            JsonNode json = null;
            try (Connection conn = Database.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(
                         "INSERT INTO metrics (metricID, metricType, value, timestamp, routeID, transportType) " +
                                 "VALUES (?, ?, ?, ?, ?, ?)")) {

                json = new ObjectMapper().readTree(value);
                UUID metricID = UUID.randomUUID();  
                String metricType = "OccupancyPercentagePerTrip";
                double valueField = json.has("occupancyPercentage") ? json.get("occupancyPercentage").asDouble() : 0.0;
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());

                logger.info("Preparing to insert occupancy percentage for tripId {}: {}", json.get("tripId").asText(), valueField);

                stmt.setObject(1, metricID);
                stmt.setString(2, metricType);
                stmt.setDouble(3, valueField);
                stmt.setTimestamp(4, timestamp);
                int routeId = json.has("routeId") ? json.get("routeId").asInt() : 0;
                stmt.setInt(5, routeId);
                stmt.setString(6, json.get("transportType").asText());

                stmt.executeUpdate();
                logger.info("Successfully updated occupancy percentage for tripId {}: {}", json.get("tripId").asText(), valueField);
            } catch (SQLException | JsonProcessingException e) {
                logger.error("Failed to write occupancy percentage to database for tripId {}", json != null ? json.get("tripId").asText() : "Unknown", e);
            }
        });

        occupancyMetrics.to(RESULTS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }

    public static void AvailableSeatsPerRoute(KTable<String, Route> routesTable) {
        KStream<String, Route> routesStream = routesTable.toStream();
        routesStream.foreach((routeId, route) -> {
            if (route != null) {
                logger.info("Processing route with routeId {}: origin = {}, destination = {}, capacity = {}, transportType = {}",
                        routeId, route.getOrigin(), route.getDestination(), route.getCapacity(), route.getTransportType());

                try (Connection conn = Database.getConnection();
                     PreparedStatement stmt = conn.prepareStatement(
                             "INSERT INTO metrics (metricID, metricType, value, timestamp, routeID, transportType) " +
                                     "VALUES (?, ?, ?, ?, ?, ?)")) {

                    UUID metricID = UUID.randomUUID();  
                    String metricType = "AvailableSeatsPerRoute";
                    double valueField = route.getCapacity();
                    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

                    stmt.setObject(1, metricID);
                    stmt.setString(2, metricType);
                    stmt.setDouble(3, valueField);
                    stmt.setTimestamp(4, timestamp);
                    stmt.setLong(5, route.getId());
                    stmt.setString(6, route.getTransportType());

                    stmt.executeUpdate();

                    logger.info("Successfully updated available seats for routeID {} with capacity: {}", routeId, route.getCapacity());
                } catch (SQLException e) {
                    logger.error("Database error when updating available seats for routeID {}: {}", routeId, e.getMessage(), e);
                }
            } else {
                logger.warn("Received null route for routeId {}", routeId);
            }
        });

        routesStream.to(RESULTS_TOPIC, Produced.with(Serdes.String(), routeSerde));

    }

    public static void TotalPassengersPerRoute(KStream<String, Trip> trips, KTable<String, Route> routesTable) {
        KStream<String, Integer> passengersPerRoute = trips
                .map((key, trip) -> {
                    logger.info("Mapping trip: {} to route: {}", trip.getTripId(), trip.getRouteId());
                    return KeyValue.pair(trip.getRouteId().toString(), trip.getCurrentPassengers());
                })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
                .reduce(Integer::sum, Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("passengers-per-route-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Integer()))
                .toStream();

        passengersPerRoute.foreach((key, value) -> logger.info("Aggregated total passengers for route {}: {}", key, value));

        KStream<String, String> enrichedMetrics = passengersPerRoute
                .leftJoin(routesTable, (totalPassengers, route) -> {
                    logger.info("Joining route details for total passengers: {} on route: {}", totalPassengers, route != null ? route.getId() : "Unknown"); // Log the join details
                    if (route != null) {
                        return String.format(
                                "{" +
                                        "\"routeID\": %d, " +
                                        "\"origin\": \"%s\", " +
                                        "\"destination\": \"%s\", " +
                                        "\"capacity\": %d, " +
                                        "\"transportType\": \"%s\", " +
                                        "\"operatorName\": \"%s\", " +
                                        "\"totalPassengers\": %d" +
                                        "}",
                                route.getId(),
                                route.getOrigin(),
                                route.getDestination(),
                                route.getCapacity(),
                                route.getTransportType(),
                                route.getOperatorName(),
                                totalPassengers
                        );
                    } else {
                        return "{\"totalPassengers\": " + totalPassengers + "}";
                    }
                }, Joined.with(Serdes.String(), Serdes.Integer(), routeSerde));

        enrichedMetrics.foreach((key, value) -> logger.info("Enriched Metric: {}", value));

        enrichedMetrics.foreach((key, value) -> {
            try (Connection conn = Database.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(
                         "INSERT INTO metrics (metricID, metricType, value, timestamp, routeID, transportType) " +
                                 "VALUES (?, ?, ?, ?, ?, ?)")) {

                JsonNode json = new ObjectMapper().readTree(value);
                UUID metricID = UUID.randomUUID();  
                String metricType = "TotalPassengersPerRoute";
                double valueField = json.get("totalPassengers").asDouble();
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());

                stmt.setObject(1, metricID);
                stmt.setString(2, metricType);
                stmt.setDouble(3, valueField);
                stmt.setTimestamp(4, timestamp);
                if (json.hasNonNull("routeID")) {
                    stmt.setInt(5, json.get("routeID").asInt());
                } else {
                    stmt.setNull(5, java.sql.Types.INTEGER);
                }
                stmt.setString(6, json.hasNonNull("transportType") ? json.get("transportType").asText() : "Unknown");

                stmt.executeUpdate();
                logger.info("Successfully inserted/updated metric for routeID {}", key);
            } catch (SQLException | JsonProcessingException e) {
                logger.error("Failed to write metric to database for key {}", key, e);
            }
        });

        enrichedMetrics.to(RESULTS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

    }

    public static void MostUsedTransportTypeInLastHour(KStream<String, Trip> trips) {
        KTable<Windowed<String>, Long> windowedCounts = trips
                .map((key, trip) -> new KeyValue<>(trip.getTransportType(), 1L))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .windowedBy(TimeWindows.of(Duration.ofHours(1)))
                .reduce(Long::sum,
                        Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("transport-type-counts-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long()));


        windowedCounts.toStream()
                .map((key, value) -> KeyValue.pair("global", new KeyValue<>(key.key(), value)))
                .groupBy(
                        (key, value) -> key,
                        Grouped.with(Serdes.String(), keyValueSerdeLong))
                .reduce((aggValue, newValue) -> newValue.value > aggValue.value ? newValue : aggValue,
                        Materialized.<String, KeyValue<String, Long>, KeyValueStore<Bytes, byte[]>>as("most-used-transport-type-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(keyValueSerdeLong))
                .toStream()
                .foreach((key, value) -> {
                    String transportType = value.key;
                    long count = value.value;

                    logger.info("Most used transport type: {}, Count: {}", transportType, count);


                    try (Connection conn = Database.getConnection();
                         PreparedStatement stmt = conn.prepareStatement(
                                 "INSERT INTO metrics (metricID, metricType, value, timestamp, transportType) " +
                                         "VALUES (?, ?, ?, ?, ?)")) {

                        UUID metricID = UUID.randomUUID();
                        String metricType = "MostUsedTransportTypeInLastHour";
                        Timestamp timestamp = new Timestamp(System.currentTimeMillis());

                        stmt.setObject(1, metricID);
                        stmt.setString(2, metricType);
                        stmt.setLong(3, count);
                        stmt.setTimestamp(4, timestamp);
                        stmt.setString(5, transportType);

                        stmt.executeUpdate();
                        logger.info("Successfully stored most used transport type: {} with count: {}", transportType, count);
                    } catch (SQLException e) {
                        logger.error("Failed to write most used transport type to database", e);
                    }
                });


        windowedCounts.toStream()
                .map((key, value) -> KeyValue.pair(key.key(), value.toString()))
                .to(RESULTS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }

    public static void LeastOccupiedTransportTypeInLastHour(KStream<String, Trip> trips) {

        KTable<Windowed<String>, Double> windowedOccupancy = trips
                .map((key, trip) -> {

                    double occupancyPercentage = (trip.getCapacity() > 0)
                            ? (100.0 * trip.getCurrentPassengers() / trip.getCapacity())
                            : 0.0;
                    return KeyValue.pair(trip.getTransportType(), occupancyPercentage);
                })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .windowedBy(TimeWindows.of(Duration.ofHours(1)))
                .reduce((aggValue, newValue) -> aggValue + newValue,
                        Materialized.<String, Double, WindowStore<Bytes, byte[]>>as("transport-type-occupancy-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Double()));


        windowedOccupancy.toStream()
                .map((key, value) -> KeyValue.pair("global", new KeyValue<>(key.key(), value)))
                .groupBy(
                        (key, value) -> key,
                        Grouped.with(Serdes.String(), keyValueSerde))
                .reduce((aggValue, newValue) -> newValue.value < aggValue.value ? newValue : aggValue,
                        Materialized.<String, KeyValue<String, Double>, KeyValueStore<Bytes, byte[]>>as("least-occupied-transport-type-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(keyValueSerde))
                .toStream()
                .foreach((key, value) -> {
                    String transportType = value.key;
                    double occupancyPercentage = value.value;

                    logger.info("Least occupied transport type: {}, Occupancy: {}", transportType, occupancyPercentage);

                    try (Connection conn = Database.getConnection();
                         PreparedStatement stmt = conn.prepareStatement(
                                 "INSERT INTO metrics (metricID, metricType, value, timestamp, transportType) " +
                                         "VALUES (?, ?, ?, ?, ?)")) {

                        UUID metricID = UUID.randomUUID(); // Generate a unique metric ID
                        String metricType = "LeastOccupiedTransportTypeInLastHour";
                        Timestamp timestamp = new Timestamp(System.currentTimeMillis());

                        stmt.setObject(1, metricID);
                        stmt.setString(2, metricType);
                        stmt.setDouble(3, occupancyPercentage);
                        stmt.setTimestamp(4, timestamp);
                        stmt.setString(5, transportType);

                        stmt.executeUpdate();
                        logger.info("Successfully stored least occupied transport type: {} with occupancy: {}", transportType, occupancyPercentage);
                    } catch (SQLException e) {
                        logger.error("Failed to write least occupied transport type to database", e);
                    }
                });

        windowedOccupancy.toStream()
                .map((key, value) -> KeyValue.pair(key.key(), value.toString()))
                .to(RESULTS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }


}
