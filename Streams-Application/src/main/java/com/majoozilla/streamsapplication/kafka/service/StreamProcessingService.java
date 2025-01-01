package com.majoozilla.streamsapplication.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.majoozilla.streamsapplication.database.Database;
import com.majoozilla.streamsapplication.kafka.KeyValueSerde;
import com.majoozilla.streamsapplication.kafka.KeyValueSerdeLong;
import com.majoozilla.streamsapplication.models.Route;
import com.majoozilla.streamsapplication.models.Trip;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;

import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

public class StreamProcessingService {


    private static final Logger logger = LoggerFactory.getLogger(StreamProcessingService.class);
    private static final String RESULTS_TOPIC = "Results";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final KafkaProducer<String, String> producer = new KafkaProducer<>(getProducerProperties());
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

            // Generating dynamic values
            UUID metricId = UUID.randomUUID();
            String metricType = "PassengerWithMostTrips";
            String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()); // Dynamic timestamp

            // Constructing the payload
            Map<String, Object> payload = new HashMap<>();
            payload.put("metricid", metricId.toString());
            payload.put("metrictype", metricType);
            payload.put("value", (float) tripCount);
            payload.put("timestamp", timestamp);
            payload.put("routeid", 0);
            payload.put("transporttype", "");
            payload.put("operatorname", "");
            payload.put("passengername", passengerName);

            // Constructing the schema
            List<Map<String, Object>> fields = Arrays.asList(
                    Map.of("field", "metricid", "type", "string"),
                    Map.of("field", "metrictype", "type", "string"),
                    Map.of("field", "value", "type", "float"),
                    Map.of("field", "timestamp", "type", "string"),
                    Map.of("field", "routeid", "type", "int32"),
                    Map.of("field", "transporttype", "type", "string"),
                    Map.of("field", "operatorname", "type", "string"),
                    Map.of("field", "passengername", "type", "string")
            );

            Map<String, Object> schema = new HashMap<>();
            schema.put("type", "struct");
            schema.put("fields", fields);
            schema.put("optional", false);
            schema.put("name", "Metrics");

            // Combining schema and payload into the final message
            Map<String, Object> message = new HashMap<>();
            message.put("schema", schema);
            message.put("payload", payload);

            try {
                    JsonNode jsonNode = objectMapper.valueToTree(message); // Converts safely to a JSON tree structure
                    String jsonMessage = objectMapper.writeValueAsString(jsonNode);

                // Publish the message to Kafka
                ProducerRecord<String, String> record = new ProducerRecord<>(RESULTS_TOPIC, null, jsonMessage);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        logger.error("Failed to publish message to topic", exception);
                    } else {
                        logger.info("Message published to topic {} with offset {}", metadata.topic(), metadata.offset());
                    }
                });

            } catch (JsonProcessingException e) {
                logger.error("Failed to serialize message to JSON", e);
            } catch (Exception e) {
                logger.error("Error while creating or publishing message to Kafka topic", e);
            }
        });

         // Producing the stream to the Kafka topic
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

            // Generating dynamic values
            UUID metricId = UUID.randomUUID();
            String metricType = "OperatorWithMaxOccupancy";
            String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()); // Dynamic timestamp

            // Constructing the payload
            Map<String, Object> payload = new HashMap<>();
            payload.put("metricid", metricId.toString());
            payload.put("metrictype", metricType);
            payload.put("value", value.value);
            payload.put("timestamp", timestamp);
            payload.put("routeid", 0);
            payload.put("transporttype", "");
            payload.put("operatorname", "");
            payload.put("passengername", value.key);

            // Constructing the schema
            List<Map<String, Object>> fields = Arrays.asList(
                    Map.of("field", "metricid", "type", "string"),
                    Map.of("field", "metrictype", "type", "string"),
                    Map.of("field", "value", "type", "double"),
                    Map.of("field", "timestamp", "type", "string"),
                    Map.of("field", "routeid", "type", "int32"),
                    Map.of("field", "transporttype", "type", "string"),
                    Map.of("field", "operatorname", "type", "string"),
                    Map.of("field", "passengername", "type", "string")
            );

            Map<String, Object> schema = new HashMap<>();
            schema.put("type", "struct");
            schema.put("fields", fields);
            schema.put("optional", false);
            schema.put("name", "Metrics");

            // Combining schema and payload into the final message
            Map<String, Object> message = new HashMap<>();
            message.put("schema", schema);
            message.put("payload", payload);

            try {
                JsonNode jsonNode = objectMapper.valueToTree(message); // Converts safely to a JSON tree structure
                String jsonMessage = objectMapper.writeValueAsString(jsonNode);

                // Publish the message to Kafka
                ProducerRecord<String, String> record = new ProducerRecord<>(RESULTS_TOPIC, null, jsonMessage);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        logger.error("Failed to publish message to topic", exception);
                    } else {
                        logger.info("Message published to topic {} with offset {}", metadata.topic(), metadata.offset());
                    }
                });

            } catch (JsonProcessingException e) {
                logger.error("Failed to serialize message to JSON", e);
            } catch (Exception e) {
                logger.error("Error while creating or publishing message to Kafka topic", e);
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

            // Generating dynamic values
            UUID metricId = UUID.randomUUID();
            String metricType = "RoutesWithLeastOccupancyPerTransportType";
            String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()); // Dynamic timestamp

            // Constructing the payload
            Map<String, Object> payload = new HashMap<>();
            payload.put("metricid", metricId.toString());
            payload.put("metrictype", metricType);
            payload.put("value", routeAndOccupancy.value);
            payload.put("timestamp", timestamp);
            payload.put("routeid", Integer.parseInt(routeAndOccupancy.key));
            payload.put("transporttype", transportType);
            payload.put("operatorname", "");
            payload.put("passengername", "");

            // Constructing the schema
            List<Map<String, Object>> fields = Arrays.asList(
                    Map.of("field", "metricid", "type", "string"),
                    Map.of("field", "metrictype", "type", "string"),
                    Map.of("field", "value", "type", "double"),
                    Map.of("field", "timestamp", "type", "string"),
                    Map.of("field", "routeid", "type", "int32"),
                    Map.of("field", "transporttype", "type", "string"),
                    Map.of("field", "operatorname", "type", "string"),
                    Map.of("field", "passengername", "type", "string")
            );

            Map<String, Object> schema = new HashMap<>();
            schema.put("type", "struct");
            schema.put("fields", fields);
            schema.put("optional", false);
            schema.put("name", "Metrics");

            // Combining schema and payload into the final message
            Map<String, Object> message = new HashMap<>();
            message.put("schema", schema);
            message.put("payload", payload);

            try {
                JsonNode jsonNode = objectMapper.valueToTree(message); // Converts safely to a JSON tree structure
                String jsonMessage = objectMapper.writeValueAsString(jsonNode);

                // Publish the message to Kafka
                ProducerRecord<String, String> record = new ProducerRecord<>(RESULTS_TOPIC, null, jsonMessage);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        logger.error("Failed to publish message to topic", exception);
                    } else {
                        logger.info("Message published to topic {} with offset {}", metadata.topic(), metadata.offset());
                    }
                });

            } catch (JsonProcessingException e) {
                logger.error("Failed to serialize message to JSON", e);
            } catch (Exception e) {
                logger.error("Error while creating or publishing message to Kafka topic", e);
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

            // Generating dynamic values
            UUID metricId = UUID.randomUUID();
            String metricType = "TransportTypeWithMaxPassengers";
            String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()); // Dynamic timestamp

            // Constructing the payload
            Map<String, Object> payload = new HashMap<>();
            payload.put("metricid", metricId.toString());
            payload.put("metrictype", metricType);
            payload.put("value", value);
            payload.put("timestamp", timestamp);
            payload.put("routeid", 0);
            payload.put("transporttype", key);
            payload.put("operatorname", "");
            payload.put("passengername", "");

            // Constructing the schema
            List<Map<String, Object>> fields = Arrays.asList(
                    Map.of("field", "metricid", "type", "string"),
                    Map.of("field", "metrictype", "type", "string"),
                    Map.of("field", "value", "type", "double"),
                    Map.of("field", "timestamp", "type", "string"),
                    Map.of("field", "routeid", "type", "int32"),
                    Map.of("field", "transporttype", "type", "string"),
                    Map.of("field", "operatorname", "type", "string"),
                    Map.of("field", "passengername", "type", "string")
            );

            Map<String, Object> schema = new HashMap<>();
            schema.put("type", "struct");
            schema.put("fields", fields);
            schema.put("optional", false);
            schema.put("name", "Metrics");

            // Combining schema and payload into the final message
            Map<String, Object> message = new HashMap<>();
            message.put("schema", schema);
            message.put("payload", payload);

            try {
                JsonNode jsonNode = objectMapper.valueToTree(message); // Converts safely to a JSON tree structure
                String jsonMessage = objectMapper.writeValueAsString(jsonNode);

                // Publish the message to Kafka
                ProducerRecord<String, String> record = new ProducerRecord<>(RESULTS_TOPIC, null, jsonMessage);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        logger.error("Failed to publish message to topic", exception);
                    } else {
                        logger.info("Message published to topic {} with offset {}", metadata.topic(), metadata.offset());
                    }
                });

            } catch (JsonProcessingException e) {
                logger.error("Failed to serialize message to JSON", e);
            } catch (Exception e) {
                logger.error("Error while creating or publishing message to Kafka topic", e);
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

            // Generating dynamic values
            UUID metricId = UUID.randomUUID();
            String metricType = "AveragePassengersPerTransportType";
            String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()); // Dynamic timestamp

            // Constructing the payload
            Map<String, Object> payload = new HashMap<>();
            payload.put("metricid", metricId.toString());
            payload.put("metrictype", metricType);
            payload.put("value", value);
            payload.put("timestamp", timestamp);
            payload.put("routeid", 0);
            payload.put("transporttype", key);
            payload.put("operatorname", "");
            payload.put("passengername", "");

            // Constructing the schema
            List<Map<String, Object>> fields = Arrays.asList(
                    Map.of("field", "metricid", "type", "string"),
                    Map.of("field", "metrictype", "type", "string"),
                    Map.of("field", "value", "type", "double"),
                    Map.of("field", "timestamp", "type", "string"),
                    Map.of("field", "routeid", "type", "int32"),
                    Map.of("field", "transporttype", "type", "string"),
                    Map.of("field", "operatorname", "type", "string"),
                    Map.of("field", "passengername", "type", "string")
            );

            Map<String, Object> schema = new HashMap<>();
            schema.put("type", "struct");
            schema.put("fields", fields);
            schema.put("optional", false);
            schema.put("name", "Metrics");

            // Combining schema and payload into the final message
            Map<String, Object> message = new HashMap<>();
            message.put("schema", schema);
            message.put("payload", payload);

            try {
                JsonNode jsonNode = objectMapper.valueToTree(message); // Converts safely to a JSON tree structure
                String jsonMessage = objectMapper.writeValueAsString(jsonNode);

                // Publish the message to Kafka
                ProducerRecord<String, String> record = new ProducerRecord<>(RESULTS_TOPIC, null, jsonMessage);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        logger.error("Failed to publish message to topic", exception);
                    } else {
                        logger.info("Message published to topic {} with offset {}", metadata.topic(), metadata.offset());
                    }
                });

            } catch (JsonProcessingException e) {
                logger.error("Failed to serialize message to JSON", e);
            } catch (Exception e) {
                logger.error("Error while creating or publishing message to Kafka topic", e);
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

            // Generating dynamic values
            UUID metricId = UUID.randomUUID();
            String metricType = "TotalOccupancyPercentageForAllRoutes";
            String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()); // Dynamic timestamp

            // Constructing the payload
            Map<String, Object> payload = new HashMap<>();
            payload.put("metricid", metricId.toString());
            payload.put("metrictype", metricType);
            payload.put("value", value);
            payload.put("timestamp", timestamp);
            payload.put("routeid", 0);
            payload.put("transporttype", "");
            payload.put("operatorname", "");
            payload.put("passengername", "");

            // Constructing the schema
            List<Map<String, Object>> fields = Arrays.asList(
                    Map.of("field", "metricid", "type", "string"),
                    Map.of("field", "metrictype", "type", "string"),
                    Map.of("field", "value", "type", "double"),
                    Map.of("field", "timestamp", "type", "string"),
                    Map.of("field", "routeid", "type", "int32"),
                    Map.of("field", "transporttype", "type", "string"),
                    Map.of("field", "operatorname", "type", "string"),
                    Map.of("field", "passengername", "type", "string")
            );

            Map<String, Object> schema = new HashMap<>();
            schema.put("type", "struct");
            schema.put("fields", fields);
            schema.put("optional", false);
            schema.put("name", "Metrics");

            // Combining schema and payload into the final message
            Map<String, Object> message = new HashMap<>();
            message.put("schema", schema);
            message.put("payload", payload);

            try {
                JsonNode jsonNode = objectMapper.valueToTree(message); // Converts safely to a JSON tree structure
                String jsonMessage = objectMapper.writeValueAsString(jsonNode);

                // Publish the message to Kafka
                ProducerRecord<String, String> record = new ProducerRecord<>(RESULTS_TOPIC, null, jsonMessage);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        logger.error("Failed to publish message to topic", exception);
                    } else {
                        logger.info("Message published to topic {} with offset {}", metadata.topic(), metadata.offset());
                    }
                });

            } catch (JsonProcessingException e) {
                logger.error("Failed to serialize message to JSON", e);
            } catch (Exception e) {
                logger.error("Error while creating or publishing message to Kafka topic", e);
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

            // Generating dynamic values
            UUID metricId = UUID.randomUUID();
            String metricType = "TotalSeatingCapacityForAllRoutes";
            String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()); // Dynamic timestamp

            // Constructing the payload
            Map<String, Object> payload = new HashMap<>();
            payload.put("metricid", metricId.toString());
            payload.put("metrictype", metricType);
            payload.put("value", value);
            payload.put("timestamp", timestamp);
            payload.put("routeid", 0);
            payload.put("transporttype", "");
            payload.put("operatorname", "");
            payload.put("passengername", "");

            // Constructing the schema
            List<Map<String, Object>> fields = Arrays.asList(
                    Map.of("field", "metricid", "type", "string"),
                    Map.of("field", "metrictype", "type", "string"),
                    Map.of("field", "value", "type", "double"),
                    Map.of("field", "timestamp", "type", "string"),
                    Map.of("field", "routeid", "type", "int32"),
                    Map.of("field", "transporttype", "type", "string"),
                    Map.of("field", "operatorname", "type", "string"),
                    Map.of("field", "passengername", "type", "string")
            );

            Map<String, Object> schema = new HashMap<>();
            schema.put("type", "struct");
            schema.put("fields", fields);
            schema.put("optional", false);
            schema.put("name", "Metrics");

            // Combining schema and payload into the final message
            Map<String, Object> message = new HashMap<>();
            message.put("schema", schema);
            message.put("payload", payload);

            try {
                JsonNode jsonNode = objectMapper.valueToTree(message); // Converts safely to a JSON tree structure
                String jsonMessage = objectMapper.writeValueAsString(jsonNode);

                // Publish the message to Kafka
                ProducerRecord<String, String> record = new ProducerRecord<>(RESULTS_TOPIC, null, jsonMessage);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        logger.error("Failed to publish message to topic", exception);
                    } else {
                        logger.info("Message published to topic {} with offset {}", metadata.topic(), metadata.offset());
                    }
                });

            } catch (JsonProcessingException e) {
                logger.error("Failed to serialize message to JSON", e);
            } catch (Exception e) {
                logger.error("Error while creating or publishing message to Kafka topic", e);
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

            // Generating dynamic values
            UUID metricId = UUID.randomUUID();
            String metricType = "TotalPassengersForAllTrips";
            String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()); // Dynamic timestamp

            // Constructing the payload
            Map<String, Object> payload = new HashMap<>();
            payload.put("metricid", metricId.toString());
            payload.put("metrictype", metricType);
            payload.put("value", value);
            payload.put("timestamp", timestamp);
            payload.put("routeid", 0);
            payload.put("transporttype", "");
            payload.put("operatorname", "");
            payload.put("passengername", "");

            // Constructing the schema
            List<Map<String, Object>> fields = Arrays.asList(
                    Map.of("field", "metricid", "type", "string"),
                    Map.of("field", "metrictype", "type", "string"),
                    Map.of("field", "value", "type", "double"),
                    Map.of("field", "timestamp", "type", "string"),
                    Map.of("field", "routeid", "type", "int32"),
                    Map.of("field", "transporttype", "type", "string"),
                    Map.of("field", "operatorname", "type", "string"),
                    Map.of("field", "passengername", "type", "string")
            );

            Map<String, Object> schema = new HashMap<>();
            schema.put("type", "struct");
            schema.put("fields", fields);
            schema.put("optional", false);
            schema.put("name", "Metrics");

            // Combining schema and payload into the final message
            Map<String, Object> message = new HashMap<>();
            message.put("schema", schema);
            message.put("payload", payload);

            try {
                JsonNode jsonNode = objectMapper.valueToTree(message); // Converts safely to a JSON tree structure
                String jsonMessage = objectMapper.writeValueAsString(jsonNode);

                // Publish the message to Kafka
                ProducerRecord<String, String> record = new ProducerRecord<>(RESULTS_TOPIC, null, jsonMessage);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        logger.error("Failed to publish message to topic", exception);
                    } else {
                        logger.info("Message published to topic {} with offset {}", metadata.topic(), metadata.offset());
                    }
                });

            } catch (JsonProcessingException e) {
                logger.error("Failed to serialize message to JSON", e);
            } catch (Exception e) {
                logger.error("Error while creating or publishing message to Kafka topic", e);
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
            try {
                json = new ObjectMapper().readTree(value);
            // Generating dynamic values
            UUID metricId = UUID.randomUUID();
            String metricType = "OccupancyPercentagePerTrip";
            String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()); // Dynamic timestamp
            double valueField = json.has("occupancyPercentage") ? json.get("occupancyPercentage").asDouble() : 0.0;

            // Constructing the payload
            Map<String, Object> payload = new HashMap<>();
            payload.put("metricid", metricId.toString());
            payload.put("metrictype", metricType);
            payload.put("value", valueField);
            payload.put("timestamp", timestamp);
            payload.put("routeid", json.has("routeId") ? json.get("routeId").asInt() : 0);
            payload.put("transporttype", json.get("transportType").asText());
            payload.put("operatorname", "");
            payload.put("passengername", "");

            // Constructing the schema
            List<Map<String, Object>> fields = Arrays.asList(
                    Map.of("field", "metricid", "type", "string"),
                    Map.of("field", "metrictype", "type", "string"),
                    Map.of("field", "value", "type", "double"),
                    Map.of("field", "timestamp", "type", "string"),
                    Map.of("field", "routeid", "type", "int32"),
                    Map.of("field", "transporttype", "type", "string"),
                    Map.of("field", "operatorname", "type", "string"),
                    Map.of("field", "passengername", "type", "string")
            );

            Map<String, Object> schema = new HashMap<>();
            schema.put("type", "struct");
            schema.put("fields", fields);
            schema.put("optional", false);
            schema.put("name", "Metrics");

            // Combining schema and payload into the final message
            Map<String, Object> message = new HashMap<>();
            message.put("schema", schema);
            message.put("payload", payload);


                JsonNode jsonNode = objectMapper.valueToTree(message); // Converts safely to a JSON tree structure
                String jsonMessage = objectMapper.writeValueAsString(jsonNode);

                // Publish the message to Kafka
                ProducerRecord<String, String> record = new ProducerRecord<>(RESULTS_TOPIC, null, jsonMessage);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        logger.error("Failed to publish message to topic", exception);
                    } else {
                        logger.info("Message published to topic {} with offset {}", metadata.topic(), metadata.offset());
                    }
                });

            } catch (JsonProcessingException e) {
                logger.error("Failed to serialize message to JSON", e);
            } catch (Exception e) {
                logger.error("Error while creating or publishing message to Kafka topic", e);
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
               // Generating dynamic values
                UUID metricId = UUID.randomUUID();
                String metricType = "AvailableSeatsPerRoute";
                String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()); // Dynamic timestamp

                // Constructing the payload
                Map<String, Object> payload = new HashMap<>();
                payload.put("metricid", metricId.toString());
                payload.put("metrictype", metricType);
                payload.put("value", route.getCapacity());
                payload.put("timestamp", timestamp);
                payload.put("routeid", route.getId());
                payload.put("transporttype", route.getTransportType());
                payload.put("operatorname", "");
                payload.put("passengername", "");

                // Constructing the schema
                List<Map<String, Object>> fields = Arrays.asList(
                        Map.of("field", "metricid", "type", "string"),
                        Map.of("field", "metrictype", "type", "string"),
                        Map.of("field", "value", "type", "double"),
                        Map.of("field", "timestamp", "type", "string"),
                        Map.of("field", "routeid", "type", "int32"),
                        Map.of("field", "transporttype", "type", "string"),
                        Map.of("field", "operatorname", "type", "string"),
                        Map.of("field", "passengername", "type", "string")
                );

                Map<String, Object> schema = new HashMap<>();
                schema.put("type", "struct");
                schema.put("fields", fields);
                schema.put("optional", false);
                schema.put("name", "Metrics");

                // Combining schema and payload into the final message
                Map<String, Object> message = new HashMap<>();
                message.put("schema", schema);
                message.put("payload", payload);

                try {
                    JsonNode jsonNode = objectMapper.valueToTree(message); // Converts safely to a JSON tree structure
                    String jsonMessage = objectMapper.writeValueAsString(jsonNode);

                    // Publish the message to Kafka
                    ProducerRecord<String, String> record = new ProducerRecord<>(RESULTS_TOPIC, null, jsonMessage);
                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            logger.error("Failed to publish message to topic", exception);
                        } else {
                            logger.info("Message published to topic {} with offset {}", metadata.topic(), metadata.offset());
                        }
                    });

                } catch (JsonProcessingException e) {
                    logger.error("Failed to serialize message to JSON", e);
                } catch (Exception e) {
                    logger.error("Error while creating or publishing message to Kafka topic", e);
                }
            }

        routesStream.to(RESULTS_TOPIC, Produced.with(Serdes.String(), routeSerde));
        });
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
            try {
           // Generating dynamic values
            UUID metricId = UUID.randomUUID();
            String metricType = "TotalPassengersPerRoute";
            String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()); // Dynamic timestamp

                 JsonNode json = new ObjectMapper().readTree(value);
                 double valueField = json.get("totalPassengers").asDouble();

                // Constructing the payload
            Map<String, Object> payload = new HashMap<>();
            payload.put("metricid", metricId.toString());
            payload.put("metrictype", metricType);
            payload.put("value", valueField);
            payload.put("timestamp", timestamp);
            payload.put("routeid", json.get("routeID").asInt());
            payload.put("transporttype", json.hasNonNull("transportType") ? json.get("transportType").asText() : "Unknown");
            payload.put("operatorname", "");
            payload.put("passengername", "");

            // Constructing the schema
            List<Map<String, Object>> fields = Arrays.asList(
                    Map.of("field", "metricid", "type", "string"),
                    Map.of("field", "metrictype", "type", "string"),
                    Map.of("field", "value", "type", "double"),
                    Map.of("field", "timestamp", "type", "string"),
                    Map.of("field", "routeid", "type", "int32"),
                    Map.of("field", "transporttype", "type", "string"),
                    Map.of("field", "operatorname", "type", "string"),
                    Map.of("field", "passengername", "type", "string")
            );

            Map<String, Object> schema = new HashMap<>();
            schema.put("type", "struct");
            schema.put("fields", fields);
            schema.put("optional", false);
            schema.put("name", "Metrics");

            // Combining schema and payload into the final message
            Map<String, Object> message = new HashMap<>();
            message.put("schema", schema);
            message.put("payload", payload);

                JsonNode jsonNode = objectMapper.valueToTree(message); // Converts safely to a JSON tree structure
                String jsonMessage = objectMapper.writeValueAsString(jsonNode);

                // Publish the message to Kafka
                ProducerRecord<String, String> record = new ProducerRecord<>(RESULTS_TOPIC, null, jsonMessage);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        logger.error("Failed to publish message to topic", exception);
                    } else {
                        logger.info("Message published to topic {} with offset {}", metadata.topic(), metadata.offset());
                    }
                });

            } catch (JsonProcessingException e) {
                logger.error("Failed to serialize message to JSON", e);
            } catch (Exception e) {
                logger.error("Error while creating or publishing message to Kafka topic", e);
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

// Generating dynamic values
        UUID metricId = UUID.randomUUID();
        String metricType = "MostUsedTransportTypeInLastHour";
        String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()); // Dynamic timestamp

        // Constructing the payload
        Map<String, Object> payload = new HashMap<>();
        payload.put("metricid", metricId.toString());
        payload.put("metrictype", metricType);
        payload.put("value", count);
        payload.put("timestamp", timestamp);
        payload.put("routeid", 0);
        payload.put("transporttype", transportType);
        payload.put("operatorname", "");
        payload.put("passengername", "");

        // Constructing the schema
        List<Map<String, Object>> fields = Arrays.asList(
                Map.of("field", "metricid", "type", "string"),
                Map.of("field", "metrictype", "type", "string"),
                Map.of("field", "value", "type", "double"),
                Map.of("field", "timestamp", "type", "string"),
                Map.of("field", "routeid", "type", "int32"),
                Map.of("field", "transporttype", "type", "string"),
                Map.of("field", "operatorname", "type", "string"),
                Map.of("field", "passengername", "type", "string")
        );

        Map<String, Object> schema = new HashMap<>();
        schema.put("type", "struct");
        schema.put("fields", fields);
        schema.put("optional", false);
        schema.put("name", "Metrics");

        // Combining schema and payload into the final message
        Map<String, Object> message = new HashMap<>();
        message.put("schema", schema);
        message.put("payload", payload);

        try {
            JsonNode jsonNode = objectMapper.valueToTree(message); // Converts safely to a JSON tree structure
            String jsonMessage = objectMapper.writeValueAsString(jsonNode);

            // Publish the message to Kafka
            ProducerRecord<String, String> record = new ProducerRecord<>(RESULTS_TOPIC, null, jsonMessage);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Failed to publish message to topic", exception);
                } else {
                    logger.info("Message published to topic {} with offset {}", metadata.topic(), metadata.offset());
                }
            });

        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize message to JSON", e);
        } catch (Exception e) {
            logger.error("Error while creating or publishing message to Kafka topic", e);
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

                    // Generating dynamic values
                    UUID metricId = UUID.randomUUID();
                    String metricType = "LeastOccupiedTransportTypeInLastHour";
                    String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()); // Dynamic timestamp

                    // Constructing the payload
                    Map<String, Object> payload = new HashMap<>();
                    payload.put("metricid", metricId.toString());
                    payload.put("metrictype", metricType);
                    payload.put("value", occupancyPercentage);
                    payload.put("timestamp", timestamp);
                    payload.put("routeid", 0);
                    payload.put("transporttype", transportType);
                    payload.put("operatorname", "");
                    payload.put("passengername", "");

                    // Constructing the schema
                    List<Map<String, Object>> fields = Arrays.asList(
                            Map.of("field", "metricid", "type", "string"),
                            Map.of("field", "metrictype", "type", "string"),
                            Map.of("field", "value", "type", "double"),
                            Map.of("field", "timestamp", "type", "string"),
                            Map.of("field", "routeid", "type", "int32"),
                            Map.of("field", "transporttype", "type", "string"),
                            Map.of("field", "operatorname", "type", "string"),
                            Map.of("field", "passengername", "type", "string")
                    );

                    Map<String, Object> schema = new HashMap<>();
                    schema.put("type", "struct");
                    schema.put("fields", fields);
                    schema.put("optional", false);
                    schema.put("name", "Metrics");

                    // Combining schema and payload into the final message
                    Map<String, Object> message = new HashMap<>();
                    message.put("schema", schema);
                    message.put("payload", payload);

                    try {
                        JsonNode jsonNode = objectMapper.valueToTree(message); // Converts safely to a JSON tree structure
                        String jsonMessage = objectMapper.writeValueAsString(jsonNode);

                        // Publish the message to Kafka
                        ProducerRecord<String, String> record = new ProducerRecord<>(RESULTS_TOPIC, null, jsonMessage);
                        producer.send(record, (metadata, exception) -> {
                            if (exception != null) {
                                logger.error("Failed to publish message to topic", exception);
                            } else {
                                logger.info("Message published to topic {} with offset {}", metadata.topic(), metadata.offset());
                            }
                        });

                    } catch (JsonProcessingException e) {
                        logger.error("Failed to serialize message to JSON", e);
                    } catch (Exception e) {
                        logger.error("Error while creating or publishing message to Kafka topic", e);
                    }
                });

        windowedOccupancy.toStream()
                .map((key, value) -> KeyValue.pair(key.key(), value.toString()))
                .to(RESULTS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }

    // Helper method to get Kafka producer properties
    private static Properties getProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }
}
