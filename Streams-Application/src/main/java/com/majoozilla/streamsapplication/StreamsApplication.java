package com.majoozilla.streamsapplication;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.majoozilla.streamsapplication.database.Database;
import com.majoozilla.streamsapplication.kafka.*;
import com.majoozilla.streamsapplication.kafka.service.StreamProcessingService;
import com.majoozilla.streamsapplication.models.Route;
import com.majoozilla.streamsapplication.models.Trip;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.JsonSerde;

import org.apache.kafka.streams.kstream.KStream;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.UUID;

import static com.majoozilla.streamsapplication.kafka.service.StreamProcessingService.*;

public class StreamsApplication {

    private static final String TRIPS_TOPIC = "Trips";
    private static final String ROUTES_TOPIC = "Routes";
    static Serde<Trip> tripSerde = new JsonSerde<>(Trip.class);
    static Serde<Route> routeSerde = new JsonSerde<>(Route.class);

    private static final Logger logger = LoggerFactory.getLogger(StreamProcessingService.class);

    public static void main(String[] args) {
        Database.setupDataSource();
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Trip> trips = builder.stream(TRIPS_TOPIC, Consumed.with(Serdes.String(), tripSerde));
        KTable<String, Route> routesTable = builder.table(ROUTES_TOPIC, Consumed.with(Serdes.String(), routeSerde));

        // TotalPassengersPerRoute
        TotalPassengersPerRoute(trips, routesTable);

        // AvailableSeatsPerRoute
        AvailableSeatsPerRoute(routesTable);

        // OccupancyPercentagePerTrip
        OccupancyPercentagePerTrip(trips);

        // TotalPassengersForAllTrips
        TotalPassengersForAllTrips(trips);

        // TotalSeatingCapacityForAllRoutes
        TotalSeatingCapacityForAllRoutes(routesTable);

        // TotalOccupancyPercentageForAllRoutes
        TotalOccupancyPercentageForAllRoutes(trips, routesTable);

        // AveragePassengersPerTransportType
        AveragePassengersPerTransportType(trips);

        // TransportTypeWithMaxPassengers
        TransportTypeWithMaxPassengers(trips);

        // RoutesWithLeastOccupancyPerTransportType
        RoutesWithLeastOccupancyPerTransportType(trips, routesTable);

        // OperatorWithMaxOccupancy
        OperatorWithMaxOccupancy(trips);

        // PassengerWithMostTrips
        PassengerWithMostTrips(trips);

        // MostUsedTransportTypeInLastHour
        MostUsedTransportTypeInLastHour(trips);

        // LeastOccupiedTransportTypeInLastHour
        LeastOccupiedTransportTypeInLastHour(trips);


        // Initialize and start the stream
        KafkaStreams streams = new KafkaStreams(builder.build(), StreamConfig.getStreamProperties());
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}