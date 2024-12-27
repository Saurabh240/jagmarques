package com.majoozilla.tripsapplication.trips;

import com.majoozilla.tripsapplication.kafka.RouteCache;
import com.majoozilla.tripsapplication.kafka.TripProducer;
import com.majoozilla.tripsapplication.route.Route;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Random;
import java.util.UUID;

@Component
public class TripSimulator {
    private final TripProducer tripProducer;
    private final RouteCache routeCache;
    private final Random random = new Random();
    @Value("${simulation.passengers}")
    private String[] passengers;

    public TripSimulator(TripProducer tripProducer, RouteCache routeCache) {
        this.tripProducer = tripProducer;
        this.routeCache = routeCache;
    }

    @Scheduled(fixedRateString = "${simulation.fixed-rate:10000}")
    public void generateTrip() {
        Route route = routeCache.getRandomRoute();
        if (route != null) {
            Trip trip = new Trip();
            trip.setTripId(UUID.randomUUID().toString());
            trip.setRouteId(route.getId().toString());
            trip.setOrigin(route.getOrigin());
            trip.setDestination(route.getDestination());
            trip.setTransportType(route.getTransportType());
            trip.setOperatorName(route.getOperatorName());
            trip.setCapacity(route.getCapacity()); // Assuming capacity is part of the Route model
            trip.setCurrentPassengers(random.nextInt(route.getCapacity() + 1)); // Random number of passengers up to capacity
            trip.setPassengerName(passengers[random.nextInt(passengers.length)]);
            tripProducer.sendTrip(trip);
        }
    }
}


