package com.majoozilla.routesapplication.kafka;

import com.majoozilla.routesapplication.routes.Route;
import com.majoozilla.routesapplication.routes.RouteRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Random;

@Component
public class RouteSimulator {
    @Value("${simulation.origins}")
    private String[] origins;

    @Value("${simulation.destinations}")
    private String[] destinations;

    @Value("${simulation.transportTypes}")
    private String[] transportTypes;

    @Value("${simulation.operators}")
    private String[] operators;

    private final RouteProducer routeProducer;
    private final Random random = new Random();
    private final RouteRepository routeRepository;

    public RouteSimulator(RouteProducer routeProducer, RouteRepository routeRepository) {
        this.routeProducer = routeProducer;
        this.routeRepository = routeRepository;
    }

    @Scheduled(fixedRateString = "${simulation.fixed-rate:10000}")
    public void simulateRoute() {
        List<Route> predefinedRoutes = routeRepository.findAll();
        if (!predefinedRoutes.isEmpty() && random.nextBoolean()) { // 50% chance
            Route randomRoute = predefinedRoutes.get(random.nextInt(predefinedRoutes.size()));
            routeProducer.sendRoute(randomRoute);
        } else {
            // Generate random route as fallback
            generateRandomRoute();
        }
    }

    private void generateRandomRoute() {
        Route route = new Route();
        route.setCapacity(10 + random.nextInt(191));
        route.setOrigin(origins[random.nextInt(origins.length)]);
        route.setDestination(destinations[random.nextInt(destinations.length)]);
        route.setTransportType(transportTypes[random.nextInt(transportTypes.length)]);
        route.setOperatorName(operators[random.nextInt(operators.length)]);
        Route savedRoute = routeRepository.save(route); // Save to get an ID
        routeProducer.sendRoute(savedRoute);
    }



}
