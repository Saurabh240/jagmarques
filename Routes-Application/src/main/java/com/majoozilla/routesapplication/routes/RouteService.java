package com.majoozilla.routesapplication.routes;

import com.majoozilla.routesapplication.kafka.RouteProducer;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class RouteService {


    private final RouteRepository routeRepository;

    private final RouteProducer routeProducer;

    public RouteService(RouteRepository routeRepository, RouteProducer routeProducer) {
        this.routeRepository = routeRepository;
        this.routeProducer = routeProducer;
    }

    public Route addRoute(Route route) {
        Route savedRoute = routeRepository.save(route);
        routeProducer.sendRoute(savedRoute);
        return savedRoute;
    }

    public Route updateRoute(Long id, Route routeDetails) {
        return routeRepository.findById(id)
                .map(route -> {
                    route.setOrigin(routeDetails.getOrigin());
                    route.setDestination(routeDetails.getDestination());
                    route.setCapacity(routeDetails.getCapacity());
                    route.setTransportType(routeDetails.getTransportType());
                    route.setOperatorName(routeDetails.getOperatorName());
                    Route updatedRoute = routeRepository.save(route);
                    routeProducer.sendRoute(updatedRoute); // Send updated info to Kafka
                    return updatedRoute;
                }).orElseThrow(() -> new RuntimeException("Route not found with id " + id));
    }

    public List<Route> listRoutes() {
        return routeRepository.findAll();
    }
}
