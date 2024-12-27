package com.majoozilla.routesapplication.routes;

import com.majoozilla.routesapplication.kafka.ApplicationContextProvider;
import com.majoozilla.routesapplication.kafka.RouteProducer;
import jakarta.annotation.PostConstruct;
import jakarta.persistence.*;

@EntityListeners(RouteEntityListener.class)
@Entity
public class Route {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String origin;
    private String destination;
    private int capacity;
    private String transportType;
    private String operatorName;

    public Route() {

    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public int getCapacity() {
        return capacity;
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    public String getTransportType() {
        return transportType;
    }

    public void setTransportType(String transportType) {
        this.transportType = transportType;
    }

    public String getOperatorName() {
        return operatorName;
    }

    public void setOperatorName(String operatorName) {
        this.operatorName = operatorName;
    }

    public Route(Long id, String origin, String destination, int capacity, String transportType, String operatorName) {
        this.id = id;
        this.origin = origin;
        this.destination = destination;
        this.capacity = capacity;
        this.transportType = transportType;
        this.operatorName = operatorName;
    }
}

class RouteEntityListener {
    @PostUpdate
    public void onRouteChanged(Route route) {
        RouteProducer routeProducer = ApplicationContextProvider.getBean(RouteProducer.class);
        routeProducer.sendRouteInfoToDBInfoTopic(route);
    }
}