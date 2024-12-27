package com.majoozilla.tripsapplication.trips;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.UUID;

public class Trip {
    @JsonProperty("tripId")
    private String tripId;

    @JsonProperty("routeId")
    private String routeId;

    @JsonProperty("origin")
    private String origin;

    @JsonProperty("destination")
    private String destination;

    @JsonProperty("transportType")
    private String transportType;

    @JsonProperty("operatorName")
    private String operatorName;

    // Constructors
    public Trip() {
        this.tripId = UUID.randomUUID().toString();
    }

    // Getters and setters with @JsonProperty if needed for consistency
    public String getTripId() {
        return tripId;
    }

    public void setTripId(String tripId) {
        this.tripId = tripId;
    }

    public String getRouteId() {
        return routeId;
    }

    public void setRouteId(String routeId) {
        this.routeId = routeId;
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

    @JsonProperty("capacity")
    private int capacity; // Number of total available seats on the vehicle

    @JsonProperty("currentPassengers")
    private int currentPassengers; // Number of passengers currently on the vehicle

    public int getCapacity() {
        return capacity;
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    public int getCurrentPassengers() {
        return currentPassengers;
    }

    public void setCurrentPassengers(int currentPassengers) {
        this.currentPassengers = currentPassengers;
    }

    @JsonProperty("passengerName")
    private String passengerName;

    public String getPassengerName() {
        return passengerName;
    }

    public void setPassengerName(String passengerName) {
        this.passengerName = passengerName;
    }
}
