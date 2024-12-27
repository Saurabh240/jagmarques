package com.majoozilla.routesapplication.metrics;


import jakarta.persistence.*;

import java.util.Date;
import java.util.UUID;

@Entity
@Table(name = "metrics")
public class Metrics {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private UUID metricID;

    @Column(nullable = false, name = "metrictype")
    private String metricType;

    @Column(nullable = false, name = "value")
    private double value;

    @Column(nullable = false, name = "timestamp")
    private Date timestamp;

    @Column(name = "routeid")
    private Integer routeID;  // Use object Integer to allow null

    @Column(name = "transporttype")
    private String transportType;

    @Column(name = "operatorname")
    private String operatorName;

    @Column(name = "passengername")
    private String passengerName;

    // Constructors, getters, and setters

    public Metrics() {
    }

    public Metrics(UUID metricID, String metricType, double value, Date timestamp, Integer routeID, String transportType, String operatorName, String passengerName) {
        this.metricID = metricID;
        this.metricType = metricType;
        this.value = value;
        this.timestamp = timestamp;
        this.routeID = routeID;
        this.transportType = transportType;
        this.operatorName = operatorName;
        this.passengerName = passengerName;
    }

    // Getters and setters

    public UUID getMetricID() {
        return metricID;
    }

    public void setMetricID(UUID metricID) {
        this.metricID = metricID;
    }

    public String getMetricType() {
        return metricType;
    }

    public void setMetricType(String metricType) {
        this.metricType = metricType;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public Integer getRouteID() {
        return routeID;
    }

    public void setRouteID(Integer routeID) {
        this.routeID = routeID;
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

    public String getPassengerName() {
        return passengerName;
    }

    public void setPassengerName(String passengerName) {
        this.passengerName = passengerName;
    }
}
