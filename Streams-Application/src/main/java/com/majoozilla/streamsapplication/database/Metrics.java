package com.majoozilla.streamsapplication.database;

import java.util.Date;

public class Metrics {
    private int metricID;
    private String metricType;
    private double value;
    private Date timestamp;
    private Integer routeID;  // Use object Integer to allow null
    private String transportType;
    private String operatorName;
    private String passengerName;

    // Constructors, getters, and setters

    public Metrics() {
    }

    public Metrics(int metricID, String metricType, double value, Date timestamp, Integer routeID, String transportType, String operatorName, String passengerName) {
        this.metricID = metricID;
        this.metricType = metricType;
        this.value = value;
        this.timestamp = timestamp;
        this.routeID = routeID;
        this.transportType = transportType;
        this.operatorName = operatorName;
        this.passengerName = passengerName;
    }

    // Getters and setters for each field

    public int getMetricID() {
        return metricID;
    }

    public void setMetricID(int metricID) {
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

    public void setOperatorName(String operatorName) {
        this.operatorName = operatorName;
    }

    public String getOperatorName() {
        return operatorName;
    }

    public String getPassengerName() {
        return passengerName;
    }

    public void setPassengerName(String passengerName) {
        this.passengerName = passengerName;
    }
}
