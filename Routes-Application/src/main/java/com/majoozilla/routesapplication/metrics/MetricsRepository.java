package com.majoozilla.routesapplication.metrics;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface MetricsRepository extends JpaRepository<Metrics, UUID> {
    List<Metrics> findByMetricType(String metricType);
}