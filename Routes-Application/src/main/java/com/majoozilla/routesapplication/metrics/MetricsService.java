package com.majoozilla.routesapplication.metrics;

import org.springframework.stereotype.Service;
import java.util.List;

import java.util.Optional;
import java.util.UUID;

@Service
public class MetricsService {

    private final MetricsRepository metricsRepository;

    public MetricsService(MetricsRepository metricsRepository) {
        this.metricsRepository = metricsRepository;
    }

    public List<Metrics> getMetricsByType(String type) {
        return metricsRepository.findByMetricType(type);
    }


    public List<Metrics> listAllMetrics() {
        return metricsRepository.findAll();
    }

    public Metrics getMetricById(UUID id) {
        Optional<Metrics> metric = metricsRepository.findById(id);
        return metric.orElse(null);
    }

}
