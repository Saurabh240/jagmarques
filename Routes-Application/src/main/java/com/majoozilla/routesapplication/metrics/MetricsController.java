package com.majoozilla.routesapplication.metrics;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/metrics")
public class MetricsController {

    private final MetricsService metricsService;

    public MetricsController(MetricsService metricsService) {
        this.metricsService = metricsService;
    }


    @GetMapping
    public ResponseEntity<List<Metrics>> getAllMetrics() {
        List<Metrics> metrics = metricsService.listAllMetrics();
        return ResponseEntity.ok(metrics);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Metrics> getMetricById(@PathVariable UUID id) {
        Metrics metric = metricsService.getMetricById(id);
        return metric != null ? ResponseEntity.ok(metric) : ResponseEntity.notFound().build();
    }

    @GetMapping("/type/{type}")
    public ResponseEntity<List<Metrics>> getMetricsByType(@PathVariable String type) {
        List<Metrics> metrics = metricsService.getMetricsByType(type);
        return metrics.isEmpty() ? ResponseEntity.noContent().build() : ResponseEntity.ok(metrics);
    }

}
