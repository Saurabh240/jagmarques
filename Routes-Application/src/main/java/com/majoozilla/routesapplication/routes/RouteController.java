package com.majoozilla.routesapplication.routes;

import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/routes")
public class RouteController {


    private final RouteService routeService;

    public RouteController(RouteService routeService) {
        this.routeService = routeService;
    }

    @PostMapping
    public Route addRoute(@RequestBody Route route) {
        return routeService.addRoute(route);
    }

    @PutMapping("/{id}")
    public Route updateRoute(@PathVariable Long id, @RequestBody Route route) {
        return routeService.updateRoute(id, route);
    }

    @GetMapping
    public List<Route> listRoutes() {
        return routeService.listRoutes();
    }
}
