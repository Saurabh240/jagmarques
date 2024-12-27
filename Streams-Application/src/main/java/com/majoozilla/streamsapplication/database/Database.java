package com.majoozilla.streamsapplication.database;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class Database {
    private static HikariDataSource dataSource;

    public static void setupDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://localhost:5432/routes_db");
        config.setUsername("postgres");
        config.setPassword("postgres");
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        dataSource = new HikariDataSource(config);

        // Create the table if it doesn't exist
        createMetricsTableIfNotExists();
    }

    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    private static void createMetricsTableIfNotExists() {
        String createTableSQL = """
                CREATE TABLE IF NOT EXISTS metrics (
                    metricID UUID PRIMARY KEY,
                    metricType VARCHAR(255) NOT NULL,
                    value DOUBLE PRECISION NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    routeID INT,
                    transportType VARCHAR(255),
                    operatorName VARCHAR(255),
                    passengerName VARCHAR(255)
                );
                """;

        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute(createTableSQL);
            System.out.println("Metrics table ensured to exist.");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create metrics table", e);
        }
    }
}
