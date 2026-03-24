package com.factory.deliveredcars.web;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class DeliveredCarRepository {

    private final JdbcTemplate jdbcTemplate;

    public DeliveredCarRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<DeliveredCar> findPage(String model, String destination, LocalDate deliveryDate, int page, int size) {
        FilterQuery filterQuery = buildFilterQuery(model, destination, deliveryDate);
        String sql = """
                SELECT id, vin, model, color, production_line, destination, delivery_date, event_ts
                FROM vehicle_completed_events
                """;
        sql = sql + filterQuery.whereClause() + " ORDER BY event_ts DESC LIMIT ? OFFSET ?";

        List<Object> params = new ArrayList<>(filterQuery.args());
        params.add(size);
        params.add(page * size);

        return jdbcTemplate.query(sql, (rs, rowNum) -> mapDeliveredCar(rs), params.toArray());
    }

    public int count(String model, String destination, LocalDate deliveryDate) {
        FilterQuery filterQuery = buildFilterQuery(model, destination, deliveryDate);
        String sql = "SELECT COUNT(*) FROM vehicle_completed_events" + filterQuery.whereClause();
        Integer total = jdbcTemplate.queryForObject(sql, Integer.class, filterQuery.args().toArray());
        return total == null ? 0 : total;
    }

    private FilterQuery buildFilterQuery(String model, String destination, LocalDate deliveryDate) {
        List<String> conditions = new ArrayList<>();
        List<Object> args = new ArrayList<>();

        if (model != null && !model.isBlank()) {
            conditions.add("model ILIKE ?");
            args.add("%" + model.trim() + "%");
        }

        if (destination != null && !destination.isBlank()) {
            conditions.add("destination ILIKE ?");
            args.add("%" + destination.trim() + "%");
        }

        if (deliveryDate != null) {
            conditions.add("delivery_date = ?");
            args.add(deliveryDate);
        }

        if (conditions.isEmpty()) {
            return new FilterQuery("", args);
        }

        return new FilterQuery(" WHERE " + String.join(" AND ", conditions), args);
    }

    private DeliveredCar mapDeliveredCar(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new DeliveredCar(
                rs.getLong("id"),
                rs.getString("vin"),
                rs.getString("model"),
                rs.getString("color"),
                rs.getString("production_line"),
                rs.getString("destination"),
                rs.getObject("delivery_date", LocalDate.class),
                rs.getObject("event_ts", OffsetDateTime.class)
        );
    }

    private record FilterQuery(String whereClause, List<Object> args) {
    }
}
