package com.mycompany.updatecriticalcustomers;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.google.gson.Gson;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.util.Map;

public class UpdateCriticalCustomers implements RequestHandler<Map<String, Object>, String> {

    private final String dbHost = System.getenv("DB_HOST");
    private final String dbUser = System.getenv("DB_USER");
    private final String dbPass = System.getenv("DB_PASS");

    private final Gson gson = new Gson();

    // Incoming JSON → Java object
    private static class CriticalValveInput {
        Integer asset_id;
        Boolean critical_customer;
    }

    @Override
    public String handleRequest(Map<String, Object> event, Context context) {
        System.out.println("UpdateCriticalCustomers Lambda started");
        System.out.println("Incoming event: " + gson.toJson(event));

        try {
            // Convert event map to object
            CriticalValveInput input = gson.fromJson(gson.toJson(event), CriticalValveInput.class);

            if (input.asset_id == null || input.critical_customer == null) {
                return "ERROR: JSON missing required fields: asset_id, critical_customer";
            }

            updateCriticalCustomerFlag(input.asset_id, input.critical_customer);

            return "SUCCESS: Updated asset_id=" + input.asset_id +
                   " critical_customer=" + input.critical_customer;

        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR: " + e.getMessage();
        }
    }

    private void updateCriticalCustomerFlag(int assetId, boolean criticalCustomer) throws SQLException {
        String sql = """
                UPDATE valves
                SET critical_customer = ?
                WHERE asset_id = ?
                """;

        try (Connection conn = connectToDB();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setBoolean(1, criticalCustomer);
            stmt.setInt(2, assetId);

            int rows = stmt.executeUpdate();
            System.out.println("Rows modified: " + rows);
        }
    }

    private Connection connectToDB() {
        try {
            Class.forName("org.postgresql.Driver");

            String jdbcURL = "jdbc:postgresql://" + dbHost + ":5432/postgres";
            Connection conn = DriverManager.getConnection(jdbcURL, dbUser, dbPass);

            if (!conn.isValid(2)) {
                throw new RuntimeException("DB connection invalid");
            }

            System.out.println("DB connection successful");
            return conn;

        } catch (Exception e) {
            throw new RuntimeException("DB connection failed: " + e.getMessage(), e);
        }
    }
}
