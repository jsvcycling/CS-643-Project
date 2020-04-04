package edu.njit.jsv28.cs643.project;

import java.sql.*;

public class Main {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: <start location> <end location>");
            System.exit(1);
        }

        String startLocation = args[0];
        String endLocation = args[1];

        String connectionString = ""; // TODO: load this from environment variables or set it to a constant.
        String query = "SELECT ..."; // TODO: write query.

        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
           e.printStackTrace();
           System.exit(1);
        }

        try (
                Connection connection = DriverManager.getConnection(connectionString);
                PreparedStatement statement = connection.prepareStatement(query)
        ) {
            statement.setString(1, startLocation); // The first query parameter will be the start location.
            statement.setString(2, endLocation);   // The second query parameter will be the end location.

            System.out.println("Executing query... This may take a few minutes.");

            ResultSet results = statement.executeQuery(query);

            while (results.next()) {
                // TODO: output results to stdout.
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}