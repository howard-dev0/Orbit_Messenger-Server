package com.orbitserver.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DBConnection {
    private static final String HOST = "localhost";
    private static final String PORT = "3306";
    private static final String DB_NAME = "orbit_db";
    private static final String USER = "root";
    private static final String PASS = ""; 

    private static Connection connection;

    public static Connection getConnection() {
        try {
            if (connection == null || connection.isClosed()) {
                String url = "jdbc:mysql://" + HOST + ":" + PORT + "/";
                connection = DriverManager.getConnection(url, USER, PASS);
                initDatabase();
                connection = DriverManager.getConnection(url + DB_NAME, USER, PASS);
            }
        } catch (SQLException e) {
            System.err.println("DB Connection Failed: " + e.getMessage());
        }
        return connection;
    }

    private static void initDatabase() {
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS " + DB_NAME);
            stmt.execute("USE " + DB_NAME);
            String createUsers = "CREATE TABLE IF NOT EXISTS users ("
                    + "id INT AUTO_INCREMENT PRIMARY KEY, "
                    + "full_name VARCHAR(32) NOT NULL, "
                    + "email VARCHAR(255) UNIQUE NOT NULL, "
                    + "username VARCHAR(50) UNIQUE NOT NULL, "
                    + "password VARCHAR(255) NOT NULL, "
                    + "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB;";
            stmt.executeUpdate(createUsers);
        } catch (SQLException e) {
            System.err.println("Init Error: " + e.getMessage());
        }
    }
}