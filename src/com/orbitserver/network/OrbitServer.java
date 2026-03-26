package com.orbitserver.network;

import com.orbitserver.db.DBConnection;
import com.orbitserver.gui.ServerDashboard;
import java.net.ServerSocket;
import java.net.Socket;

public class OrbitServer extends Thread {
    private final int PORT = 8080;
    private ServerDashboard dashboard;
    private boolean isRunning = true;

    public OrbitServer(ServerDashboard dashboard) {
        this.dashboard = dashboard;
    }

    @Override
    public void run() {
        // Init DB First
        if (DBConnection.getConnection() != null) {
            dashboard.log("✅ Database Connected Successfully.");
        } else {
            dashboard.log("❌ Database Connection Failed! Is XAMPP running?");
            return;
        }

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            dashboard.log("🚀 Orbit Server started on Port " + PORT);

            while (isRunning) {
                Socket clientSocket = serverSocket.accept();
                dashboard.log("📡 New connection from: " + clientSocket.getInetAddress().getHostAddress());
                
                // Hand the client to a new thread so the server doesn't freeze
                new ClientHandler(clientSocket, dashboard).start();
            }
        } catch (Exception e) {
            dashboard.log("❌ Server Error: " + e.getMessage());
        }
    }
}