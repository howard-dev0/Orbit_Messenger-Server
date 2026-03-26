package com.orbitserver.main;

import com.orbitserver.gui.ServerDashboard;

public class ServerLauncher {
    public static void main(String[] args) {
        // You can add FlatLaf here if you imported it to the server project!
        java.awt.EventQueue.invokeLater(() -> {
            new ServerDashboard().setVisible(true);
        });
    }
}