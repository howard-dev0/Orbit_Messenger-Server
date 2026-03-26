package com.orbitserver.gui;

import com.orbitserver.network.OrbitServer;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Font;
import java.text.SimpleDateFormat;
import java.util.Date;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.SwingUtilities;

public class ServerDashboard extends JFrame {
    private JTextArea consoleLogs;

    public ServerDashboard() {
        setTitle("Orbit Server Control Panel");
        setSize(700, 500);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLocationRelativeTo(null);

        // Setup a hacker-style dark console
        consoleLogs = new JTextArea();
        consoleLogs.setEditable(false);
        consoleLogs.setBackground(new Color(30, 30, 30));
        consoleLogs.setForeground(new Color(0, 255, 0));
        consoleLogs.setFont(new Font("Consolas", Font.PLAIN, 14));
        
        add(new JScrollPane(consoleLogs), BorderLayout.CENTER);

        // Start the server thread immediately when GUI opens
        new OrbitServer(this).start();
    }

    // Method to write to the console safely from other threads
    public void log(String message) {
        SwingUtilities.invokeLater(() -> {
            String time = new SimpleDateFormat("HH:mm:ss").format(new Date());
            consoleLogs.append("[" + time + "] " + message + "\n");
            // Auto-scroll to bottom
            consoleLogs.setCaretPosition(consoleLogs.getDocument().getLength());
        });
    }
}