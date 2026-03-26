package com.orbitserver.network;

import java.io.PrintWriter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;

public class PresenceManager {
    private static PresenceManager instance;
    
    // Maps Username -> Their active Network Output Stream
    private final ConcurrentHashMap<String, PrintWriter> activeUsers = new ConcurrentHashMap<>();

    private PresenceManager() {}

    public static PresenceManager getInstance() {
        if (instance == null) instance = new PresenceManager();
        return instance;
    }

    public void setUserOnline(String username, PrintWriter out) {
        activeUsers.put(username, out);
        System.out.println("Presence: " + username + " is now ONLINE.");
    }

    public void setUserOffline(String username) {
        activeUsers.remove(username);
        System.out.println("Presence: " + username + " is now OFFLINE.");
    }

    public boolean isUserOnline(String username) {
        return activeUsers.containsKey(username);
    }

    public PrintWriter getUserOutput(String username) {
        return activeUsers.get(username);
    }

    public Set<String> getAllOnlineUsers() {
        return activeUsers.keySet();
    }
    
    // Helper to send a message to a specific person if they are online
    public void sendToUser(String targetUsername, String message) {
        PrintWriter out = activeUsers.get(targetUsername);
        if (out != null) {
            out.println(message);
        }
    }
}