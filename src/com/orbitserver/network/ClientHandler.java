package com.orbitserver.network;

import com.orbitserver.db.DBConnection;
import com.orbitserver.gui.ServerDashboard;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class ClientHandler extends Thread {

    private Socket socket;
    private ServerDashboard dashboard;
    public static java.util.concurrent.ConcurrentHashMap<String, PrintWriter> onlineUsers = new java.util.concurrent.ConcurrentHashMap<>();

    public ClientHandler(Socket socket, ServerDashboard dashboard) {
        this.socket = socket;
        this.dashboard = dashboard;
    }

    private String clientUsername;

@Override
    public void run() {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream())); 
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            String request;
            while ((request = in.readLine()) != null) {
                // 🚀 This INNER try-catch is the secret to a stable server!
                try {
                    String[] parts = request.split("\\|");
                    String command = parts[0];

                    switch (command) {
                        case "JOIN":
                            this.clientUsername = parts[1];
                            // Register Presence and Map
                            onlineUsers.put(clientUsername, out);
                            PresenceManager.getInstance().setUserOnline(clientUsername, out);
                            dashboard.log("🟢 " + clientUsername + " connected.");
                            
                            // Send chat list and notify friends
                            out.println("MY_CHATS|" + fetchMyChats(clientUsername));
                            broadcastStatusToFriends(clientUsername, "ONLINE");
                            break;

                        case "LOGIN":
                            String loginResult = loginUser(parts[1], parts[2]);
                            out.println(loginResult);
                            break;

                        case "REGISTER":
                            boolean success = registerUser(parts[1], parts[2], parts[3], parts[4]);
                            out.println(success ? "SUCCESS" : "FAILED");
                            break;

                        case "GET_PROFILE_DATA":
                            // parts = [GET_PROFILE_DATA, targetUsername, myUsername]
                            System.out.println("Server handling GET_PROFILE_DATA for: " + parts[1]);
                            String profileInfo = fetchProfileData(parts[1], parts[2]);
                            out.println("PROFILE_DATA|" + profileInfo);
                            break;

                        case "UPDATE_PROFILE":
                            // parts = [UPDATE_PROFILE, newName, username]
                            if (handleUpdateProfile(parts[1], parts[2])) {
                                out.println("UPDATE_SUCCESS|" + parts[1]);
                            }
                            break;

                        case "CREATE_POST":
                            savePostToDB(parts[2], parts[1]);
                            break;

                        case "GET_HOME_FEED":
                            String feedData = fetchHomeFeed(parts[1]);
                            out.println("HOME_FEED|" + feedData);
                            break;

                        case "SEND_MESSAGE":
                            handleSendMessage(parts[1], parts[2], parts[3]);
                            break;

                        case "LOAD_CHAT_HISTORY":
                            String history = loadChatHistory(parts[1], parts[2]);
                            out.println("CHAT_HISTORY|" + history);
                            break;

                        case "SEARCH_USERS":
                            String searchResults = searchUsers(parts[1], parts[2]);
                            out.println("SEARCH_RESULTS|" + searchResults);
                            break;

                        case "SEND_FRIEND_REQUEST":
                            sendFriendRequest(parts[2], parts[1].replace("@", ""));
                            break;

                        case "RESPOND_FRIEND_REQUEST":
                            respondToFriendRequest(parts[3], parts[1].replace("@", ""), parts[2]);
                            break;

                        case "REMOVE_FRIEND":
                            removeFriend(parts[2], parts[1].replace("@", ""));
                            break;

                        case "GET_FRIEND_REQUESTS":
                            String requests = fetchFriendRequests(parts[1]);
                            out.println("FRIEND_REQUESTS|" + requests);
                            break;

                        case "GET_ALL_FRIENDS":
                            String friends = fetchAllFriends(parts[1]);
                            out.println("ALL_FRIENDS|" + friends);
                            break;

                        case "GET_MY_CHATS":
                            String myChats = fetchMyChats(parts[1]);
                            out.println("MY_CHATS|" + myChats);
                            break;

                        case "CHECK_EMAIL":
                            boolean emailExists = checkExists("email", parts[1]);
                            out.println(emailExists ? "EXISTS" : "AVAILABLE");
                            break;

                        case "CHECK_USERNAME":
                            boolean userExists = checkExists("username", parts[1]);
                            out.println(userExists ? "EXISTS" : "AVAILABLE");
                            break;

                        default:
                            dashboard.log("⚠️ Unknown Command: " + command);
                            out.println("UNKNOWN_COMMAND");
                    }
                } catch (Exception e) {
                    // 🚨 If a database error happens, the server will print it here instead of crashing!
                    System.err.println("❌ ERROR PROCESSING COMMAND: " + request);
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            dashboard.log("⚠️ Client disconnected.");
        } finally {
            if (clientUsername != null) {
                onlineUsers.remove(clientUsername);
                PresenceManager.getInstance().setUserOffline(clientUsername);
                broadcastStatusToFriends(clientUsername, "OFFLINE");
            }
        }
    }
    
    private void broadcastStatusToFriends(String myUsername, String status) {
        int myId = getUserId(myUsername);
        if (myId == -1) return;

        // SQL to find all ACCEPTED friends of this user
        String sql = "SELECT u.username FROM friendships f " +
                     "JOIN users u ON (u.id = f.requester_id OR u.id = f.receiver_id) " +
                     "WHERE (f.requester_id = ? OR f.receiver_id = ?) " +
                     "AND u.id != ? AND f.status = 'ACCEPTED'";

        try (Connection conn = com.orbitserver.db.DBConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, myId);
            ps.setInt(2, myId);
            ps.setInt(3, myId);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String friendUsername = rs.getString("username");
                    
                    // 🚀 If the friend is online, send them the update packet
                    PrintWriter friendOut = PresenceManager.getInstance().getUserOutput(friendUsername);
                    if (friendOut != null) {
                        // Format: UPDATE_STATUS | WhoChanged | NewStatus
                        friendOut.println("UPDATE_STATUS|" + myUsername + "|" + status);
                        dashboard.log("📡 Notified " + friendUsername + " that " + myUsername + " is " + status);
                    }
                }
            }
        } catch (Exception e) {
            dashboard.log("Broadcast Error: " + e.getMessage());
        }
    }

    // ==============================================================
    // CORE DATABASE HELPERS
    // ==============================================================
    // Helper: Converts a username string into their Database ID integer
    private int getUserId(String username) {
        String sql = "SELECT id FROM users WHERE username = ?";
        try (Connection conn = DBConnection.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, username);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("id");
                }
            }
        } catch (Exception e) {
            dashboard.log("Error getting user ID: " + e.getMessage());
        }
        return -1;
    }

    private boolean checkExists(String column, String value) {
        String sql = "SELECT id FROM users WHERE " + column + " = ?";
        try (Connection conn = DBConnection.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, value);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (Exception e) {
            dashboard.log("DB Error: " + e.getMessage());
            return true;
        }
    }

    // ==============================================================
    // FRIENDSHIP LOGIC
    // ==============================================================
    private String searchUsers(String query, String myUsername) {
        StringBuilder sb = new StringBuilder();
        // Look for users that match the search, but exclude myself
        String sql = "SELECT username, full_name FROM users WHERE (username LIKE ? OR full_name LIKE ?) AND username != ? LIMIT 10";

        try (Connection conn = DBConnection.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, "%" + query + "%");
            ps.setString(2, "%" + query + "%");
            ps.setString(3, myUsername);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    sb.append(rs.getString("username")).append(":")
                            .append(rs.getString("full_name")).append(",");
                }
            }
        } catch (Exception e) {
            dashboard.log("Search Error: " + e.getMessage());
        }
        return sb.toString();
    }

    private void sendFriendRequest(String myUsername, String targetUsername) {
        int myId = getUserId(myUsername);
        int targetId = getUserId(targetUsername);

        if (myId == -1 || targetId == -1) {
            return;
        }

        String sql = "INSERT INTO friendships (requester_id, receiver_id, status) VALUES (?, ?, 'PENDING')";
        try (Connection conn = DBConnection.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, myId);
            ps.setInt(2, targetId);
            ps.executeUpdate();
            dashboard.log("Friend request sent from " + myUsername + " to " + targetUsername);
        } catch (Exception e) {
            dashboard.log("Friend Request Error: " + e.getMessage()); // Might catch duplicate request errors
        }
    }

    private void respondToFriendRequest(String myUsername, String targetUsername, String status) {
        int myId = getUserId(myUsername);
        int targetId = getUserId(targetUsername);

        if (status.equals("DECLINED")) {
            removeFriend(myUsername, targetUsername); // Just delete the request entirely
            return;
        }

        // If ACCEPTED, update the row
        String sql = "UPDATE friendships SET status = 'ACCEPTED' WHERE requester_id = ? AND receiver_id = ?";
        try (Connection conn = DBConnection.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, targetId); // The other person requested ME
            ps.setInt(2, myId);     // I am the receiver accepting it
            ps.executeUpdate();
            dashboard.log(myUsername + " accepted friend request from " + targetUsername);
        } catch (Exception e) {
            dashboard.log("Accept Request Error: " + e.getMessage());
        }
    }

    private void removeFriend(String myUsername, String targetUsername) {
        int myId = getUserId(myUsername);
        int targetId = getUserId(targetUsername);

        String sql = "DELETE FROM friendships WHERE (requester_id = ? AND receiver_id = ?) OR (requester_id = ? AND receiver_id = ?)";
        try (Connection conn = DBConnection.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, myId);
            ps.setInt(2, targetId);
            ps.setInt(3, targetId);
            ps.setInt(4, myId);
            ps.executeUpdate();
            dashboard.log("Friendship severed between " + myUsername + " and " + targetUsername);
        } catch (Exception e) {
            dashboard.log("Remove Friend Error: " + e.getMessage());
        }
    }

    // ==============================================================
    // ACCOUNT LOGIC
    // ==============================================================
    private String loginUser(String username, String passwordHash) {
        String sql = "SELECT full_name FROM users WHERE username = ? AND password = ?";
        try (Connection conn = DBConnection.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, username);
            ps.setString(2, passwordHash);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String fullName = rs.getString("full_name");
                    dashboard.log("🔓 User logged in: " + username);
                    return "SUCCESS|" + fullName;
                }
            }
        } catch (Exception e) {
            dashboard.log("Login Error: " + e.getMessage());
        }
        dashboard.log("🛑 Failed login attempt for: " + username);
        return "FAILED";
    }

    private boolean registerUser(String name, String email, String user, String pass) {
        String sql = "INSERT INTO users (full_name, email, username, password) VALUES (?, ?, ?, ?)";
        try (Connection conn = DBConnection.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, name);
            ps.setString(2, email);
            ps.setString(3, user);
            ps.setString(4, pass);
            ps.executeUpdate();
            dashboard.log("✅ New user registered: " + user);
            return true;
        } catch (Exception e) {
            dashboard.log("Register Error: " + e.getMessage());
            return false;
        }
    }

    private String fetchFriendRequests(String myUsername) {
        StringBuilder sb = new StringBuilder();
        int myId = getUserId(myUsername);
        if (myId == -1) {
            return "";
        }

        // Find users who sent ME a request that is still PENDING
        String sql = "SELECT u.username, u.full_name FROM friendships f "
                + "JOIN users u ON u.id = f.requester_id "
                + "WHERE f.receiver_id = ? AND f.status = 'PENDING'";

        try (Connection conn = DBConnection.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, myId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    sb.append(rs.getString("username")).append(":")
                            .append(rs.getString("full_name")).append(",");
                }
            }
        } catch (Exception e) {
            dashboard.log("Error fetching requests: " + e.getMessage());
        }
        return sb.toString();
    }

    private String fetchAllFriends(String myUsername) {
        StringBuilder sb = new StringBuilder();
        int myId = getUserId(myUsername);
        if (myId == -1) {
            return "";
        }

        // Find all ACCEPTED friendships
        String sql = "SELECT u.username, u.full_name FROM friendships f "
                + "JOIN users u ON (u.id = f.requester_id OR u.id = f.receiver_id) "
                + "WHERE (f.requester_id = ? OR f.receiver_id = ?) "
                + "AND u.id != ? AND f.status = 'ACCEPTED'";

        try (Connection conn = DBConnection.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, myId);
            ps.setInt(2, myId);
            ps.setInt(3, myId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    sb.append(rs.getString("username")).append(":")
                            .append(rs.getString("full_name")).append(",");
                }
            }
        } catch (Exception e) {
            dashboard.log("Error fetching friends: " + e.getMessage());
        }
        return sb.toString();
    }

    private String fetchMyChats(String myUsername) {
        StringBuilder sb = new StringBuilder();
        int myUserId = getUserId(myUsername);
        if (myUserId == -1) {
            return "";
        }

        // 🚀 THE FIX: Aggressively scrub the word "null"
        String friendsSql = "SELECT u.username, COALESCE(NULLIF(u.full_name, 'null'), NULLIF(u.full_name, ''), u.username) as clean_name FROM friendships f "
                + "JOIN users u ON (u.id = f.requester_id OR u.id = f.receiver_id) "
                + "WHERE (f.requester_id = ? OR f.receiver_id = ?) "
                + "AND u.id != ? AND f.status = 'ACCEPTED'";
        try (Connection conn = DBConnection.getConnection(); PreparedStatement ps = conn.prepareStatement(friendsSql)) {
            ps.setInt(1, myUserId);
            ps.setInt(2, myUserId);
            ps.setInt(3, myUserId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String friendUsername = rs.getString("username");
                    String fullName = rs.getString("clean_name");
                    
                    // 🚀 THE FIX: Check if this friend is currently in our PresenceManager
                    boolean isOnline = PresenceManager.getInstance().isUserOnline(friendUsername);
                    String status = isOnline ? "Active Now" : "Offline";

                    // Use ~ to separate data
                    sb.append(friendUsername).append("~")
                      .append(fullName).append("~")
                      .append("DM").append("~") // Type
                      .append(status).append(","); // Status text instead of "Online"
                }
            }
        } catch (Exception e) {
            dashboard.log("Error fetching friends: " + e.getMessage());
        }

        String groupsSql = "SELECT c.id, c.group_name FROM conversations c "
                + "JOIN chat_members cm ON c.id = cm.conversation_id "
                + "WHERE cm.user_id = ? AND c.type = 'GROUP'";
        try (Connection conn = DBConnection.getConnection(); PreparedStatement ps = conn.prepareStatement(groupsSql)) {
            ps.setInt(1, myUserId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    sb.append("GROUP_").append(rs.getInt("id")).append("~")
                            .append(rs.getString("group_name")).append("~GROUP,");
                }
            }
        } catch (Exception e) {
            dashboard.log("Error fetching groups: " + e.getMessage());
        }

        return sb.toString();
    }

    // ==============================================================
    // MESSAGING & ROUTING ENGINE
    // ==============================================================
    private void handleSendMessage(String target, String encryptedText, String senderUsername) {
        // Change this line to use AM/PM
        String time = new java.text.SimpleDateFormat("h:mm a").format(new java.util.Date());
        int senderId = getUserId(senderUsername);

        if (target.startsWith("GROUP_")) {
            // --- GROUP MESSAGE LOGIC ---
            int groupId = Integer.parseInt(target.replace("GROUP_", ""));
            saveMessageToDB(groupId, senderId, encryptedText);

            // Note: For a real group, you would SELECT all members from chat_members 
            // and loop through onlineUsers to forward the message.
            dashboard.log("Group message saved to DB for " + target);

        } else {
            // --- DIRECT MESSAGE LOGIC ---
            int targetId = getUserId(target);
            int convoId = getOrCreateDirectConversation(senderId, targetId);

            saveMessageToDB(convoId, senderId, encryptedText);

            // Instantly forward to the receiver if they are currently online!
            PrintWriter receiverOut = onlineUsers.get(target);
            if (receiverOut != null) {
                // The receiver's activeChatId is the sender's username
                receiverOut.println("NEW_MESSAGE|" + senderUsername + "|" + senderUsername + "|" + encryptedText + "|" + time);
            }
        }
    }

    private String loadChatHistory(String target, String myUsername) {
        StringBuilder sb = new StringBuilder();
        int myId = getUserId(myUsername);
        int convoId = -1;

        if (target.startsWith("GROUP_")) {
            convoId = Integer.parseInt(target.replace("GROUP_", ""));
        } else {
            int targetId = getUserId(target);
            convoId = getOrCreateDirectConversation(myId, targetId);
        }

        if (convoId == -1) {
            return "";
        }

        // 🚀 FIX: Use %l:%i %p for 12-hour AM/PM format (e.g., 11:15 PM)
        String sql = "SELECT u.username, COALESCE(NULLIF(u.full_name, 'null'), u.username) as clean_name, "
                + "m.encrypted_content, DATE_FORMAT(m.created_at, '%l:%i %p') as time_str "
                + "FROM messages m JOIN users u ON m.sender_id = u.id "
                + "WHERE m.conversation_id = ? ORDER BY m.created_at ASC";

        try (Connection conn = DBConnection.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, convoId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    // Use ~ to avoid clashing with colons in the time
                    sb.append(rs.getString("clean_name")).append("~")
                            .append(rs.getString("encrypted_content")).append("~")
                            .append(rs.getString("time_str")).append(",");
                }
            }
        } catch (Exception e) {
            dashboard.log("Error loading history: " + e.getMessage());
        }

        return sb.toString();
    }

    private void savePostToDB(String username, String content) {
    int userId = getUserId(username);
    String sql = "INSERT INTO posts (user_id, content, created_at) VALUES (?, ?, NOW())";
    
    try (Connection conn = DBConnection.getConnection();
         PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setInt(1, userId);
        ps.setString(2, content);
        ps.executeUpdate();
        dashboard.log("📝 New post created by " + username);
    } catch (Exception e) { dashboard.log("Post DB Error: " + e.getMessage()); }
}

private String fetchHomeFeed(String username) {
    StringBuilder sb = new StringBuilder();
    // This query gets posts from the user AND their friends
    String sql = "SELECT u.full_name, p.content, DATE_FORMAT(p.created_at, '%b %d, %h:%i %p') as time_str, " +
                 "(SELECT COUNT(*) FROM post_likes WHERE post_id = p.id) as likes, " +
                 "(SELECT COUNT(*) FROM post_comments WHERE post_id = p.id) as comments " +
                 "FROM posts p JOIN users u ON p.user_id = u.id " +
                 "ORDER BY p.created_at DESC LIMIT 20";

    try (Connection conn = DBConnection.getConnection();
         PreparedStatement ps = conn.prepareStatement(sql);
         ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
            // Format: Author|Content|Time|Likes|Comments~
            sb.append(rs.getString("full_name")).append("|")
              .append(rs.getString("content")).append("|")
              .append(rs.getString("time_str")).append("|")
              .append(rs.getInt("likes")).append("|")
              .append(rs.getInt("comments")).append("~");
        }
    } catch (Exception e) { dashboard.log("Feed Fetch Error: " + e.getMessage()); }
    
    return sb.toString();
}
    
    private void saveMessageToDB(int conversationId, int senderId, String encryptedText) {
        String sql = "INSERT INTO messages (conversation_id, sender_id, encrypted_content) VALUES (?, ?, ?)";
        try (Connection conn = DBConnection.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, conversationId);
            ps.setInt(2, senderId);
            ps.setString(3, encryptedText);
            ps.executeUpdate();
        } catch (Exception e) {
            dashboard.log("DB Error saving msg: " + e.getMessage());
        }
    }
    
    private boolean handleUpdateProfile(String newName, String username) {
        String sql = "UPDATE users SET full_name = ? WHERE username = ?";
        try (Connection conn = DBConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, newName);
            ps.setString(2, username);
            return ps.executeUpdate() > 0;
        } catch (Exception e) {
            dashboard.log("Update Profile Error: " + e.getMessage());
            return false;
        }
    }
    
private String fetchProfileData(String targetUser, String myUsername) {
        int targetId = getUserId(targetUser);
        String fullName = targetUser; // Fallback if they haven't set a name
        int postCount = 0;
        int friendCount = 0;
        StringBuilder postsBuilder = new StringBuilder();

        try (Connection conn = com.orbitserver.db.DBConnection.getConnection()) {
            // 1. Get Clean Full Name
            String nameSql = "SELECT COALESCE(NULLIF(full_name, 'null'), username) FROM users WHERE id = ?";
            PreparedStatement ps1 = conn.prepareStatement(nameSql);
            ps1.setInt(1, targetId);
            ResultSet rs1 = ps1.executeQuery();
            if (rs1.next()) fullName = rs1.getString(1);

            // 2. Count Friends
            String friendSql = "SELECT COUNT(*) FROM friendships WHERE (requester_id = ? OR receiver_id = ?) AND status = 'ACCEPTED'";
            PreparedStatement ps2 = conn.prepareStatement(friendSql);
            ps2.setInt(1, targetId); ps2.setInt(2, targetId);
            ResultSet rs2 = ps2.executeQuery();
            if (rs2.next()) friendCount = rs2.getInt(1);

            // 3. Get User's Posts
            String postSql = "SELECT content, DATE_FORMAT(created_at, '%b %d, %h:%i %p') as time_str FROM posts WHERE user_id = ? ORDER BY created_at DESC";
            PreparedStatement ps3 = conn.prepareStatement(postSql);
            ps3.setInt(1, targetId);
            ResultSet rs3 = ps3.executeQuery();
            while (rs3.next()) {
                postCount++;
                postsBuilder.append(rs3.getString("content")).append("^")
                            .append(rs3.getString("time_str")).append("~");
            }
        } catch (Exception e) { 
            dashboard.log("Profile DB Error: " + e.getMessage()); 
        }

        boolean isMe = targetUser.equals(myUsername);
        // Returns: targetUser | fullName | postCount | friendCount | isMe | PostData
        return targetUser + "|" + fullName + "|" + postCount + "|" + friendCount + "|" + (isMe ? "TRUE" : "FALSE") + "|" + postsBuilder.toString();
    }

    private int getOrCreateDirectConversation(int u1, int u2) {
        // Query to find existing DIRECT conversation between user 1 and user 2
        String findSql = "SELECT c.id FROM conversations c "
                + "JOIN chat_members cm1 ON c.id = cm1.conversation_id "
                + "JOIN chat_members cm2 ON c.id = cm2.conversation_id "
                + "WHERE c.type = 'DIRECT' AND cm1.user_id = ? AND cm2.user_id = ?";

        try (Connection conn = DBConnection.getConnection(); PreparedStatement ps = conn.prepareStatement(findSql)) {
            ps.setInt(1, u1);
            ps.setInt(2, u2);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("id"); // Chat room already exists!
                }
            }

            // If not found, create a brand new conversation room!
            String createSql = "INSERT INTO conversations (type) VALUES ('DIRECT')";
            PreparedStatement psInsert = conn.prepareStatement(createSql, java.sql.Statement.RETURN_GENERATED_KEYS);
            psInsert.executeUpdate();
            try (ResultSet keys = psInsert.getGeneratedKeys()) {
                if (keys.next()) {
                    int newId = keys.getInt(1);
                    // Add both members to the room
                    conn.createStatement().executeUpdate("INSERT INTO chat_members (conversation_id, user_id) VALUES (" + newId + ", " + u1 + "), (" + newId + ", " + u2 + ")");
                    return newId;
                }
            }
        } catch (Exception e) {
            dashboard.log("Error creating convo: " + e.getMessage());
        }
        return -1;
    }

}
