package com.milesoldenburg.jchord.chord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ChordThread implements Runnable {

    private ChordNode chordNode;
    private Socket socket = null;

    public ChordThread(ChordNode chordNode, Socket socket) {
        this.chordNode = chordNode;
        this.socket = socket;
    }

    /**
     * Method that will read/send messages. It should attempt to read PING/STORE/FIND_NODE/FIND_VALUE messages
     */
    public void run() {
        System.out.println("Client connection established on port " + this.socket.getLocalPort());

        try {
            // Create readers and writers from socket
            PrintWriter socketWriter = new PrintWriter(this.socket.getOutputStream(), true);
            BufferedReader socketReader = new BufferedReader(new InputStreamReader(this.socket.getInputStream()));

            // Read input from client
            String query;
            while ((query = socketReader.readLine()) != null) {
                // Split the query on the : token in order to get the command and the content portions
                String[] queryContents = query.split(":", 2);
                String command = queryContents[0];
                String content = queryContents[1];

                System.out.println("Received: " + command + " " + content);

                switch (command) {
                    case Chord.FIND_VALUE: {
                        String response = this.findValue(content);
                        System.out.println("Sent: " + response);

                        // Send response back to client
                        socketWriter.println(response);

                        break;
                    }
                    case Chord.FIND_NODE: {
                        String response = this.findNode(content);
                        System.out.println("Sent: " + response);

                        // Send response back to client
                        socketWriter.println(response);

                        break;
                    }
                    case Chord.NEW_PREDECESSOR: {
                        // Parse address and port from message
                        String[] contentFragments = content.split(":");
                        String address = contentFragments[0];
                        int port = Integer.valueOf(contentFragments[1]);

                        // Acquire lock
                        this.chordNode.acquire();

                        // Move fist predecessor to second
                        this.chordNode.setSecondPredecessor(this.chordNode.getFirstPredecessor());

                        // Set first predecessor to new finger received in message
                        this.chordNode.setFirstPredecessor(new Finger(address, port));

                        // Release lock
                        this.chordNode.release();

                        break;
                    }
                    case Chord.REQUEST_PREDECESSOR: {
                        // Return the first predecessor address:port
                        String response = this.chordNode.getFirstPredecessor().getAddress() + ":" + this.chordNode.getFirstPredecessor().getPort();
                        System.out.println("Sent: " + response);

                        // Send response back to client
                        socketWriter.println(response);

                        break;
                    }
                    case Chord.PING_QUERY: {
                        // Reply to the ping
                        String response = Chord.PING_RESPONSE;
                        System.out.println("Sent: " + response);

                        // Send response back to client
                        socketWriter.println(response);

                        break;
                    }
                }
            }

            // Close connections
            socketWriter.close();
            socketReader.close();
            this.socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Client connection terminated on port " + this.socket.getLocalPort());
    }

    private String findValue(String query) {
        // Get long of query
        SHA1Hasher queryHasher = new SHA1Hasher(query);
        long queryId = queryHasher.getLong();

        // Wrap the queryid if it is as big as the ring
        if (queryId >= Chord.RING_SIZE) {
            queryId -= Chord.RING_SIZE;
        }

        String response = "Not found.";

        // If the query is greater than our predecessor id and less than equal to our id then we have the value
        if (this.doesQueryIdBelongToCurrentNode(queryId)) {
            response = "VALUE_FOUND:Request acknowledged on node " + this.chordNode.getAddress() + ":" + this.chordNode.getPort();
        } else if (this.doesQueryIdBelongToNextNode(queryId)) {
            response = "VALUE_FOUND:Request acknowledged on node " + this.chordNode.getFirstSuccessor().getAddress() + ":" + this.chordNode.getFirstSuccessor().getPort();
        } else { // We don't have the query so we must search our fingers for it
            long minimumDistance = Chord.RING_SIZE;
            Finger closestPredecessor = null;

            this.chordNode.acquire();

            // Look for a node identifier in the finger table that is less than the key id and closest in the ID space to the key id
            for (Finger finger : this.chordNode.getFingers().values()) {
                long distance;

                // Find clockwise distance from finger to query
                if (queryId >= finger.getId()) {
                    distance = queryId - finger.getId();
                } else {
                    distance = queryId + Chord.RING_SIZE - finger.getId();
                }

                // If the distance we have found is smaller than the current minimum, replace the current minimum
                if (distance < minimumDistance) {
                    minimumDistance = distance;
                    closestPredecessor = finger;
                }
            }

            System.out.println("queryid: " + queryId + " minimum distance: " + minimumDistance + " on " + closestPredecessor.getAddress() + ":" + closestPredecessor.getPort());

            try {
                // Open socket to chord node
                Socket socket = new Socket(closestPredecessor.getAddress(), closestPredecessor.getPort());

                // Open reader/writer to chord node
                PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                // Send query to chord
                socketWriter.println(Chord.FIND_VALUE + ":" + query);
                System.out.println("Sent: " + Chord.FIND_VALUE + ":" + query);

                // Read response from chord
                String serverResponse = socketReader.readLine();
                System.out.println("Response from node " + closestPredecessor.getAddress() + ", port " + closestPredecessor.getPort() + ", position " + " (" + closestPredecessor.getId() + "):");

                response = serverResponse;

                // Close connections
                socketWriter.close();
                socketReader.close();
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            this.chordNode.release();
        }

        return response;
    }

    private String findNode(String query) {
        long queryId = Long.valueOf(query);

        // Wrap the queryid if it is as big as the ring
        if (queryId >= Chord.RING_SIZE) {
            queryId -= Chord.RING_SIZE;
        }

        String response = "Not found.";

        // If the query is greater than our predecessor id and less than equal to our id then we have the value
        if (this.doesQueryIdBelongToCurrentNode(queryId)) {
            response = Chord.NODE_FOUND + ":" +  this.chordNode.getAddress() + ":" + this.chordNode.getPort();
        } else if(this.doesQueryIdBelongToNextNode(queryId)) {
            response = Chord.NODE_FOUND + ":" +  this.chordNode.getFirstSuccessor().getAddress() + ":" + this.chordNode.getFirstSuccessor().getPort();
        } else { // We don't have the query so we must search our fingers for it
            long minimumDistance = Chord.RING_SIZE;
            Finger closestPredecessor = null;

            this.chordNode.acquire();

            // Look for a node identifier in the finger table that is less than the key id and closest in the ID space to the key id
            for (Finger finger : this.chordNode.getFingers().values()) {
                long distance;

                // Find clockwise distance from finger to query
                if (queryId >= finger.getId()) {
                    distance = queryId - finger.getId();
                } else {
                    distance = queryId + Chord.RING_SIZE - finger.getId();
                }

                // If the distance we have found is smaller than the current minimum, replace the current minimum
                if (distance < minimumDistance) {
                    minimumDistance = distance;
                    closestPredecessor = finger;
                }
            }

            System.out.println("queryid: " + queryId + " minimum distance: " + minimumDistance + " on " + closestPredecessor.getAddress() + ":" + closestPredecessor.getPort());

            try {
                // Open socket to chord node
                Socket socket = new Socket(closestPredecessor.getAddress(), closestPredecessor.getPort());

                // Open reader/writer to chord node
                PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                // Send query to chord
                socketWriter.println(Chord.FIND_NODE + ":" + queryId);
                System.out.println("Sent: " + Chord.FIND_NODE + ":" + queryId);

                // Read response from chord
                String serverResponse = socketReader.readLine();
                System.out.println("Response from node " + closestPredecessor.getAddress() + ", port " + closestPredecessor.getPort() + ", position " + " (" + closestPredecessor.getId() + "):");

                response = serverResponse;

                // Close connections
                socketWriter.close();
                socketReader.close();
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            this.chordNode.release();
        }

        return response;
    }

    private boolean doesQueryIdBelongToCurrentNode(long queryId) {
        boolean response = false;

        // If we are working in a nice clockwise direction without wrapping
        if (this.chordNode.getId() > this.chordNode.getFirstPredecessor().getId()) {
            // If the query id is between our predecessor and us, the query belongs to us
            if ((queryId > this.chordNode.getFirstPredecessor().getId()) && (queryId <= this.chordNode.getId())) {
                response = true;
            }
        } else { // If we are wrapping
            if ((queryId > this.chordNode.getFirstPredecessor().getId()) || (queryId <= this.chordNode.getId())) {
                response = true;
            }
        }

        return response;
    }

    private boolean doesQueryIdBelongToNextNode(long queryId) {
        boolean response = false;

        // If we are working in a nice clockwise direction without wrapping
        if (this.chordNode.getId() < this.chordNode.getFirstSuccessor().getId()) {
            // If the query id is between our successor and us, the query belongs to our successor
            if ((queryId > this.chordNode.getId()) && (queryId <= this.chordNode.getFirstSuccessor().getId())) {
                response = true;
            }
        } else { // If we are wrapping
            if ((queryId > this.chordNode.getId()) || (queryId <= this.chordNode.getFirstSuccessor().getId())) {
                response = true;
            }
        }

        return response;
    }

}
