package com.milesoldenburg.jchord.chord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

public class ChordNode {

    private String address;
    private int port;
    private String existingNodeAddress = null;
    private int existingNodePort;
    private Finger secondPredecessor;
    private Finger firstPredecessor;
    private Finger firstSuccessor;
    private Finger secondSuccessor;
    private Map<Integer, Finger> fingers = new HashMap<>();
    private long id;
    private String hex;
    private Semaphore semaphore = new Semaphore(1);

    /**
     * Constructor for creating a new Chord node that is the first in the ring.
     *
     * @param address   The address of this node
     * @param port      The port that this Chord node needs to listen on
     */
    public ChordNode(String address, String port) {
        // Set node fields
        this.address = address;
        this.port = Integer.valueOf(port);

        // Hash address
        SHA1Hasher sha1Hasher = new SHA1Hasher(this.address + ":" + this.port);
        this.id = sha1Hasher.getLong();
        this.hex = sha1Hasher.getHex();

        // Logging
        System.out.println("Creating a new Chord ring");
        System.out.println("You are listening on port " + this.port);
        System.out.println("Your position is " + this.hex + " (" + this.id + ")");

        // Initialize finger table and successors
        this.initializeFingers();
        this.initializeSuccessors();

        // Start listening for connections and heartbeats from neighbors
        new Thread(new NodeListener(this)).start();
        new Thread(new NodeStabilizer(this)).start();
        new Thread(new Heart(this)).start();
    }

    /**
     * Constructor for creating a new Chord node that will join an existing ring.
     *
     * @param address               The address of this node
     * @param port                  The port that this Chord node needs to listen on
     * @param existingNodeAddress   The address of the existing ring member
     * @param existingNodePort      The port of the existing ring member
     */
    public ChordNode(String address, String port, String existingNodeAddress, String existingNodePort) {
        // Set node fields
        this.address = address;
        this.port = Integer.valueOf(port);

        // Set contact node fields
        this.existingNodeAddress = existingNodeAddress;
        this.existingNodePort = Integer.valueOf(existingNodePort);

        // Hash address
        SHA1Hasher sha1Hasher = new SHA1Hasher(this.address + ":" + this.port);
        this.id = sha1Hasher.getLong();
        this.hex = sha1Hasher.getHex();

        // Logging
        System.out.println("Joining the Chord ring");
        System.out.println("You are listening on port " + this.port);
        System.out.println("Connected to existing node " + this.existingNodeAddress + ":" + this.existingNodePort);
        System.out.println("Your position is " + this.hex + " (" + this.id + ")");

        // Initialize finger table and successors
        this.initializeFingers();
        this.initializeSuccessors();

        // Start listening for connections and heartbeats from neighbors
        new Thread(new NodeListener(this)).start();
        new Thread(new NodeStabilizer(this)).start();
        new Thread(new Heart(this)).start();
    }

    /**
     * Initializes finger table. If an existing node has been defined it will use that node to perform lookups. Otherwise, this node is the only node in the ring and all fingers will refer to self.
     */
    private void initializeFingers() {
        // If this ring is the only node in the ring
        if (this.existingNodeAddress == null) {
            // Initialize all fingers to refer to self
            for (int i = 0; i < 32; i++) {
                this.fingers.put(i, new Finger(this.address, this.port));
            }
        } else {
            // Open connection to contact node
            try {
                Socket socket = new Socket(this.existingNodeAddress, this.existingNodePort);

                // Open reader/writer to chord node
                PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                BigInteger bigQuery = BigInteger.valueOf(2L);
                BigInteger bigSelfId = BigInteger.valueOf(this.id);

                for (int i = 0; i < 32; i++) {
                    BigInteger bigResult = bigQuery.pow(i);
                    bigResult = bigResult.add(bigSelfId);

                    // Send query to chord
                    socketWriter.println(Chord.FIND_NODE + ":" + bigResult.longValue());
                    System.out.println("Sent: " + Chord.FIND_NODE + ":" + bigResult.longValue());

                    // Read response from chord
                    String serverResponse = socketReader.readLine();

                    // Parse out address and port
                    String[] serverResponseFragments = serverResponse.split(":", 2);
                    String[] addressFragments = serverResponseFragments[1].split(":");

                    // Add response finger to table
                    this.fingers.put(i, new Finger(addressFragments[0], Integer.valueOf(addressFragments[1])));

                    System.out.println("Received: " + serverResponse);
                }

                // Close connections
                socketWriter.close();
                socketReader.close();
                socket.close();
            } catch (IOException e) {
                this.logError("Could not open connection to existing node");
                e.printStackTrace();
            }
        }
    }

    /**
     * Initializes successors. Uses the finger table to get the successors and defaults the predecessors to self until it learns about new ones.
     */
    private void initializeSuccessors() {
        this.firstSuccessor = this.fingers.get(0);
        this.secondSuccessor = this.fingers.get(1);
        this.firstPredecessor = new Finger(this.address, this.port);
        this.secondPredecessor = new Finger(this.address, this.port);

        // Notify the first successor that we are the new predecessor, provided we do not open a connection to ourselves
        if (!this.address.equals(this.firstSuccessor.getAddress()) || (this.port != this.firstSuccessor.getPort())) {
            try {
                Socket socket = new Socket(this.firstSuccessor.getAddress(), this.firstSuccessor.getPort());

                // Open writer to successor node
                PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);

                // Tell successor that this node is its new predecessor
                socketWriter.println(Chord.NEW_PREDECESSOR + ":" + this.getAddress() + ":" + this.getPort());
                System.out.println("Sent: " + Chord.NEW_PREDECESSOR + ":" + this.getAddress() + ":" + this.getPort() + " to " + this.firstSuccessor.getAddress() + ":" + this.firstSuccessor.getPort());

                // Close connections
                socketWriter.close();
                socket.close();
            } catch (IOException e) {
                this.logError("Could not open connection to first successor");
                e.printStackTrace();
            }
        }
    }

    /**
     * Logs error messages to the console
     *
     * @param errorMessage  The message to print to the console
     */
    private void logError(String errorMessage) {
        System.err.println("Error (" + this.id + "): " + errorMessage);
    }

    public void acquire() {
        try {
            this.semaphore.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void release() {
        this.semaphore.release();
    }

    public Map<Integer, Finger> getFingers() {
        return this.fingers;
    }

    public int getPort() {
        return this.port;
    }

    public String getAddress() {
        return this.address;
    }

    public Finger getFirstSuccessor() {
        return this.firstSuccessor;
    }

    public void setFirstSuccessor(Finger firstSuccessor) {
        this.firstSuccessor = firstSuccessor;
    }

    public Finger getFirstPredecessor() {
        return this.firstPredecessor;
    }

    public void setFirstPredecessor(Finger firstPredecessor) {
        this.firstPredecessor = firstPredecessor;
    }

    public Finger getSecondSuccessor() {
        return secondSuccessor;
    }

    public void setSecondSuccessor(Finger secondSuccessor) {
        this.secondSuccessor = secondSuccessor;
    }

    public Finger getSecondPredecessor() {
        return secondPredecessor;
    }

    public void setSecondPredecessor(Finger secondPredecessor) {
        this.secondPredecessor = secondPredecessor;
    }

    public long getId() {
        return this.id;
    }

    public Semaphore getSemaphore() {
        return semaphore;
    }

}
