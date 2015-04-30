package com.milesoldenburg.jchord.chord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.Socket;
import java.net.UnknownHostException;

public class NodeStabilizer extends Thread {

    private ChordNode chordNode;
    private int delaySeconds = 10;

    public NodeStabilizer(ChordNode chordNode) {
        this.chordNode = chordNode;
    }

    /**
     * Method that periodically runs to determine if node needs a new successor by contacting the listed successor and asking for its predecessor. If the current successor has a predecessor that is different than itself it sets its successor to the predecessor.
     */
    public void run() {
        try {
            // Initially sleep
            Thread.sleep(this.delaySeconds * 1000);

            Socket socket = null;
            PrintWriter socketWriter = null;
            BufferedReader socketReader = null;

            while (true) {
                // Only open a connection to the successor if it is not ourselves
                if (!this.chordNode.getAddress().equals(this.chordNode.getFirstSuccessor().getAddress()) || (this.chordNode.getPort() != this.chordNode.getFirstSuccessor().getPort())) {
                    // Open socket to successor
                    socket = new Socket(this.chordNode.getFirstSuccessor().getAddress(), this.chordNode.getFirstSuccessor().getPort());

                    // Open reader/writer to chord node
                    socketWriter = new PrintWriter(socket.getOutputStream(), true);
                    socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                    // Submit a request for the predecessor
                    socketWriter.println(Chord.REQUEST_PREDECESSOR + ":" + this.chordNode.getId() + " asking " + this.chordNode.getFirstSuccessor().getId());
                    System.out.println("Sent: " + Chord.REQUEST_PREDECESSOR + ":" + this.chordNode.getId() + " asking " + this.chordNode.getFirstSuccessor().getId());

                    // Read response from chord
                    String serverResponse = socketReader.readLine();
                    System.out.println("Received: " + serverResponse);

                    // Parse server response for address and port
                    String[] predecessorFragments = serverResponse.split(":");
                    String predecessorAddress = predecessorFragments[0];
                    int predecessorPort = Integer.valueOf(predecessorFragments[1]);

                    // If the address:port that was returned from the server is not ourselves then we need to adopt it as our new successor
                    if (!this.chordNode.getAddress().equals(predecessorAddress) || (this.chordNode.getPort() != predecessorPort)) {
                        this.chordNode.acquire();

                        Finger newSuccessor = new Finger(predecessorAddress, predecessorPort);

                        // Update finger table entries to reflect new successor
                        this.chordNode.getFingers().put(1, this.chordNode.getFingers().get(0));
                        this.chordNode.getFingers().put(0, newSuccessor);

                        // Update successor entries to reflect new successor
                        this.chordNode.setSecondSuccessor(this.chordNode.getFirstSuccessor());
                        this.chordNode.setFirstSuccessor(newSuccessor);

                        this.chordNode.release();

                        // Close connections
                        socketWriter.close();
                        socketReader.close();
                        socket.close();

                        // Inform new successor that we are now their predecessor
                        socket = new Socket(newSuccessor.getAddress(), newSuccessor.getPort());

                        // Open writer/reader to new successor node
                        socketWriter = new PrintWriter(socket.getOutputStream(), true);
                        socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                        // Tell successor that this node is its new predecessor
                        socketWriter.println(Chord.NEW_PREDECESSOR + ":" + this.chordNode.getAddress() + ":" + this.chordNode.getPort());
                        System.out.println("Sent: " + Chord.NEW_PREDECESSOR + ":" + this.chordNode.getAddress() + ":" + this.chordNode.getPort());
                    }

                    BigInteger bigQuery = BigInteger.valueOf(2L);
                    BigInteger bigSelfId = BigInteger.valueOf(this.chordNode.getId());

                    this.chordNode.acquire();

                    // Refresh all fingers by asking successor for nodes
                    for (int i = 0; i < 32; i++) {
                        BigInteger bigResult = bigQuery.pow(i);
                        bigResult = bigResult.add(bigSelfId);

                        // Send query to chord
                        socketWriter.println(Chord.FIND_NODE + ":" + bigResult.longValue());
                        System.out.println("Sent: " + Chord.FIND_NODE + ":" + bigResult.longValue());

                        // Read response from chord
                        serverResponse = socketReader.readLine();

                        // Parse out address and port
                        String[] serverResponseFragments = serverResponse.split(":", 2);
                        String[] addressFragments = serverResponseFragments[1].split(":");

                        // Add response finger to table
                        this.chordNode.getFingers().put(i, new Finger(addressFragments[0], Integer.valueOf(addressFragments[1])));
                        this.chordNode.setFirstSuccessor(this.chordNode.getFingers().get(0));
                        this.chordNode.setSecondSuccessor(this.chordNode.getFingers().get(1));

                        System.out.println("Received: " + serverResponse);
                    }

                    this.chordNode.release();

                    // Close connections
                    socketWriter.close();
                    socketReader.close();
                    socket.close();
                } else if (!this.chordNode.getAddress().equals(this.chordNode.getFirstPredecessor().getAddress()) || (this.chordNode.getPort() != this.chordNode.getFirstPredecessor().getPort())) {
                    // Open socket to successor
                    socket = new Socket(this.chordNode.getFirstPredecessor().getAddress(), this.chordNode.getFirstPredecessor().getPort());

                    // Open reader/writer to chord node
                    socketWriter = new PrintWriter(socket.getOutputStream(), true);
                    socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                    BigInteger bigQuery = BigInteger.valueOf(2L);
                    BigInteger bigSelfId = BigInteger.valueOf(this.chordNode.getId());

                    this.chordNode.acquire();

                    // Refresh all fingers by asking successor for nodes
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
                        this.chordNode.getFingers().put(i, new Finger(addressFragments[0], Integer.valueOf(addressFragments[1])));
                        this.chordNode.setFirstSuccessor(this.chordNode.getFingers().get(0));
                        this.chordNode.setSecondSuccessor(this.chordNode.getFingers().get(1));

                        System.out.println("Received: " + serverResponse);
                    }

                    this.chordNode.release();

                    // Close connections
                    socketWriter.close();
                    socketReader.close();
                    socket.close();
                }

                // Stabilize again after delay
                Thread.sleep(this.delaySeconds * 1000);
            }
        } catch (InterruptedException e) {
            System.err.println("stabilize() thread interrupted");
            e.printStackTrace();
        } catch (UnknownHostException e) {
            System.err.println("stabilize() could not find host of first successor");
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("stabilize() could not connect to first successor");
            e.printStackTrace();
        }
    }

}
