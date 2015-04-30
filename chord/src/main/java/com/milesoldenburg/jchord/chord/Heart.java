package com.milesoldenburg.jchord.chord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class Heart implements Runnable {

    private ChordNode chordNode = null;
    private final static int initialDelaySeconds = 30;
    private final static int delaySeconds = 5;

    public Heart(ChordNode chordNode) {
        this.chordNode = chordNode;
    }

    /**
     * Sends heartbeats out to neighbors and updates pointers as necessary
     */
    public void run() {
        try {
            Thread.sleep(Heart.initialDelaySeconds * 1000);

            while (true) {
                this.testSuccessor();
                this.testPredecessor();

                Thread.sleep(Heart.delaySeconds * 1000);
            }
        } catch (InterruptedException e) {
            System.err.println("checkNeighbors() thread interrupted");
            e.printStackTrace();
        }
    }

    private void testSuccessor() {
        // Only send heartbeats if we are not the destination
        if (!this.chordNode.getAddress().equals(this.chordNode.getFirstSuccessor().getAddress()) || (this.chordNode.getPort() != this.chordNode.getFirstSuccessor().getPort())) {
            try {
                // Open socket to successor
                Socket socket = new Socket(this.chordNode.getFirstSuccessor().getAddress(), this.chordNode.getFirstSuccessor().getPort());

                // Open reader/writer to chord node
                PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                // Send a ping
                socketWriter.println(Chord.PING_QUERY + ":" + this.chordNode.getId());
                System.out.println("Sent: " + Chord.PING_QUERY + ":" + this.chordNode.getId());

                // Read response
                String serverResponse = socketReader.readLine();
                System.out.println("Received: " + serverResponse);

                // If we do not receive the proper response then something has gone wrong and we need to set our new immediate successor to the backup
                if (!serverResponse.equals(Chord.PING_RESPONSE)) {
                    this.chordNode.acquire();
                    this.chordNode.setFirstSuccessor(this.chordNode.getSecondSuccessor());
                    this.chordNode.getFingers().put(0, this.chordNode.getSecondSuccessor());
                    this.chordNode.release();
                }

                // Close connections
                socketWriter.close();
                socketReader.close();
                socket.close();
            } catch (IOException e) {
                this.chordNode.acquire();
                this.chordNode.setFirstSuccessor(this.chordNode.getSecondSuccessor());
                this.chordNode.getFingers().put(0, this.chordNode.getSecondSuccessor());
                this.chordNode.release();
            }
        }
    }

    private void testPredecessor() {
        // Only send heartbeats if we are not the destination
        if (!this.chordNode.getAddress().equals(this.chordNode.getFirstPredecessor().getAddress()) || (this.chordNode.getPort() != this.chordNode.getFirstPredecessor().getPort())) {
            try {
                // Open socket to predecessor
                Socket socket = new Socket(this.chordNode.getFirstPredecessor().getAddress(), this.chordNode.getFirstPredecessor().getPort());

                // Open reader/writer to chord node
                PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                // Send a ping
                socketWriter.println(Chord.PING_QUERY + ":" + this.chordNode.getId());
                System.out.println("Sent: " + Chord.PING_QUERY + ":" + this.chordNode.getId());

                // Read response
                String serverResponse = socketReader.readLine();
                System.out.println("Received: " + serverResponse);

                // If we do not receive the proper response then something has gone wrong and we need to set our new immediate predecessor to the backup
                if (!serverResponse.equals(Chord.PING_RESPONSE)) {
                    this.chordNode.acquire();
                    this.chordNode.setFirstPredecessor(this.chordNode.getSecondPredecessor());
                    this.chordNode.release();
                }

                // Close connections
                socketWriter.close();
                socketReader.close();
                socket.close();
            } catch (IOException e) {
                this.chordNode.acquire();
                this.chordNode.setFirstPredecessor(this.chordNode.getSecondPredecessor());
                this.chordNode.release();
            }
        }
    }

}
