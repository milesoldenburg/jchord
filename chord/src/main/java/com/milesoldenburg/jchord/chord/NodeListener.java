package com.milesoldenburg.jchord.chord;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class NodeListener implements Runnable {

    private ChordNode chordNode;

    public NodeListener(ChordNode chordNode) {
        this.chordNode = chordNode;
    }

    public void run() {
        try {
            // Listen for connections on port
            ServerSocket serverSocket = new ServerSocket(this.chordNode.getPort());

            // Continuously loop for connections
            while (true) {
                // When connection is established launch a new thread for communicating with client
                Socket clientSocket = serverSocket.accept();
                new Thread(new ChordThread(this.chordNode, clientSocket)).start();
            }
        } catch (IOException e) {
            System.err.println("error when listening for connections");
            e.printStackTrace();
        }
    }

}
