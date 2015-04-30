package com.milesoldenburg.jchord.chord;

public class Chord {

    public final static String  NEW_PREDECESSOR     = "NEW_PREDECESSOR";
    public final static String  FIND_VALUE          = "FIND_VALUE";
    public final static String  FIND_NODE           = "FIND_NODE";
    public final static String  NODE_FOUND          = "NODE_FOUND";
    public final static String  REQUEST_PREDECESSOR = "REQUEST_PREDECESSOR";
    public final static String  PING_QUERY          = "IS THERE ANYONE OUT THERE?";
    public final static String  PING_RESPONSE       = "REUNITED";
    public final static long    RING_SIZE           = 4294967296L;

    public static void main(String[] args) {
        // Check arguments
        if (args.length == 1) {
            // Create new node
            new ChordNode("127.0.0.1", args[0]);
        } else if (args.length == 3) {
            // Create new node
            new ChordNode("127.0.0.1", args[0], args[1], args[2]);
        } else {
            System.err.println("Usage: chord [port] || chord [port] [nodeaddress] [nodeport]");
            System.exit(1);
        }
    }

}
