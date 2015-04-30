package com.milesoldenburg.jchord.query;

public class Query {

    public static void main(String[] args) {
        // Check arguments
        if (args.length == 2) {
            // Create query node with chord node address and port
            new QueryNode(args[0], args[1]);
        } else {
            System.err.println("Usage: query [nodeaddress] [nodeport]");
            System.exit(1);
        }
    }

}
