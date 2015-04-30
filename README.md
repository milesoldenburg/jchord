#jchord

A Chord DHT implementation in Java

##Requirements
1. JDK 1.7+
2. Maven

##Compiling

	maven clean install
	
##Running Chord
All commands should be run from the root directory.

Create a new ring on the specified port

	java -jar chord/target/jchord-chord-1.0-SNAPSHOT.jar [port]
	
Create a new node and bootstrap it to an existing ring
	
	java -jar chord/target/jchord-chord-1.0-SNAPSHOT.jar [port] [nodeaddress] [nodeport]
	
This example creates a new ring consisting of one node on port 8001 then creates another node on port 8002 and connects it to the initial rode to form a ring of 2 nodes

	java -jar chord/target/jchord-chord-1.0-SNAPSHOT.jar 8001
	java -jar chord/target/jchord-chord-1.0-SNAPSHOT.jar 8002 127.0.0.1 8001
	
##Running Query
The query program creates a connection to a chord ring and can be used to search for values

	java -jar query/target/jchord-query-1.0-SNAPSHOT-jar-with-dependencies.jar [address] [port]
	
Continuing the above example this opens a connection to the Chord ring by specifying the entry point on port 8001. If following the above example the entry point could also be on port 8002. The entry point can be any node in a Chord ring.

	java -jar query/target/jchord-query-1.0-SNAPSHOT-jar-with-dependencies.jar 127.0.0.1 8001