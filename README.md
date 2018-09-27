# Project for Distributed Systems at National Technical University of Athens, 2016
##Authors:
##Manolis Pentarakis,
##George Chatzikyriakos

In this project we developed a DHT (Distributed hash table), a simplified version of Chord, using JAVA.
The core functions implemented in this DHT are:
- initial creation of the network keys and nodes
- node insertion and departure (while the DHT is running)
- ring routing
- replication for the data stored in the DHT.


Each DHT node implements all DHT functions like  creating server and client processes, opening sockets, responding to incoming requests, Chord routing protocol etc.
For the replication we implemented 2 types of consistency : linearizability(chain replication) and eventual.

At the end of the project we run some tests and studied how the DHT works with different numbers of nodes, replication and consistencies and conducted a relevant report with our conclusions. 




