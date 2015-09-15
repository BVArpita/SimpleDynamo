                                                 SimpleDynamo
 
This application is a simplified version of Dynamo. There are three components to this program:
1. Partitioning
2. Replication
3. Failure Handling

The main goal is to provide both availibility and linearizability at the same time i.e., read-write operations will happen
successfully even under node failures. At the same time, read operations will always return the most recent value. Partitioning and 
replication are taken care of exactly like Dynamo.

There is support for insert/delete/query operations. It also handles * (return all the key-value pairs in the system)and @ 
(returns all key-value pairs of a certain node)query operaions. This system can handle one temporary failure at a time. It also 
handles failures occurring at the same time as read-write operations.

This simpleDynamo does not handle Virtual nodes and Hinted Handoff like Amazon's Dynamo. Based on Amazon's Dynamo, following
features have been implemented in SimpleDynamo:

1. Membership : Evry node knows about every other node. It means each node knows to which partition it belngs and also knows
about the partition of other nodes.
2.  Request Routing : If there are no failed nodes, request is sent directly to the node responsible as every node knows about
every other node.
3.  Chain Replication : This replication strategy provides linearizability. In chain replication, a write operation
always comes to the first partition; then it propagates to the next two partitions in sequence. The last partition returns 
the result of the write. A read operation always comes to the last partition and reads the value from the last partition.
4.Failure Handling :  Just as the original Dynamo, each request is used to detect a node failure. For this purpose, 
a timeout for a socket read is used; and if a node does not respond within the timeout, it is considered as a failed node.
When a coordinator for a request fails and it does not respond to the request, its successor is contacted next for the request.




