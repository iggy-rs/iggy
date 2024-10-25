
// So this one is quite tricky,
// We define a trait `Consesus`
// Our consesus is going to be split between metadata consesus (stream / topic / partition / cluster managment ops)
// And messages consesus ( user data messages ).
//
//
// Multiple reasons for this.
// First of all, we can use the redundancy from the cluster also as a load balancing mechanism.
// Utilize other machines as primary for stream / topic / partition
// And distribute the traffic from our clients.
// Secondly we need different terminology for metadata and messages
// For example `op_number` is proposed by the VRR paper to determine ordering of operations.
// This works fine for metadata, but for messages we already have `offset` that serves exactly that purpose.
