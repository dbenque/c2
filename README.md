Coordinator
===========

Coordinator discover themselves thanks to the endpoint registry were they register when they start.
This endpoint registry could be the load balancer or another distributed storage.
It must provide an API to Add an enpoint, Remove an endpoint, and List the endpoints

Since they know each other and constantly refresh the registry, if one coordinator fails, the other notice it and remove its entry from the list of endpoints. Note that the last one surviving may stay in the registry if it stops abnormally. (except if the registry allow TTLs, the list of entry point will in that case self destroy)

Not implemented yet:
- When a task is given to one coordinator in the cluster, this coordinator retrieve the real coordinator to target (if not himself) and forward the task.
- Tasks could be handled by the cluster under a task tree. Each node of the tree is a simple task the is associated to one and only one coordinator.
- A task tree branch (or the complete tree) will be interrupted if one of the task is associated to a missing/failing coordinator.
- The task tree and its state are persisted in a shared storage (couchbase, ETCD ...) known by all coordinators.

Tips
====

Building myscratch docker image:
tar cv --files-from /dev/null | docker import - myscratch

Static build:
CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o c2 .
