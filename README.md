## kvdb
kvdb is a distributed key-value store that uses raft for data replication and boltdb for raft log storage

Create executables `kvdb` and `cli` under the folder bin
``` 
make build
``` 

Create raft cluster.
```shell
./bin/kvdb -id=node1 -httpaddr=localhost:11001 -raftaddr=localhost:12001

./bin/kvdb -id=node2 -httpaddr=localhost:11002 -raftaddr=localhost:12002 -join=localhost:11001

./bin/kvdb -id=node3 -httpaddr=localhost:11003 -raftaddr=localhost:12003 -join=localhost:11001
```  

In another terminal, run the cli
```
./bin/cli
``` 

### From the cli

Find leader node (from any node)
```shell
raft leader addr=localhost:11001
# result: {"NodeID":"node1","RaftAddr":"127.0.0.1:12001"}
```

Get raft servers (from any node)
```shell
raft servers addr=localhost:11001
# result: [{"NodeID":"node1","RaftAddr":"127.0.0.1:12001"},{"NodeID":"node2","RaftAddr":"localhost:12002"},{"NodeID":"node3","RaftAddr":"localhost:12003"}]
```

Set a key (only from leader node)
```shell
kv set k1=v1 addr=localhost:11001
# result: {"k1":"v1"}
``` 

Get list of keys (from any node)
```shell
kv list keys addr=localhost:11001
# result: ["k1"]

kv list keys addr=localhost:11002
# result: ["k1"]

kv list keys addr=localhost:11003
# result: ["k1"]
``` 

Get value for a key (from any node)
```shell
kv get k1 addr=localhost:11001
# result: {"k1":"v1"}

kv get k1 addr=localhost:11002
# result: {"k1":"v1"}

kv get k1 addr=localhost:11003
# result: {"k1":"v1"}
``` 

Delete a key (only from leader node)
```shell
kv delete k1 addr=localhost:11001
# result: k1
``` 

Exit CLI
```shell
exit
```

