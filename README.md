## kvdb

Create executables
```
make build
``` 

Create cluster
```
./bin/kvdb -id=node1 -httpaddr=localhost:11001 -raftaddr=localhost:12001
./bin/kvdb -id=node2 -httpaddr=localhost:11002 -raftaddr=localhost:12002 -join=localhost:11001
./bin/kvdb -id=node3 -httpaddr=localhost:11003 -raftaddr=localhost:12003 -join=localhost:11001
```  

In another terminal, run the cli
```
./bin/cli
``` 

From the cli
Set a key (only from leader node)
```
kv set k1=v1 addr=localhost:11001
``` 

Get list of keys (from any node)
```
kv list keys addr=localhost:11001
kv list keys addr=localhost:11002
kv list keys addr=localhost:11003
``` 

Get value for a key (from any node)
```
kv get k1 addr=localhost:11001
kv get k1 addr=localhost:11002
kv get k1 addr=localhost:11003
``` 

Delete a key (only from leader node)
```
kv delete k1 addr=localhost:11001
``` 

Exit CLI
```
exit
```

