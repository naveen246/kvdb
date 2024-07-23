# kvdb

# Create executable
make build

# Create cluster
./bin/kvdb -id=node1 -httpaddr=localhost:11001 -raftaddr=localhost:12001

./bin/kvdb -id=node2 -httpaddr=localhost:11002 -raftaddr=localhost:12002 -join=localhost:11001

./bin/kvdb -id=node3 -httpaddr=localhost:11003 -raftaddr=localhost:12003 -join=localhost:11001

# In another terminal, input curl commands
# Add a key
curl -X POST localhost:11001/keys -d '{"abc":"123"}'

# Get list of keys
curl localhost:11001/keys

# Get value for a key
curl localhost:11001/keys/abc

# Delete key
curl -X DELETE localhost:11001/keys/abc

