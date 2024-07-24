package store

import (
	"fmt"
	"github.com/hashicorp/raft"
)

type Node struct {
	NodeID   string
	RaftAddr string
}

// AddNode adds a new Node to raft cluster. This should be called from the leader Node
func (s *Store) AddNode(nodeID, addr string) error {
	s.logger.Printf("received add request for remote Node %s at %s", nodeID, addr)

	nodes, err := s.NodeList()
	if err != nil {
		return err
	}

	for _, node := range nodes {
		alreadyJoined := node.NodeID == nodeID && node.RaftAddr == addr
		if alreadyJoined {
			s.logger.Printf("Node %s at %s already member of cluster, ignoring add request", nodeID, addr)
			return nil
		}

		belongsToCluster := node.NodeID == nodeID || node.RaftAddr == addr
		if belongsToCluster {
			f := s.raft.RemoveServer(raft.ServerID(node.NodeID), 0, 0)
			if f.Error() != nil {
				return fmt.Errorf("error removing existing Node %s at %s", nodeID, addr)
			}
		}
	}

	f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}

	s.logger.Printf("Node %s at %s joined successfully", nodeID, addr)
	return nil
}

func (s *Store) Leader() Node {
	nodeAddr, nodeID := s.raft.LeaderWithID()
	fmt.Println(nodeID, nodeAddr)
	return Node{
		NodeID:   string(nodeID),
		RaftAddr: string(nodeAddr),
	}
}

func (s *Store) NodeList() ([]Node, error) {
	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Printf("failed to get raft configuration: %v", err)
		return nil, err
	}

	servers := configFuture.Configuration().Servers
	nodes := []Node{}
	for _, server := range servers {
		nodes = append(nodes, Node{
			NodeID:   string(server.ID),
			RaftAddr: string(server.Address),
		})
	}
	return nodes, nil
}

func (s *Store) Snapshot() error {
	f := s.raft.Snapshot()
	return f.Error()
}
