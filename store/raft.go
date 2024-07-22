package store

import (
	"fmt"
	"github.com/hashicorp/raft"
)

// AddNode adds a new node to raft cluster. This should be called from the leader node
func (s *Store) AddNode(nodeID, addr string) error {
	s.logger.Printf("received add request for remote node %s at %s", nodeID, addr)

	nodes, err := s.NodeList()
	if err != nil {
		return err
	}

	for _, node := range nodes {
		alreadyJoined := node.ID == raft.ServerID(nodeID) && node.Address == raft.ServerAddress(addr)
		if alreadyJoined {
			s.logger.Printf("node %s at %s already member of cluster, ignoring add request", nodeID, addr)
			return nil
		}

		belongsToCluster := node.ID == raft.ServerID(nodeID) || node.Address == raft.ServerAddress(addr)
		if belongsToCluster {
			f := s.raft.RemoveServer(node.ID, 0, 0)
			if f.Error() != nil {
				return fmt.Errorf("error removing existing node %s at %s", nodeID, addr)
			}
		}
	}

	f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}

	s.logger.Printf("node %s at %s joined successfully", nodeID, addr)
	return nil
}

func (s *Store) Leader() raft.Server {
	nodeAddr, nodeID := s.raft.LeaderWithID()
	return raft.Server{
		ID:      nodeID,
		Address: nodeAddr,
	}
}

func (s *Store) NodeList() ([]raft.Server, error) {
	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Printf("failed to get raft configuration: %v", err)
		return nil, err
	}

	return configFuture.Configuration().Servers, nil
}

func (s *Store) Snapshot() error {
	f := s.raft.Snapshot()
	return f.Error()
}
