package store

import (
	"fmt"
	"github.com/hashicorp/raft"
)

func (s *Store) Join(nodeID, addr string) error {
	s.logger.Printf("received join request for remote node %s at %s", nodeID, addr)

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Printf("failed to get raft configuration: %v", err)
		return err
	}

	servers := configFuture.Configuration().Servers
	for _, server := range servers {
		alreadyJoined := server.ID == raft.ServerID(nodeID) && server.Address == raft.ServerAddress(addr)
		if alreadyJoined {
			s.logger.Printf("node %s at %s already member of cluster, ignoring join request", nodeID, addr)
			return nil
		}

		belongsToCluster := server.ID == raft.ServerID(nodeID) || server.Address == raft.ServerAddress(addr)
		if belongsToCluster {
			f := s.raft.RemoveServer(server.ID, 0, 0)
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
