package main

import (
	"flag"
	"fmt"
	"github.com/go-resty/resty/v2"
	"github.com/naveen246/kvdb/service"
	"github.com/naveen246/kvdb/store"
	"log"
	"os"
	"os/signal"
)

// Command line defaults
const (
	DefaultHTTPAddr = "localhost:11001"
	DefaultRaftAddr = "localhost:12001"
)

// Command line parameters
var httpAddr string
var raftAddr string
var joinAddr string
var nodeID string

func init() {
	flag.StringVar(&httpAddr, "httpaddr", DefaultHTTPAddr, "Set the HTTP bind address")
	flag.StringVar(&raftAddr, "raftaddr", DefaultRaftAddr, "Set Raft bind address")
	flag.StringVar(&joinAddr, "join", "", "Set join address, if any")
	flag.StringVar(&nodeID, "id", "", "Node ID. If not set, same as Raft bind address")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <raft-data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	if nodeID == "" {
		nodeID = raftAddr
	}

	stor := store.NewStore()
	stor.RaftAddr = raftAddr
	stor.RaftDir = stor.DataDir(raftAddr)

	err := stor.Open(joinAddr == "", nodeID)
	if err != nil {
		log.Fatalf("failed to open stor: %s", err.Error())
	}

	service.New(httpAddr, stor, stor).Start()

	// If join was specified, make the join request.
	if joinAddr != "" {
		err := join(joinAddr, raftAddr, nodeID)
		if err != nil {
			log.Fatalf("failed to join node at %s: %s", joinAddr, err.Error())
		}
	}

	log.Printf("kvdb started successfully, listening on %s", httpAddr)

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	log.Println("kvdb exiting")
}

func join(joinAddr, raftAddr, nodeID string) error {
	url := fmt.Sprintf("http://%s/raft/join", joinAddr)

	_, err := resty.New().R().
		SetBody(map[string]string{"addr": raftAddr, "nodeID": nodeID}).
		Post(url)
	if err != nil {
		return err
	}

	return nil
}
