package main

import (
	"fmt"
	"github.com/c-bata/go-prompt"
	"github.com/go-resty/resty/v2"
	"strings"
)

func completer(d prompt.Document) []prompt.Suggest {
	s := []prompt.Suggest{
		{Text: "kv set k1=v1 addr=localhost:11001", Description: "Set key k1 to value v1"},
		{Text: "kv get k1 addr=localhost:11001", Description: "Get the value for key k1"},
		{Text: "kv list keys addr=localhost:11001", Description: "List the keys"},
		{Text: "kv delete k1 addr=localhost:11001", Description: "Delete the key k1"},

		{Text: "raft leader addr=localhost:11001", Description: "Get the raft leader"},
		{Text: "raft servers addr=localhost:11001", Description: "Get all raft servers"},

		{Text: "exit", Description: "Exit the prompt"},
	}
	return prompt.FilterHasPrefix(s, d.GetWordBeforeCursor(), true)
}

func main() {
	for {
		input := prompt.Input("> ", completer)
		fields := strings.Fields(input)
		if fields != nil && len(fields) > 0 {
			if strings.ToLower(fields[0]) == "kv" {
				if len(fields) == 4 {
					handleKVCmd(fields[1], fields[2], fields[3])
				} else {
					fmt.Println("Invalid command")
				}
			} else if strings.ToLower(fields[0]) == "raft" {
				if len(fields) == 3 {
					handleRaftCmd(fields[1], fields[2])
				} else {
					fmt.Println("Invalid command")
				}
			} else if strings.ToLower(fields[0]) == "exit" {
				break
			}
		}
	}
}

func handleKVCmd(cmd string, param string, addr string) {
	cmd = strings.ToLower(cmd)
	addr = strings.Split(addr, "=")[1]
	addr = fmt.Sprintf("http://%s", addr)
	if cmd == "set" {
		p := strings.Split(param, "=")
		if len(p) != 2 {
			fmt.Println("Invalid command")
			return
		}
		kvSet(p[0], p[1], addr)
	} else if cmd == "get" {
		kvGet(param, addr)
	} else if cmd == "list" {
		kvList(addr)
	} else if cmd == "delete" {
		kvDelete(param, addr)
	}
}

func handleRaftCmd(cmd string, addr string) {
	cmd = strings.ToLower(cmd)
	addr = strings.Split(addr, "=")[1]
	addr = fmt.Sprintf("http://%s", addr)
	if cmd == "leader" {
		raftLeader(addr)
	} else if cmd == "servers" {
		raftServers(addr)
	}
}

func kvSet(key string, value string, addr string) {
	resp, err := resty.New().R().
		SetBody(map[string]string{key: value}).
		Post(fmt.Sprintf("%s/keys", addr))
	if err != nil {
		fmt.Println("Failed to set key", err)
	}

	fmt.Println(resp)
}

func kvGet(key string, addr string) {
	resp, err := resty.New().R().
		Get(fmt.Sprintf("%s/keys/%s", addr, key))
	if err != nil {
		fmt.Println("Failed to get key", err)
	}

	fmt.Println(resp)
}

func kvList(addr string) {
	resp, err := resty.New().R().
		Get(fmt.Sprintf("%s/keys", addr))
	if err != nil {
		fmt.Println("Failed to list keys", err)
	}

	fmt.Println(resp)
}

func kvDelete(key string, addr string) {
	resp, err := resty.New().R().
		SetHeader("Accept", "application/json").
		Delete(fmt.Sprintf("%s/keys/%s", addr, key))
	if err != nil {
		fmt.Println("Failed to delete key", err)
	}

	fmt.Println(resp)
}

func raftLeader(addr string) {
	url := fmt.Sprintf("%s/raft/leader", addr)
	resp, err := resty.New().R().
		Get(url)
	if err != nil {
		fmt.Println("Failed to get leader", err)
	}

	fmt.Println(resp)
}

func raftServers(addr string) {
	url := fmt.Sprintf("%s/raft/servers", addr)
	resp, err := resty.New().R().
		Get(url)
	if err != nil {
		fmt.Println("Failed to get servers", err)
	}

	fmt.Println(resp)
}
