package v

import (
    "encoding/json"
    "fmt"
)

type Node struct {
    Name string `json:"name"`
    Id string `json:"id"`
    Address string `json:"addr"`
}

func (n *Node) MarshalKey() ([]byte, error) {
    k := fmt.Sprintf("%v/%v", n.Name, n.Id)
    return []byte(k), nil
}

func (n *Node) MarshalNode() ([]byte, error) {
    return json.Marshal(n)
}

func (n *Node) UnmarshalNode(b []byte) error {
    return json.Unmarshal(b, n)
}

func (n *Node) Addr() string {
    return n.Address
}