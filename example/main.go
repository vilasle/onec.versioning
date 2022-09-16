package main

import (
	// "bytes"
	"bytes"
	"encoding/xml"
	"fmt"
	// "fmt"
	"io/ioutil"
	// "github.com/vilamslep/onec.versioning/dbms/postgres"
	// "github.com/vilamslep/onec.versioning/lib"
)

type Node struct {
	XMLName xml.Name
	Content []byte `xml:",innerxml"`
	Nodes   []Node `xml:",any"`
}

func main() {

	// lib.LoadEnv("dev.env")
	// err := lib.CreateConnections()
	// if err != nil {
	// 	panic(err)
	// }

	data, err := ioutil.ReadFile("data")
	if err != nil {
		panic(err)
	}
	buf := bytes.NewBuffer(data)
	dec := xml.NewDecoder(buf)

	var n Node
	err = dec.Decode(&n)
	if err != nil {
		panic(err)
	}

	walk([]Node{n}, func(n Node) bool {
		if n.XMLName.Local == "p" {
			fmt.Println(string(n.Content))
		}
		return true
	})
	// q := `INSERT INTO test (ver) VALUES($1);`
	// _, err = postgres.Session.Exec(q, data)
	// if err != nil {
	// 	panic(err)
	// }
}

func walk(nodes []Node, f func(Node) bool) {
	for _, n := range nodes {
		if f(n) {
			walk(n.Nodes, f)
		}
	}
}
