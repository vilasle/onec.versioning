package main

import (
	"fmt"
	"log"
	"os"

	"github.com/vilamslep/iokafka"
	"github.com/vilamslep/onec.versioning/lib"
)

const DEBUG = true

func main() {

	if DEBUG {
		lib.LoadEnv("dev.env")
	}

	var err error
	if err = lib.CreateConnections(); err != nil {
		log.Fatalln(err)
	}

	scanner := iokafka.NewScanner(loadKafkaConfigFromEnv(0))

	if DEBUG {
		s := `{"#",7433d4e8-f07d-4ace-819f-00191a4913db,431:b3c1a4bf015829f711ed05ebb7aaf42c}`
		u := "7433d4e8-f07d-4ace-819f-00191a4913db"
		if err := handleMessage([]byte(s), []byte(u)); err != nil {
			log.Println(err)
		}
		os.Exit(0)
	}

	for scanner.Scan() {
		msg := scanner.Message()
		if err := handleMessage(msg.Value, msg.Key); err != nil {
			log.Println(err)
		}
	}
}

func handleMessage(content []byte, user []byte) error {
	
	return nil
}


func loadKafkaConfigFromEnv(offset int64) iokafka.ScannerConfig {

	host := os.Getenv("KAFKAHOST")
	port := os.Getenv("KAFKAPORT")
	topic := os.Getenv("KAFKATOPIC")
	group := os.Getenv("KAFKAGROUP")

	soc := fmt.Sprintf("%s:%s", host, port)

	return iokafka.ScannerConfig{
		Brokers:       []string{soc},
		Topic:         topic,
		GroupID:       group,
		AttemtsOnFail: 5,
		FailTimeout:   10,
		StartOffset:   offset,
	}
}