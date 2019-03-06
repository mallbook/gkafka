package gkafka

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/mallbook/commandline"
	kafka "github.com/segmentio/kafka-go"
)

var (
	writers = make(map[string]*kafka.Writer)
	readers = make(map[string]*kafka.Reader)
)

// Writer return writer of the topic
func Writer(topic string) (writer *kafka.Writer, ok bool) {
	writer, ok = writers[topic]
	return
}

// Reader return reader of the topic
func Reader(topic string) (reader *kafka.Reader, ok bool) {
	reader, ok = readers[topic]
	return
}

// Close close all kafka connection
func Close() {
	for _, writer := range writers {
		writer.Close()
	}

	for _, reader := range readers {
		reader.Close()
	}
}

type writerConfig struct {
	Balancer string `json:"balancer"`
	Async    bool   `json:"async"`
}

type readerConfig struct {
	GroupID   string `json:"groupID"`
	Partition int    `json:"partition"`
	MinBytes  int    `json:"minBytes"`
	MaxBytes  int    `json:"maxBytes"`
}

type kafkaConfig struct {
	Brokers []string                `json:"brokers"`
	Writer  map[string]writerConfig `json:"writer"`
	Reader  map[string]readerConfig `json:"reader"`
}

func init() {
	prefix := commandline.GetPrefixPath()
	configFile := prefix + "/etc/conf/kafka.json"
	config, err := loadConfig(configFile)
	if err != nil {
		fmt.Printf("Load config file(%s) fail, err = %s", configFile, err.Error())
		os.Exit(1)
	}

	initWriter(config)
	initReader(config)
}

func initWriter(config kafkaConfig) {
	for topic, conf := range config.Writer {
		var balancer kafka.Balancer
		switch strings.ToUpper(conf.Balancer) {
		case "HASH":
			balancer = &kafka.Hash{}
		case "LEASTBYTES":
			balancer = &kafka.LeastBytes{}
		case "ROUNDROBIN":
			balancer = &kafka.RoundRobin{}
		default:
			balancer = &kafka.RoundRobin{}
		}

		w := kafka.NewWriter(kafka.WriterConfig{
			Brokers:  config.Brokers,
			Topic:    topic,
			Balancer: balancer,
			Async:    conf.Async,
		})

		if _, ok := writers[topic]; !ok {
			writers[topic] = w
		} else {
			w.Close()
		}
	}
}

func initReader(config kafkaConfig) {
	for topic, conf := range config.Reader {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   config.Brokers,
			GroupID:   conf.GroupID,
			Partition: conf.Partition,
			Topic:     topic,
			MinBytes:  conf.MinBytes,
			MaxBytes:  conf.MaxBytes,
		})

		if _, ok := readers[topic]; !ok {
			readers[topic] = r
		} else {
			r.Close()
		}
	}
}

func loadConfig(fileName string) (conf kafkaConfig, err error) {
	bytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		return
	}

	if err = json.Unmarshal(bytes, &conf); err != nil {
		return
	}

	return
}
