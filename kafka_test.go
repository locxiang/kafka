package kafka_test

import (
	"fmt"
	"github.com/locxiang/kafka"
	"testing"
	"time"
)

func TestInit(t *testing.T) {

	err := kafka.Init([]string{"192.168.1.212:9092"}, "test_kafka")
	if err != nil {
		return
	}

	kafka.Singleton().Consume(map[string]kafka.ReceiveTopicHandle{
		"test-dev": new(topicDemo),
	})

	time.Sleep(10 * time.Second)

	kafka.WriteMessage("test-dev", time.Now(), nil, []byte("stringtest"))

	time.Sleep(3 * time.Second)

	kafka.Close()
}

type topicDemo struct {
}

func (C topicDemo) JSON() []byte {
	return nil
}

func (C topicDemo) Topic() string {
	return "test-dev"
}

func (C topicDemo) Key() kafka.IKey {
	return new(topicDemo)
}

func (C topicDemo) Struct() kafka.ReceiveTopicHandle {
	return new(topicDemo)
}

func (C topicDemo) Handle(key kafka.IKey, timestamp time.Time) error {

	fmt.Printf("time:%s", timestamp)
	return nil
}
