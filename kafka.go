package kafka

import (
	"encoding/json"
	"github.com/locxiang/util"
	"strings"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	_ "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka/librdkafka_vendor"
)

type kafkaDrive struct {
	addrs            []string
	producer         *kafka.Producer
	consumer         *kafka.Consumer
	consumerHandlers map[string]ReceiveTopicHandle
	isClose          chan bool
}

var k *kafkaDrive

func Init(addrs []string, groupId string) (err error) {

	p, err2 := kafka.NewProducer(&kafka.ConfigMap{
		//"plugin.library.paths": "monitoring-interceptor",
		"bootstrap.servers": strings.Join(addrs, ","),
                "compression.type":         "gzip", //压缩
		"compression.codec":        "gzip", //压缩
	})
	if err2 != nil {
		return err2
	}
	p.Flush(3 * 1000)

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Errorf("Delivery failed: %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	clientId := util.UUID()
	log.Infof("kafka consumer client id: %s", clientId)
	c, err3 := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        strings.Join(addrs, ","),
		"group.id":                 groupId,
		"auto.offset.reset":        "earliest",
		"client.id":                clientId,
		"session.timeout.ms":       15 * 1000,
		"go.events.channel.enable": true,
		"compression.type":         "gzip", //压缩
		"compression.codec":        "gzip", //压缩

		//"plugin.library.paths":     "monitoring-interceptor",
		//"go.application.rebalance.enable": true,
		// Enable generation of PartitionEOF when the
		// end of a partition is reached.
		//"enable.partition.eof": false,
		//"debug":                              "all",
		"topic.metadata.refresh.interval.ms": 60 * 1000,
		"socket.keepalive.enable":            true,
		"broker.address.family":              "v4",
	})

	if err3 != nil {
		panic(err3)
	}

	k = &kafkaDrive{
		addrs:    addrs,
		producer: p,
		consumer: c,
		isClose:  make(chan bool),
	}

	return nil

}

// 获取kafka
func Singleton() *kafkaDrive {
	if k == nil {
		panic("kafka未初始化")
	}
	return k
}

func WriteMessage(topic string, timestamp time.Time, key, message []byte) error {
	log.Debugf("写入kafka[%s]", topic)
	err := Singleton().producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Timestamp:      timestamp,
		Value:          message,
		Key:            key,
	}, nil)
	return err
}

type ReceiveTopicHandle interface {
	Topic() string
	Key() IKey //返回key结构
	Struct() ReceiveTopicHandle
	Handle(key IKey, timestamp time.Time) error
}

type IKey interface {
	JSON() []byte
}

// 监听topics数据
func (k *kafkaDrive) Consume(handlers map[string]ReceiveTopicHandle) error {

	k.consumerHandlers = make(map[string]ReceiveTopicHandle)
	topics := make([]string, 0)

	for _, h := range handlers {
		topics = append(topics, h.Topic())
		log.Info("注册topic：", h.Topic())
		k.consumerHandlers[h.Topic()] = h
	}

	err := k.consumer.SubscribeTopics(topics, nil)
	if err != nil {
		return err
	}

	go func() {
		k.consumerSubscribeEvents()
	}()

	return nil
}

func (k *kafkaDrive) consumerSubscribeEvents() {
	for {
		select {
		case ev := <-k.consumer.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				log.Info("AssignedPartitions 分配成功：", e.Partitions)
				k.consumer.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				k.consumer.Unassign()
				log.Panic("RevokedPartitions 释放成功：", e.Partitions)
			case *kafka.Message:
				if err := k.handleMessage(e); err != nil {
					log.Errorf("kafka events message error：%s", err)
				}
			case kafka.PartitionEOF:
				log.Debugf("kafka 没有数据：%v", e)
			case kafka.Error:
				// Errors should generally be considered as informational, the client will try to automatically recover
				log.Errorf("Error: %s", e)
				go func(e error) {
					time.Sleep(5 * time.Second)
					log.Panicf("Error: %s", e)
				}(e)
			}
		case <-k.isClose:
			log.Info("kafka consumer结束")
			return
		}
	}
}

func (k *kafkaDrive) handleMessage(msg *kafka.Message) error {
	sg, ok := k.consumerHandlers[*msg.TopicPartition.Topic]
	if !ok {
		return errors.New("接收到未订阅的topic: " + *msg.TopicPartition.Topic)
	}

	svc := sg.Struct()

	//转换key 和value
	if err := json.Unmarshal(msg.Value, svc); err != nil {
		log.Errorf("错误value转换数据：%s", msg.Value)
		return errors.Wrap(err, "转换value出错")
	}
	iKey := sg.Key()
	if err := json.Unmarshal(msg.Key, iKey); err != nil {
		return errors.Wrap(err, "转换Key出错:"+string(msg.Key))
	}

	log.Debugf("接收到【%s】的数据", sg.Topic())
	err := svc.Handle(iKey, msg.Timestamp)
	if err != nil {
		return errors.Wrap(err, "调用handle失败")
	}

	return nil
}

func (k *kafkaDrive) Close() {
	k.isClose <- true
	log.Info("关闭kafka服务")
	k.consumer.Unassign()
	k.consumer.Close()
	k.producer.Close()
}
