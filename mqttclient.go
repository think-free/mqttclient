package mqttclient

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/surgemq/message"
	"github.com/surgemq/surgemq/service"
)

// MqttClient is a wrapper for surgemq mqtt
type MqttClient struct {
	name      string
	server    string
	topics    map[string]*service.OnPublishFunc
	connected bool
	c         *service.Client
	sync.Mutex
}

// NewMqttClient return a mqtt client
func NewMqttClient(name, server string) *MqttClient {

	cli := &MqttClient{
		name:      name,
		server:    server,
		connected: false,
		topics:    make(map[string]*service.OnPublishFunc),
		c:         nil,
	}

	return cli
}

func (cli *MqttClient) SendHB(topic string) {

	go func() {

		for {
			cli.PublishMessage(topic, int64(time.Now().Unix()))
			time.Sleep(time.Second * 10)
		}
	}()
}

// Connect (or reconnect) to server
func (cli *MqttClient) Connect() error {

	log.Println("[" + cli.name + "] connecting mqtt")

	cli.setConnected(false)

	cli.c = &service.Client{}
	msg := message.NewConnectMessage()
	msg.SetWillQos(1)
	msg.SetVersion(4)
	msg.SetCleanSession(true)
	msg.SetClientId([]byte(cli.name))
	msg.SetKeepAlive(10)
	msg.SetWillTopic([]byte(cli.name))
	msg.SetWillMessage([]byte(cli.name + " client disconnected"))
	err := cli.c.Connect("tcp://"+cli.server+":1883", msg)

	if err == nil {

		cli.setConnected(true)

		log.Println("[" + cli.name + "] connected mqtt")

		for topic, onPublish := range cli.topics {

			cli.SubscribeTopic(topic, *onPublish)
		}
	} else {

		log.Println("["+cli.name+"] error :", err)
	}

	return err
}

func (cli *MqttClient) setConnected(c bool) {

	cli.Lock()
	cli.connected = c
	cli.Unlock()
}

func (cli *MqttClient) isConnected() bool {

	cli.Lock()
	c := cli.connected
	cli.Unlock()

	return c
}

// SubscribeTopic subscribe to mqtt
// Example : func onPublished(msg *message.PublishMessage) error {}
func (cli *MqttClient) SubscribeTopic(topic string, onPublish service.OnPublishFunc) {

	cli.topics[topic] = &onPublish

	if !cli.isConnected() {
		return
	}

	submsgset := message.NewSubscribeMessage()
	submsgset.AddTopic([]byte(topic), 1)
	cli.c.Subscribe(submsgset, nil, onPublish)
}

// PublishMessage to mqtt
func (cli *MqttClient) PublishMessage(topic string, value interface{}) {

	if !cli.isConnected() {
		log.Println("Client is not connected, can't send", topic, value)
		return
	}

	pubmsg := message.NewPublishMessage()
	pubmsg.SetTopic([]byte(topic))
	pubmsg.SetQoS(1)
	pubmsg.SetRetain(true)
	js, _ := json.Marshal(value)
	pubmsg.SetPayload(js)

	// Publish to the server by sending the message
	err := cli.c.Publish(pubmsg, nil)
	if err != nil {
		log.Println("Error :", err)
		cli.setConnected(false)

		for {
			cli.Connect()

			if cli.isConnected() {

				return
			}

			time.Sleep(time.Second)
		}
	}
}

// TracePublishMessage to mqtt with optional trace
func (cli *MqttClient) TracePublishMessage(topic string, value interface{}, trace bool) {

	if trace {
		log.Println(topic, "->", value)
	}
	cli.PublishMessage(topic, value)
}
