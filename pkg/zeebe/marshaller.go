package zeebe

import (
	"encoding/json"
	"strconv"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/zeebe-io/zeebe/clients/go/pkg/entities"
)

const (
	MessageNameMetadataKey = "message_name"
	TimeToLiveMetadataKey  = "message_ttl"
)

type Message struct {
	// the name of the message
	Name string
	// the correlation key of the message
	CorrelationKey string
	// how long the message should be buffered on the broker, in milliseconds
	TimeToLive int64
	// the unique ID of the message; can be omitted. only useful to ensure only one message
	// with the given ID will ever be published (during its lifetime)
	MessageId string
	// the message variables as a JSON document; to be valid, the root of the document must be an
	// object, e.g. { "a": "foo" }. [ "foo" ] would not be valid.
	Variables string
}

// Marshaller marshals Watermill's message to Zeebe's message.
type Marshaller interface {
	Marshal(topic string, msg *message.Message) (Message, error)
}

// Unmarshaler marshals Zeebe's message to Watermill's message.
type Unmarshaler interface {
	Unmarshal(entities.Job) (*message.Message, error)
}

type MarshalerUnmarshaler interface {
	Marshaller
	Unmarshaler
}

type DefaultMarshaler struct{}

func (DefaultMarshaler) Marshal(topic string, msg *message.Message) (*Message, error) {
	return &Message{
		Name:           MessageName(msg),
		CorrelationKey: middleware.MessageCorrelationID(msg),
		TimeToLive:     TimeToLive(msg),
		MessageId:      msg.UUID,
		Variables:      string(msg.Payload),
	}, nil
}

func (DefaultMarshaler) Unmarshal(job entities.Job) (*message.Message, error) {
	config, err := NewConfigurationMap(job)
	if err != nil {
		return nil, err
	}

	vars := config.GetMap("variables")

	b, err := json.Marshal(vars)

	msg := message.NewMessage(watermill.NewUUID(), b)

	SetMessageName(config.GetString(MessageNameMetadataKey), msg)
	middleware.SetCorrelationID(config.GetString(middleware.CorrelationIDMetadataKey), msg)
	SetTimeToLiveString(config.GetInt(TimeToLiveMetadataKey), msg)

	return msg, nil
}

func SetMessageName(name string, msg *message.Message) {
	if MessageName(msg) != "" {
		return
	}
	msg.Metadata.Set(MessageNameMetadataKey, name)
}

func MessageName(msg *message.Message) string {
	return msg.Metadata.Get(MessageNameMetadataKey)
}

func SetTimeToLiveString(ttl int64, msg *message.Message) {
	if TimeToLive(msg) != 0 {
		return
	}
	msg.Metadata.Set(TimeToLiveMetadataKey, strconv.FormatInt(ttl, 10))
}

func TimeToLive(msg *message.Message) int64 {
	val := msg.Metadata.Get(TimeToLiveMetadataKey)
	if val == "" {
		return 0
	}
	ttl, _ := strconv.ParseInt(val, 10, 64)
	return ttl
}
