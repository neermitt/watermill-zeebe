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
	var name, correlationKey string
	var ttl int64
	var metadata = make(map[string]string)
	for k, v := range msg.Metadata {
		switch k {
		case MessageNameMetadataKey:
			name = v
		case TimeToLiveMetadataKey:
			ttl = StringToInt64(v)
		case middleware.CorrelationIDMetadataKey:
			correlationKey = v
		default:
			metadata[k] = v
		}
	}
	var payload interface{}
	json.Unmarshal(msg.Payload, &payload)
	variables, _ := json.Marshal(map[string]interface{}{
		"metadata": metadata,
		"payload":  payload,
	})
	return &Message{
		Name:           name,
		CorrelationKey: correlationKey,
		TimeToLive:     ttl,
		MessageId:      msg.UUID,
		Variables:      string(variables),
	}, nil
}

func (DefaultMarshaler) Unmarshal(job entities.Job) (*message.Message, error) {
	config, err := NewConfigurationMap(job)
	if err != nil {
		return nil, err
	}

	payload := config.Get("payload")

	b, err := json.Marshal(payload)

	msg := message.NewMessage(watermill.NewUUID(), b)

	if messageName := config.GetString(MessageNameMetadataKey); messageName != "" {
		SetMessageName(messageName, msg)
	}

	if correlationId := config.GetString(middleware.CorrelationIDMetadataKey); correlationId != "" {
		middleware.SetCorrelationID(correlationId, msg)
	}

	if ttl := config.GetInt(TimeToLiveMetadataKey); ttl != 0 {
		SetTimeToLiveString(ttl, msg)
	}

	metadata := config.GetMap("metadata")

	for k, v := range metadata {
		msg.Metadata[k] = v.(string)
	}

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
	return StringToInt64(val)
}

func StringToInt64(val string) int64 {
	if val == "" {
		return 0
	}
	ttl, _ := strconv.ParseInt(val, 10, 64)
	return ttl
}
