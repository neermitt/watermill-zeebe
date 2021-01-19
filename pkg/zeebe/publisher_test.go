package zeebe

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zeebe-io/zeebe/clients/go/pkg/zbc"
)

func TestPublishSingle(t *testing.T) {
	client, err := zbc.NewClient(&zbc.ClientConfig{
		GatewayAddress:         "127.0.0.1:26500",
		UsePlaintextConnection: true,
	})
	require.NoError(t, err)

	defer client.Close()

	pub, err := NewPublisher(PublisherConfig{
		Marshaller: DefaultMarshaler{},
		Client:     client,
	}, watermill.NopLogger{})
	require.NoError(t, err)
	defer pub.Close()

	payload := `{"variable_1": "value1", "variable_2": 2}`

	err = pub.Publish("test", &message.Message{
		UUID:     uuid.New().String(),
		Metadata: message.Metadata{MessageNameMetadataKey: "test", TimeToLiveMetadataKey: "10"},
		Payload:  []byte(payload),
	})

	assert.NoError(t, err)
}

func TestPublishRepeat(t *testing.T) {
	client, err := zbc.NewClient(&zbc.ClientConfig{
		GatewayAddress:         "127.0.0.1:26500",
		UsePlaintextConnection: true,
	})
	require.NoError(t, err)

	defer client.Close()

	pub, err := NewPublisher(PublisherConfig{
		Marshaller: DefaultMarshaler{},
		Client:     client,
	}, watermill.NopLogger{})
	require.NoError(t, err)
	defer pub.Close()

	payload := `{"variable_1": "value1", "variable_2": 2}`

	uuuid := uuid.New().String()
	err = pub.Publish("test", &message.Message{
		UUID:     uuuid,
		Metadata: message.Metadata{MessageNameMetadataKey: "test", TimeToLiveMetadataKey: "10"},
		Payload:  []byte(payload),
	})

	assert.NoError(t, err)

	err = pub.Publish("test", &message.Message{
		UUID:     uuuid,
		Metadata: message.Metadata{MessageNameMetadataKey: "test", TimeToLiveMetadataKey: "10"},
		Payload:  []byte(payload),
	})

	assert.NoError(t, err)
}
