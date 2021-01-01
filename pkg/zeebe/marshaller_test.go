package zeebe_test

import (
	"encoding/json"
	"math/rand"
	"strconv"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zeebe-io/zeebe/clients/go/pkg/entities"
	"github.com/zeebe-io/zeebe/clients/go/pkg/pb"

	"github.com/neermitt/watermill-zeebe/pkg/zeebe"
)

func TestDefaultMarshaler_MarshalUnmarshal(t *testing.T) {
	m := zeebe.DefaultMarshaler{}

	msg := message.NewMessage(watermill.NewUUID(), []byte("{\"test\":123}"))

	marshaled, err := m.Marshal("topic", msg)
	require.NoError(t, err)

	unmarshaledMsg, err := m.Unmarshal(messageToJob(marshaled))
	require.NoError(t, err)

	assert.Equal(t, msg.Payload, unmarshaledMsg.Payload)
}

func BenchmarkDefaultMarshaler_Marshal(b *testing.B) {
	m := zeebe.DefaultMarshaler{}

	msg := message.NewMessage(watermill.NewUUID(), []byte("{\"test\":123}"))
	middleware.SetCorrelationID(watermill.NewUUID(), msg)

	for i := 0; i < b.N; i++ {
		m.Marshal("foo", msg)
	}
}

func BenchmarkDefaultMarshaler_Unmarshal(b *testing.B) {
	m := zeebe.DefaultMarshaler{}

	msg := message.NewMessage(watermill.NewUUID(), []byte("{\"test\":123}"))

	marshaled, err := m.Marshal("foo", msg)
	if err != nil {
		b.Fatal(err)
	}

	job := messageToJob(marshaled)

	for i := 0; i < b.N; i++ {
		m.Unmarshal(job)
	}
}

func messageToJob(marshaled *zeebe.Message) entities.Job {
	headers, _ := json.Marshal(map[string]string{
		zeebe.MessageNameMetadataKey: marshaled.Name,
		zeebe.TimeToLiveMetadataKey:  strconv.FormatInt(marshaled.TimeToLive, 10),
	})

	return entities.Job{
		ActivatedJob: &pb.ActivatedJob{
			Key:                       rand.Int63(),
			Type:                      "message",
			WorkflowInstanceKey:       0,
			BpmnProcessId:             "test-workflow",
			WorkflowDefinitionVersion: 1,
			WorkflowKey:               rand.Int63(),
			ElementId:                 "test-element",
			ElementInstanceKey:        rand.Int63(),
			CustomHeaders:             string(headers),
			Worker:                    "test-worker",
			Retries:                   1,
			Deadline:                  0,
			Variables:                 marshaled.Variables,
		},
	}
}
