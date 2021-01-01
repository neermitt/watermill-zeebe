package zeebe

import (
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"github.com/zeebe-io/zeebe/clients/go/pkg/zbc"
)

type PublisherConfig struct {
	Marshaller Marshaller
	Client     zbc.Client
}

func (c *PublisherConfig) setDefaults() {
}

func (c PublisherConfig) validate() error {
	if c.Client == nil {
		return errors.New("missing zeebe client")
	}
	if c.Marshaller == nil {
		return errors.New("missing marshaller")
	}

	return nil
}

type Publisher struct {
	logger watermill.LoggerAdapter
	config PublisherConfig

	closed bool
}

func NewPublisher(config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	config.setDefaults()
	if err := config.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid Publisher config")
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &Publisher{
		config: config,
		logger: logger,
	}, nil
}

func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	if p.closed {
		return errors.New("publisher closed")
	}

	logFields := make(watermill.LogFields, 3)
	logFields["topic"] = topic

	for _, msg := range messages {
		logFields["message_uuid"] = msg.UUID
		p.logger.Trace("Sending message to zeebe", logFields)

		zbMsg, err := p.config.Marshaller.Marshal(topic, msg)
		if err != nil {
			return errors.Wrapf(err, "cannot marshal message %s", msg.UUID)
		}

		dispatcher, err := p.config.Client.NewPublishMessageCommand().
			MessageName(zbMsg.Name).
			CorrelationKey(zbMsg.CorrelationKey).
			MessageId(zbMsg.MessageId).
			TimeToLive(time.Duration(zbMsg.TimeToLive) * time.Millisecond).
			VariablesFromString(zbMsg.Variables)
		if err != nil {
			return errors.Wrapf(err, "can't marshal message %s", msg.UUID)
		}
		resp, err := dispatcher.Send(msg.Context())
		if err != nil {
			return errors.Wrapf(err, "can't send message %s", msg.UUID)
		}

		logFields["message-key"] = resp.Key

		p.logger.Trace("Message sent to zeebe", logFields)
	}
	return nil
}

func (p *Publisher) Close() error {
	if p.closed {
		return nil
	}

	p.closed = true
	return nil
}
