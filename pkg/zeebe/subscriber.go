package zeebe

import (
	"context"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"github.com/zeebe-io/zeebe/clients/go/pkg/entities"
	"github.com/zeebe-io/zeebe/clients/go/pkg/worker"
	"github.com/zeebe-io/zeebe/clients/go/pkg/zbc"
)

type SubscriberConfig struct {
	Client      zbc.Client
	Worker      string
	Timeout     time.Duration
	Unmarshaler Unmarshaler
}

func (c *SubscriberConfig) setDefaults() {
	if c.Worker == "" {
		c.Worker = "default"
	}
	if c.Timeout == 0 {
		c.Timeout = 5 * time.Minute
	}
}

func (c SubscriberConfig) validate() error {
	if c.Client == nil {
		return errors.New("missing zeebe client")
	}
	if c.Unmarshaler == nil {
		return errors.New("missing unmarshaller")
	}

	return nil
}

type Subscriber struct {
	config SubscriberConfig
	logger watermill.LoggerAdapter
	worker worker.JobWorker
}

func NewSubscriber(config SubscriberConfig, logger watermill.LoggerAdapter) (*Subscriber, error) {
	config.setDefaults()
	if err := config.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid Publisher config")
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}
	return &Subscriber{config: config, logger: logger}, nil
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	messages := make(chan *message.Message)

	logFields := watermill.LogFields{"topic": topic, "provider": "zeebe", "worker": s.config.Worker}

	s.logger.Info("Subscribing to Zeebe job", logFields)
	s.worker = s.config.Client.NewJobWorker().
		JobType(topic).
		Handler(s.Handler(ctx, messages, logFields)).
		Timeout(s.config.Timeout).
		Name(s.config.Worker).
		Concurrency(worker.DefaultJobWorkerConcurrency).
		MaxJobsActive(worker.DefaultJobWorkerMaxJobActive).
		PollInterval(worker.DefaultJobWorkerPollInterval).
		PollThreshold(worker.DefaultJobWorkerPollThreshold).
		Open()

	return messages, nil
}

func (s *Subscriber) Handler(ctx context.Context, messages chan *message.Message, logFields watermill.LogFields) worker.JobHandler {
	return func(client worker.JobClient, job entities.Job) {
		msg, err := s.config.Unmarshaler.Unmarshal(job)
		if err != nil {
			s.logger.Error("Cannot unmarshal message", err, logFields)
			client.NewFailJobCommand().JobKey(job.Key).Retries(0).
				ErrorMessage(errors.Wrap(err, "cannot unmarshal message").Error()).
				Send(ctx)
			return
		}
		if msg == nil {
			s.logger.Info("No message returned by UnmarshalMessageFunc", logFields)
			client.NewFailJobCommand().JobKey(job.Key).Retries(0).
				ErrorMessage("No message returned by UnmarshalMessageFunc").
				Send(ctx)
			return
		}
		msgContext, cancelCtx := context.WithTimeout(ctx, s.config.Timeout)
		msg.SetContext(msgContext)
		defer cancelCtx()

		logFields := logFields.Add(watermill.LogFields{"message_uuid": msg.UUID})

		s.logger.Trace("Sending msg", logFields)
		messages <- msg

		s.logger.Trace("Waiting for ACK", logFields)
		select {
		case <-msg.Acked():
			s.logger.Trace("Message acknowledged", logFields.Add(watermill.LogFields{"err": err}))
			client.NewCompleteJobCommand().JobKey(job.Key).Send(ctx)
		case <-msg.Nacked():
			s.logger.Trace("Message nacked", logFields)
			errorCommand := client.NewFailJobCommand().JobKey(job.Key).Retries(job.Retries - 1).ErrorMessage("Message nacked")
			if _, err := errorCommand.Send(ctx); err != nil {
				s.logger.Error("fail job command ", err, logFields)
			}
		}
	}
}

func (s *Subscriber) Close() error {
	s.worker.Close()
	s.worker.AwaitClose()
	return nil
}
