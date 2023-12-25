package main

import (
	"context"
	"encoding/json"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-googlecloud/pkg/googlecloud"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"time"
)

type Service func(ctx context.Context) (int64, error)

//func main() {
//	svcs := []Service{
//		Service1,
//		Service2,
//		Service3,
//	}
//
//	var total int64
//
//	start := time.Now()
//
//	g, ctx := errgroup.WithContext(context.Background())
//
//	for _, svc := range svcs {
//		svc := svc
//		g.Go(func() error {
//			v, err := svc(ctx)
//			if err != nil {
//				return err
//			}
//
//			_ = atomic.AddInt64(&total, v)
//
//			return nil
//		})
//	}
//
//	if err := g.Wait(); err != nil {
//		log.Fatalln("Group", err)
//	}
//
//	fmt.Println("Total", total, "Duration", time.Now().Sub(start))
//}

var (
	logger           = watermill.NewStdLogger(false, false)
	googleCloudTopic = "events"
	mongoClient      *mongo.Client
	handlerName      = "gcp-mongo"
)

type MyEvent struct {
	ID        string    `bson:"_id"`
	EventType string    `bson:"event_type"`
	Payload   string    `bson:"payload"`
	Timestamp time.Time `bson:"timestamp"`
}

func main() {
	initMongoClient("mongodb://mongodb:27017")
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		log.Fatalln("Router", err)
	}

	router.AddPlugin(plugin.SignalsHandler)
	router.AddMiddleware(middleware.Recoverer)

	publisher, subscriber := setupPubSub()
	go simulateEvents()

	router.AddHandler(
		handlerName,
		googleCloudTopic,
		subscriber,
		"dummy_topic",
		publisher,
		func(msg *message.Message) ([]*message.Message, error) {
			consumedEvent := MyEvent{}
			err := json.Unmarshal(msg.Payload, &consumedEvent)
			if err != nil {
				return nil, err
			}

			log.Printf("received event %+v with UUID %s", consumedEvent, msg.UUID)

			return []*message.Message{msg}, nil
		},
	)

	if err := router.Run(context.Background()); err != nil {
		panic(err)
	}
}

func simulateEvents() {
	pub, err := googlecloud.NewPublisher(googlecloud.PublisherConfig{}, logger)
	if err != nil {
		panic(err)
	}

	log.Printf("publishing events to topic %s", googleCloudTopic)

	for {
		e := MyEvent{
			ID:        watermill.NewUUID(),
			EventType: "example_event",
			Payload:   "Hello, World!",
			Timestamp: time.Now(),
		}

		log.Printf("publishing event %+v", e)

		payload, err := json.Marshal(e)
		if err != nil {
			panic(err)
		}

		// Store event in MongoDB, should be donne in a transaction
		err = storeEventInMongo(e)
		if err != nil {
			log.Printf("Failed to store event: %v", err)
			continue // TODO: retry/circuit breaker
		} else {
			log.Println("Event stored:", e)
		}

		err = pub.Publish(googleCloudTopic, message.NewMessage(
			watermill.NewUUID(),
			payload,
		))
		if err != nil {
			panic(err)
		}

		time.Sleep(5 * time.Second)
	}
}

func Service1(ctx context.Context) (int64, error) {
	time.Sleep(100 * time.Millisecond)
	return 1, nil
}

func Service2(ctx context.Context) (int64, error) {
	time.Sleep(100 * time.Millisecond)
	return 2, nil
}

func Service3(ctx context.Context) (int64, error) {
	time.Sleep(200 * time.Millisecond)
	return 3, nil
}

func initMongoClient(uri string) {
	var err error
	mongoClient, err = mongo.Connect(context.Background(), options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
}

func storeEventInMongo(event MyEvent) error {
	collection := mongoClient.Database("testdb").Collection("events")
	_, err := collection.InsertOne(context.Background(), event)
	return err
}

func setupPubSub() (message.Publisher, message.Subscriber) {
	logger := watermill.NewStdLogger(false, false)

	publisher, err := googlecloud.NewPublisher(
		googlecloud.PublisherConfig{},
		logger,
	)
	if err != nil {
		log.Fatalf("could not create publisher: %v", err)
	}

	subscriber, err := googlecloud.NewSubscriber(
		googlecloud.SubscriberConfig{
			GenerateSubscriptionName: func(topic string) string {
				return topic + "_subscription"
			},
		},
		logger,
	)
	if err != nil {
		log.Fatalf("could not create subscriber: %v", err)
	}

	return publisher, subscriber
}
