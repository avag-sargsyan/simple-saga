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
	// init mongodb connection
	initMongoClient("mongodb://mongodb:27017")

	// create a new watermill message router
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		log.Fatalln("Router", err)
	}

	// this adds a plugin to the router for handling OS signals
	router.AddPlugin(plugin.SignalsHandler)

	// middleware for automatic recovery
	router.AddMiddleware(middleware.Recoverer)

	// get a publisher and subscriber
	publisher, subscriber := setupPubSub()

	// generate and publish events continuously without blocking the main thread
	go simulateEvents()

	// add a handler to the router subscribed to googleCloudTopic and process incoming events
	router.AddHandler(
		handlerName,      // handler name
		googleCloudTopic, // topic to subscribe to
		subscriber,       // subscriber which receives messages from the topic
		"dummy_topic",    // topic to publish to, not used here
		publisher,        // publisher which publishes messages to the topic
		func(msg *message.Message) ([]*message.Message, error) { // process incoming messages
			consumedEvent := MyEvent{}
			err := json.Unmarshal(msg.Payload, &consumedEvent)
			if err != nil {
				return nil, err
			}

			log.Printf("received event %+v with UUID %s", consumedEvent, msg.UUID)

			// return indicated that the message has been processed successfully
			// in this case handler doesn't produce any new messages to be published
			return []*message.Message{msg}, nil
		},
	)

	// start the router to process incoming events
	if err := router.Run(context.Background()); err != nil {
		log.Fatalf("could not start router: %v", err)
	}
}

// simulateEvents continuously generates and publishes events to a Google Cloud Pub/Sub topic
func simulateEvents() {
	// Initialize a new publisher for Google Cloud Pub/Sub
	pub, err := googlecloud.NewPublisher(googlecloud.PublisherConfig{}, logger)
	if err != nil {
		log.Fatalf("could not create publisher: %v", err)
	}

	log.Printf("publishing events to topic %s", googleCloudTopic)

	// Init a loop to continuously generate and publish events
	for {
		// Generate a new event with unique ID
		e := MyEvent{
			ID:        watermill.NewUUID(),
			EventType: "example_event",
			Payload:   "Hello, World!",
			Timestamp: time.Now(),
		}

		log.Printf("publishing event %+v", e)

		// Marshal event to JSON
		payload, err := json.Marshal(e)
		if err != nil {
			log.Fatalf("could not marshal event: %v", err)
		}

		// Store event in MongoDB, should be donne in a transaction
		err = storeEventInMongo(e)
		if err != nil {
			log.Printf("Failed to store event: %v", err)
			continue // TODO: retry/circuit breaker
		} else {
			log.Println("Event stored:", e)
		}

		// Publish event to Google Cloud Pub/Sub
		err = pub.Publish(googleCloudTopic, message.NewMessage(
			watermill.NewUUID(),
			payload,
		))
		if err != nil {
			log.Fatalf("could not publish event: %v", err)
		}

		time.Sleep(5 * time.Second)
	}
}

// initMongoClient initializes a MongoDB client
func initMongoClient(uri string) {
	var err error
	mongoClient, err = mongo.Connect(context.Background(), options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
}

// storeEventInMongo stores an event in MongoDB
func storeEventInMongo(event MyEvent) error {
	collection := mongoClient.Database("testdb").Collection("events")
	_, err := collection.InsertOne(context.Background(), event)
	return err
}

// setupPubSub sets up a Google Cloud Pub/Sub publisher and subscriber
// implementation details: https://watermill.io/pubsubs/googlecloud/
func setupPubSub() (message.Publisher, message.Subscriber) {
	// init watermill logger
	logger := watermill.NewStdLogger(false, false)

	// create a new publisher
	// we need this to create a handler
	publisher, err := googlecloud.NewPublisher(
		googlecloud.PublisherConfig{},
		logger,
	)
	if err != nil {
		log.Fatalf("could not create publisher: %v", err)
	}

	// create a new subscriber
	// we need this to create a handler
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
