# Simple Saga
## Current Implementation
1. The application is configured to subscribe to a specific topic on Google Cloud Pub/Sub. It listens for incoming messages and processes them as events.
2. After creating an event it stored in MongoDB.
3. TODO: add state reconstruction functionality from events (also make events more semantic)
4. TODO: implement a simple saga orchestrator

## How to run
1. `docker compose up`

## Watch the events
1. `mongo --host localhost --port 27017`
2. `use testdb`
3. `db.events.find()`