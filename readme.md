# MatchMaking Solution

A distributed matchmaking system built with .NET 9, Kafka, and Redis. This solution handles high-concurrency user requests by decoupling the API from the processing logic using a message broker.

## Architecture

### MatchMaking.Service
* ASP.NET Core Web API
* Receives HTTP requests
* Enforces rate limits
* Publishes messages to Kafka

### MatchMaking.Worker
* Background worker (typically run in 2 instances)
* Consumes messages from Kafka
* Groups players into matches using Redis
* Stores match results

### Infrastructure
* **Kafka** – Message broker for reliable event handling
* **Zookeeper** – Kafka coordination and metadata
* **Redis** – Shared in-memory store for lobby and match states

## How to Run

### Prerequisites
* Docker Desktop installed and running

### 1. Clone & Start

```bash
docker-compose up --build
```

You might need to wait for several minutes for everything to start properly.

The .NET containers wait for Kafka to become healthy before starting. Look for `Worker started` logs.

### 2. Access the API
* Base URL: `http://localhost:5000`

## API Usage (CURL Examples)
Match size by default: **3 players**.

### 1. Queue a User
Immediately adds a user to the matchmaking queue.

```bash
curl --location 'http://localhost:5000/match/search' \
--header 'Content-Type: application/json' \
--data '{
    "userId": "user_123"
}'
```

### 2. Check Match Status
Returns:
* **404** → User still waiting
* **200** → Match found

```bash
curl --location 'http://localhost:5000/match/status?userId=user_123'
```

## Simulation: Create a Full Match
Queue 3 different players. Please note that after queueing a third player, it might take some time before the match is created.


### Player A
```bash
curl --location 'http://localhost:5000/match/search' \
--header 'Content-Type: application/json' \
--data '{ "userId": "Player_A" }'
```

### Player B
```bash
curl --location 'http://localhost:5000/match/search' \
--header 'Content-Type: application/json' \
--data '{ "userId": "Player_B" }'
```

### Player C (Triggers Match)
```bash
curl --location 'http://localhost:5000/match/search' \
--header 'Content-Type: application/json' \
--data '{ "userId": "Player_C" }'
```

### Retrieve Result
```bash
curl --location 'http://localhost:5000/match/status?userId=Player_A'
```

Expected JSON:

```json
{
  "matchId": "a1b2c3d4-e5f6-7890-1234-56789abcdef0",
  "userIds": ["Player_A", "Player_B", "Player_C"]
}
```

Please note that after queueing a third player, it might take some time before the match is created.

## Configuration
Configuration controlled via `docker-compose.yml`.

### Service Port
* Default: **5000**

### Match Settings
| Setting | Default | Description |
|--------|---------|-------------|
| MatchSize | 3 | Number of players needed to form a match |

### Rate Limit
* **1 request per 100ms** (in Program.cs)
* Exceeding this returns **503 Service Unavailable**

## Troubleshooting

### 1. Address Already in Use
Port 5000 may conflict.

Change in `docker-compose.yml`:
```yaml
ports:
  - "5001:8080"
```

### 2. Duplicate Matches or Unexpected Behavior
Redis may contain stale data.

Reset by clearing volumes:
```bash
docker-compose down -v
docker-compose up --build
```
