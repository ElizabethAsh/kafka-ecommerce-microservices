# Task 2 - Event-Driven E-Commerce System

## 1. Student Information
**Full Name:** Elizabeth Ashurov  
**ID Number:** 209449321

**Full Name:** Yael Yakobovich  
**ID Number:** 209534551

---

## 2. Topic Names

| Topic Name | Purpose | Used By |
|------------|---------|---------|
| **order-events** | Main event topic containing all order events (CREATE and UPDATE) using Avro Union schema for type-safe event processing | Producer (publishes) / Consumer (consumes) |
| **order-events-dlq** | Dead Letter Queue for failed messages after 3 retries with exponential backoff. Contains enriched metadata for debugging | Consumer (publishes failed messages and monitors) |

---

## 3. Message Key

**Key:** `orderId` (Order ID string)

**Why:**
- Kafka guarantees message ordering within a partition
- All events for the same order go to the same partition
- Ensures order creation is processed before status updates
- Prevents race conditions between concurrent updates to the same order

---

## 4. Error Handling

**Note:** Errors are handled at multiple layers (Producer, Consumer, Kafka level) to ensure data integrity, fault isolation, and continued system availability without message loss.

### Dead Letter Queue (DLQ)
- Failed messages sent to `order-events-dlq` after 3 retries
- Exponential backoff: 2s → 5s → 10s
- Enriched with error metadata, timestamps, original offset
- Monitoring via `/dlq/stats` and `/dlq/messages` endpoints

**Why:** Prevents data loss, allows investigation, doesn't block processing

### Producer Error Handling
- **400 Bad Request**: Data/serialization errors (schema mismatch, invalid data)
- **503 Service Unavailable**: Transient network/broker errors (retryable)
- **500 Internal Server Error**: Unexpected errors

**Why:** Proper HTTP semantics help clients handle failures correctly

### Consumer Validation
- **Duplicate detection**: Existing orderIds with CREATE event → DLQ
- **Invalid UPDATE**: UPDATE event for non-existent orderId → DLQ
- **Deserialization errors**: Malformed Avro messages → DLQ immediately

**Why:** Data integrity, invalid events isolated in DLQ for manual review

### Topic Pre-Creation
- Explicit topic creation using `AdminClient` on startup
- Controlled partitions (3), replication (1), and retention (7 days)
- No reliance on auto-creation

**Why:** Production-ready configuration, no surprises

---

## 5. API Endpoints

### CartService (Producer) - Port 8000
- **POST** `/create-order` - Create new order with random items
  ```bash
  {"orderId": "ORDER-001", "itemsCount": 3}
  ```
- **PUT** `/update-order` - Update order status (status-only update)
  ```bash
  {"orderId": "ORDER-001", "status": "shipped"}
  ```
- **GET** `/health` - Health check

### OrderService (Consumer) - Port 8001
- **GET** `/order-details?orderId=ORDER-001` - Get order details with shipping cost
- **GET** `/getAllOrderIdsFromTopic?topic=order-events` - Get all orderIds from a topic
- **GET** `/dlq/stats` - DLQ statistics (total messages, error breakdown)
- **GET** `/dlq/messages` - View DLQ messages with error details
- **GET** `/health` - Health check

---

## 6. Architecture Highlights

- **Avro True Union Schema** with Schema Registry
  - `CreateOrderPayload`: Full order data (customerId, items, totalAmount, etc.)
  - `UpdateOrderPayload`: Status only (efficient updates, ~95% smaller)
  - No null fields, no data duplication, type-safe
- **Single topic** (`order-events`) with `eventType` field for routing
- **Stateless producer**: No database, just publishes events
- **Stateful consumer**: Single source of truth for order data
- **Manual offset management**: At-least-once delivery guarantee
- **Comprehensive error handling** at all layers (producer, consumer, DLQ)

---

## 7. Running the System

```bash
docker-compose -f docker-compose.yml up -d
```

Services:
- CartService (Producer): http://localhost:8000
- OrderService (Consumer): http://localhost:8001
- Kafka: localhost:9092
- Schema Registry: localhost:8081

**Docker Hub Images:**
- `elizabeth496/task2-producer:latest`
- `elizabeth496/task2-consumer:latest`

---

## 8. Testing

```bash
# 1. Create first order
curl -X POST http://localhost:8000/create-order \
  -H "Content-Type: application/json" \
  -d '{"orderId": "ORDER-001", "itemsCount": 3}'

# 2. Create second order
curl -X POST http://localhost:8000/create-order \
  -H "Content-Type: application/json" \
  -d '{"orderId": "ORDER-002", "itemsCount": 5}'

# 3. Get order details
curl "http://localhost:8001/order-details?orderId=ORDER-001"
curl "http://localhost:8001/order-details?orderId=ORDER-002"

# 4. Get all orders from topic
curl "http://localhost:8001/getAllOrderIdsFromTopic?topic=order-events"

# 5. Update order status
curl -X PUT http://localhost:8000/update-order \
  -H "Content-Type: application/json" \
  -d '{"orderId": "ORDER-001", "status": "shipped"}'

# 6. Verify update
curl "http://localhost:8001/order-details?orderId=ORDER-001"

# 7. Test missing required field (should return 422)
curl -X POST http://localhost:8000/create-order \
  -H "Content-Type: application/json" \
  -d '{"itemsCount": 3}'

# 8. Test update non-existent order (goes to DLQ)
curl -X PUT http://localhost:8000/update-order \
  -H "Content-Type: application/json" \
  -d '{"orderId": "ORDER-NONEXISTENT", "status": "confirmed"}'

# 9. Test duplicate CREATE (goes to DLQ)
curl -X POST http://localhost:8000/create-order \
  -H "Content-Type: application/json" \
  -d '{"orderId": "ORDER-001", "itemsCount": 2}'

# 10. Check DLQ (wait 5 seconds after error tests)
curl http://localhost:8001/dlq/stats
curl http://localhost:8001/dlq/messages
curl "http://localhost:8001/dlq/messages?limit=1&offset=0"

# 12. Health checks
curl http://localhost:8000/health
curl http://localhost:8001/health
```

---

