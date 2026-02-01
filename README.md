# Task 2: Event-Driven E-Commerce Platform with Kafka and Avro

## Student Information
- Elizabeth Ashurov 209449321
- Yael Yakobovich 209534551

---

## 2. Service Names and Their Purpose

### **Producer Service: Cart Service**
- **Port**: `8000`
- **Purpose**: 
  - Accepts user requests to create new orders
  - Accepts user requests to update existing order status
  - Publishes order events to Kafka topic
  - Acts as a **stateless event publisher** - does not store orders
  - Validates input data and serializes messages using Avro schema

### **Consumer Service: Order Service**
- **Port**: `8001`
- **Purpose**:
  - Consumes order events from Kafka topic
  - Validates order lifecycle (detects duplicate creates, invalid updates)
  - Stores orders with enriched data (shipping cost) in memory
  - Provides API endpoints to query order details
  - Manages Dead Letter Queue (DLQ) for failed messages
  - Acts as the **single source of truth** for order state

---

## 3. Topic Names and Their Purpose

The system uses **2 Kafka topics**:

### **1. `order-events`**
- **Purpose**: Main topic for all order-related events
- **Used by**: 
  - **Producer**: Publishes all order events (both CREATE and UPDATE operations)
  - **Consumer**: Subscribes to receive and process all order events
- **Configuration**:
  - Partitions: 3 (for parallelism)
  - Replication Factor: 1
  - Retention: 7 days (604800000 ms)
  - Message Key: `orderId` (ensures ordering per order)
- **Message Format**: Uses **Avro Union Schema** with two event types:
  - **CREATE**: Contains full order data (customerId, items, totalAmount, etc.)
  - **UPDATE**: Contains only orderId and new status (efficient updates)

### **2. `order-events-dlq`**
- **Purpose**: Dead Letter Queue for failed messages
- **Used by**:
  - **Consumer**: Publishes messages that cannot be processed successfully
  - **Consumer API**: Provides endpoints to query DLQ messages (`/dlq/stats`, `/dlq/messages`)
- **Configuration**:
  - Partitions: 1 (sequential processing of errors)
  - Replication Factor: 1
  - Retention: 30 days (2592000000 ms) - longer than main topic for error analysis
  - Message Key: `orderId`
- **Contains**: Failed messages with rich metadata (error reason, retry count, original offset, timestamp)

---

## 4. The Key Used in Messages and Why

### **Key Used: `orderId` (as UTF-8 encoded string)**

### **Why `orderId` is Used as the Message Key:**

**1. Ordering Guarantee**
- Kafka guarantees that messages with the same key are processed in order within a partition
- All events for the same `orderId` are processed sequentially (CREATE → UPDATE → UPDATE)
- This ensures updates are processed in the correct sequence

**2. Partition Affinity**
- Messages with the same `orderId` always go to the same partition using: `partition = hash(key) % num_partitions`
- Ensures consistent routing for all lifecycle events of an order

**3. Duplicate Detection**
- Consumer can reliably detect duplicate CREATE requests for the same `orderId`
- Since all events for an order go to the same consumer instance (via partition assignment), validation is consistent

---

## 5. Avro True Union Schema Design

The system uses **Avro True Union Schema** to elegantly handle different event types (CREATE and UPDATE) in the same topic with maximum efficiency.

### **Schema Structure:**
```json
{
  "eventType": "CREATE" or "UPDATE",
  "orderId": "ORDER-123",
  "timestamp": "2026-01-30T15:00:00Z",
  "payload": {
    "CreateOrderPayload": { /* full order data */ }
    OR
    "UpdateOrderPayload": { /* status only */ }
  }
}
```

### **Key Design Principles:**
✅ **No Data Duplication**: `orderId` appears only at top level (not inside payload)  
✅ **No Null Fields**: Union ensures only relevant payload is present (no unused fields)  
✅ **Event Timestamp**: Every event has a timestamp for audit trail  
✅ **Type Safety**: Avro enforces structure at serialization time  
✅ **Efficiency**: UPDATE events are ~95% smaller (only status vs. full order)  
✅ **Extensibility**: Easy to add new event types (e.g., DELETE, CANCEL)  

### **CREATE Event Example:**
```json
{
  "eventType": "CREATE",
  "orderId": "ORDER-001",
  "timestamp": "2026-01-30T15:00:00Z",
  "payload": {
    "CreateOrderPayload": {
      "customerId": "CUST-123",
      "orderDate": "2026-01-30T15:00:00Z",
      "items": [
        {
          "itemId": "ITEM-456",
          "quantity": 2,
          "price": 49.99
        }
      ],
      "totalAmount": 99.98,
      "currency": "USD",
      "status": "new"
    }
  }
}
```
**Size**: ~500 bytes  
**Note**: No `UpdateOrderPayload` field - union means only one type present

### **UPDATE Event Example:**
```json
{
  "eventType": "UPDATE",
  "orderId": "ORDER-001",
  "timestamp": "2026-01-30T16:00:00Z",
  "payload": {
    "UpdateOrderPayload": {
      "status": "shipped"
    }
  }
}
```
**Size**: ~50 bytes (90% smaller than CREATE!)  
**Note**: No `CreateOrderPayload` field - true union efficiency

---

## 6. Error Handling Approaches

The implementation uses a **multi-layered error handling strategy** with 7 distinct approaches:

### **6.1 Retry Logic with Exponential Backoff**

**Purpose**: Handle transient failures (network issues, broker temporarily down)

**Implementation:**
```python
# Consumer: Retry configuration
MAX_RETRIES = 3
RETRY_DELAYS = [2, 5, 10]  # Exponential backoff in seconds

# Producer: Transient error detection
if error_code in (KafkaError._MSG_TIMED_OUT, KafkaError.REQUEST_TIMED_OUT, 
                 KafkaError._TRANSPORT, KafkaError.NETWORK_EXCEPTION):
    raise HTTPException(status_code=503, detail="Service Unavailable...")
```

**Why This Approach:**
- Network issues are often **temporary** (broker restart, network congestion)
- Exponential backoff prevents **overwhelming** the failing service
- HTTP 503 signals to client that **retry is appropriate**
- Automatic recovery without manual intervention

**Example Scenario:**
```
Attempt 1: Kafka broker unreachable → Wait 2 seconds
Attempt 2: Still failing → Wait 5 seconds
Attempt 3: Still failing → Wait 10 seconds
Attempt 4: Success! ✅
```

---

### **6.2 Try-Catch Blocks for Expected Exceptions**

**Purpose**: Handle known failure modes gracefully (schema errors, serialization issues)

**Implementation:**
```python
# Producer: Serialization errors
try:
    serialized_value = avro_serializer(order, SerializationContext(ORDER_TOPIC, MessageField.VALUE))
except (TypeError, ValueError, KeyError) as serialization_error:
    raise HTTPException(status_code=400, detail=f"Bad Request: {serialization_error}")

# Consumer: Deserialization errors
try:
    order_data = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
except Exception as deserialize_err:
    send_to_dlq(msg, ErrorCategory.NO_RETRY_DLQ, f"Deserialization failed: {deserialize_err}")
    kafka_consumer.commit(message=msg)
    continue  # Process next message
```

**Why This Approach:**
- **Fail fast** for data quality issues (schema mismatch)
- **HTTP 400** indicates client error - they should fix their data
- Consumer **doesn't crash** on bad message - continues processing
- Offset is **committed** to avoid infinite reprocessing
- **Graceful degradation** - one bad message doesn't stop the system

---

### **6.3 Dead Letter Queue (DLQ) Pattern**

**Purpose**: Preserve messages that cannot be processed for later analysis/retry

**Implementation:**
```python
# Error Categories
class ErrorCategory(str, Enum):
    NO_RETRY_DLQ = "NO_RETRY_DLQ"           # Immediate to DLQ (duplicate CREATE)
    RETRY_THEN_DLQ = "RETRY_THEN_DLQ"       # Retry first, then DLQ (processing errors)
    RETRY_FOREVER = "RETRY_FOREVER"          # Infrastructure errors

# Send to DLQ with rich metadata
def send_to_dlq(original_message, error_category, error_message, retry_count):
    dlq_message = {
        "original_value": base64.b64encode(original_message.value()).decode('utf-8'),
        "orderId": order_id,
        "error_category": error_category,
        "error_message": error_message,
        "retry_count": retry_count,
        "timestamp": datetime.utcnow().isoformat(),
        "original_topic": original_message.topic(),
        "original_partition": original_message.partition(),
        "original_offset": original_message.offset()
    }
```

**Why This Approach:**
- **No data loss** - failed messages are preserved
- **Rich context** - we know exactly why and when it failed
- **Operational visibility** - ops team can monitor DLQ
- **Different error handling** - some errors need human intervention
- **Audit trail** - can analyze patterns in failures

**Use Cases:**
1. **Duplicate CREATE** (`NO_RETRY_DLQ`) - Likely user error, needs manual review
2. **Invalid UPDATE for non-existent order** (`NO_RETRY_DLQ`) - Order might have been deleted
3. **Processing failure after retries** (`RETRY_THEN_DLQ`) - Temporary issue became permanent
4. **Deserialization errors** (`NO_RETRY_DLQ`) - Data corruption or schema evolution issue

**DLQ Monitoring:**
```bash
# Check DLQ statistics
GET http://localhost:8001/dlq/stats

# View failed messages
GET http://localhost:8001/dlq/messages?limit=10
```

---

### **6.4 Input Validation**

**Purpose**: Prevent invalid data from entering the system

**Implementation:**
```python
# Custom validation error handler
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    error_messages = []
    for error in exc.errors():
        field_name = error["loc"][-1]
        if error_type == "missing":
            error_messages.append(f"Missing required field: '{field_name}'")
        elif error_type.startswith("type_error"):
            error_messages.append(f"Type error: Field '{field_name}' must be...")
    return JSONResponse(status_code=400, content={"errors": error_messages})

# Business validation
def validate_create_request(body: CreateOrderRequest):
    if not body.orderId.strip():
        raise HTTPException(status_code=400, detail="orderId cannot be empty")
    if body.itemsCount <= 0:
        raise HTTPException(status_code=400, detail="itemsCount must be positive")
        
def validate_update_request(body: UpdateOrderRequest):
    if body.status.lower() == "new":
        raise HTTPException(status_code=400, 
            detail="status='new' is reserved for creation. Use 'pending', 'confirmed', etc.")
```

**Why This Approach:**
- **Fail fast** at API boundary - don't process invalid data
- **Clear error messages** - user knows exactly what's wrong
- **Prevents cascading failures** - bad data doesn't reach Kafka
- **Reduces load** - invalid requests rejected immediately
- **Better UX** - user gets immediate feedback

**Validation Layers:**
1. **Pydantic models** - Type validation (string, int, etc.)
2. **Custom validators** - Business rules (positive numbers, non-empty strings)
3. **Status validation** - Prevent `status="new"` in updates
4. **Avro schema** - Final validation before serialization

---

### **6.5 HTTP Status Code Differentiation**

**Purpose**: Communicate error type clearly to API consumers

**Implementation:**
```python
# 400 Bad Request - Client error (fix your data)
raise HTTPException(status_code=400, detail="Invalid orderId")

# 404 Not Found - Resource doesn't exist
raise HTTPException(status_code=404, detail=f"Order '{orderId}' not found")

# 500 Internal Server Error - Unexpected server error
raise HTTPException(status_code=500, detail="Internal error")

# 503 Service Unavailable - Temporary issue (retry recommended)
raise HTTPException(status_code=503, detail="Kafka broker unreachable")
```

**Why This Approach:**
- **Standard HTTP semantics** - clients understand immediately
- **Retry logic** - 5xx → retry, 4xx → don't retry
- **Debugging** - status code indicates error category
- **Monitoring** - ops can alert on 5xx rates

**Status Code Strategy:**
| Code | Meaning | When Used | Client Action |
|------|---------|-----------|---------------|
| 201 | Created | Order created successfully | Continue |
| 400 | Bad Request | Invalid input, schema mismatch, validation failure | Fix data and retry |
| 404 | Not Found | Order not found (consumer only) | Verify orderId |
| 500 | Internal Error | Unexpected exceptions, Kafka errors | Report to ops |
| 503 | Service Unavailable | Kafka down, Schema Registry unreachable | Retry after delay |

---

### **6.6 Comprehensive Logging**

**Purpose**: Debugging, monitoring, and audit trail

**Implementation:**
```python
# Success logging
print(f"[Producer] Successfully loaded Avro schema from {SCHEMA_FILE}")
print(f"[Producer] Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
print(f"[Consumer] Order {order_id} processed successfully with shipping cost ${shipping}")

# Error logging
print(f"[Producer] Serialization error: {serialization_error}")
print(f"[Consumer] Duplicate CREATE detected: {order_id} already exists")
print(f"[Consumer] Invalid UPDATE detected: {order_id} doesn't exist")

# Detailed event logging
print(f"[Consumer] Received order event:")
print(f"  Order ID: {order_id}")
print(f"  Status: {order_data.get('status')}")
print(f"  Total Amount: ${order_total}")
print(f"  Partition: {msg.partition()}, Offset: {msg.offset()}")
```

**Why This Approach:**
- **Production debugging** - trace message flow through system
- **Performance analysis** - identify slow operations
- **Security audit** - who did what and when
- **Business intelligence** - order patterns, failure rates
- **Prefix tags** `[Producer]`/`[Consumer]` - easy to filter logs

**Log Aggregation (Production):**
```bash
# View producer logs
docker logs cart-service-producer | grep "[Producer]"

# View consumer logs  
docker logs order-service-consumer | grep "[Consumer]"

# Monitor DLQ events
docker logs order-service-consumer | grep "DLQ"
```

---

### **6.7 Graceful Degradation**

**Purpose**: System continues operating even when individual components fail

**Implementation:**

**Consumer Loop Resilience:**
```python
def process_order_message():
    while True:  # Infinite loop - never stop consuming
        try:
            msg = kafka_consumer.poll(timeout=1.0)
            # Process message...
            
        except KafkaException as kafka_err:
            print(f"[Consumer] Kafka error: {kafka_err}. Will retry...")
            time.sleep(2)
            # Reinitialize consumer
            kafka_consumer = None
            
        except Exception as err:
            print(f"[Consumer] Error: {err}. Will retry...")
            time.sleep(2)
            # Continue processing next message
```

**Offset Management:**
```python
# Commit offset after successful processing OR DLQ
if processed_successfully:
    kafka_consumer.commit(message=msg)
else:
    # Message sent to DLQ, commit to move forward
    kafka_consumer.commit(message=msg)
```

**Thread Safety:**
```python
# Protect shared state with locks
with db_lock:
    order_database[order_id] = order_with_shipping
    topic_order_ids[topic_name].add(order_id)
```

**Stateless Producer:**
```python
# Producer doesn't store orders - can restart anytime
# No state = no data loss on restart
def create_order(body: CreateOrderRequest):
    order = generate_random_order(...)
    publish_order_event(order)  # Just publish, don't store
    return {"message": "Published successfully"}
```

**Why This Approach:**
- **One failure doesn't cascade** - bad message sent to DLQ, next message processed
- **Service isolation** - producer and consumer failures don't affect each other
- **Automatic recovery** - consumer reconnects automatically
- **No stuck consumers** - offset committed even for failed messages (after DLQ)
- **Thread safety** - prevents race conditions in concurrent environment
- **Stateless design** - producer can scale horizontally without state sync

**Degradation Scenarios:**

| Failure | System Behavior | Recovery |
|---------|-----------------|----------|
| Bad message | Sent to DLQ, next message processed | ✅ Automatic |
| Kafka broker down | Retry with backoff, return 503 | ✅ Automatic when broker up |
| Schema Registry down | Return 503, cached schema used if available | ✅ Automatic |
| Consumer crash | Offset preserved, resume on restart | ✅ Automatic |
| Producer crash | No state lost (stateless), restart immediately | ✅ Automatic |

---

## 7. Architecture Overview

### **Event Flow Diagram:**
```
┌─────────────────┐
│  API Client     │
└────────┬────────┘
         │ POST /create-order {"orderId": "123", "itemsCount": 3}
         ↓
┌─────────────────────────────────────────────────────┐
│  Producer (Cart Service) - Port 8000                │
│  - Validates input                                  │
│  - Generates random order data                      │
│  - Serializes with Avro                             │
│  - Publishes to Kafka (key=orderId)                 │
│  - STATELESS (no storage)                           │
└────────┬────────────────────────────────────────────┘
         │
         ↓
┌─────────────────────────────────────────────────────┐
│  Kafka Topic: order-events                          │
│  - 3 partitions (for parallelism)                   │
│  - Key: orderId (ensures ordering)                  │
│  - Retention: 7 days                                │
│  - Replication: 1 (single broker)                   │
└────────┬────────────────────────────────────────────┘
         │
         ↓
┌─────────────────────────────────────────────────────┐
│  Consumer (Order Service) - Port 8001               │
│  - Deserializes with Avro                           │
│  - Validates order lifecycle                        │
│    • Detects duplicate CREATE (→ DLQ)               │
│    • Detects invalid UPDATE (→ DLQ)                 │
│  - Calculates shipping (2% of total)                │
│  - Stores in memory                                 │
│  - STATEFUL (single source of truth)                │
└────────┬────────────────────────────────────────────┘
         │
         ├─────→ Success: Store in order_database
         │
         └─────→ Failure: Send to DLQ
                 ┌──────────────────────────────────┐
                 │  DLQ Topic: order-events-dlq     │
                 │  - 1 partition                   │
                 │  - Retention: 30 days            │
                 │  - Rich error metadata           │
                 └──────────────────────────────────┘
```

### **Key Design Decisions:**

1. **Single Topic Strategy**
   - All order events (create + update) in `order-events` topic
   - Status field differentiates: `status="new"` → CREATE, other → UPDATE
   - Ensures ordering guarantee via orderId key

2. **Stateless Producer, Stateful Consumer**
   - Producer: Captures user intent, publishes events
   - Consumer: Enforces business rules, maintains state
   - Clear separation of concerns

3. **Placeholder Pattern for Updates**
   - Update-order only sends `orderId` + `status`
   - Producer creates Avro-compliant message with placeholders
   - Consumer detects placeholders and merges with existing order
   - Efficient: small messages for updates

4. **DLQ with Error Categories**
   - `NO_RETRY_DLQ`: Immediate (duplicates, validation errors)
   - `RETRY_THEN_DLQ`: After 3 retries with exponential backoff
   - Rich metadata for debugging

---

## 8. API Endpoints

### **Producer (Cart Service) - Port 8000**

#### POST `/create-order`
Create a new order and publish to Kafka.
  ```json
Request:
  {
  "orderId": "ORDER-001",
    "itemsCount": 3
  }

Response (201):
{
  "message": "Order creation event published successfully",
  "order": { /* full order with generated data */ }
}
```

#### PUT `/update-order`
Update order status (status-only update).
  ```json
Request:
{
  "orderId": "ORDER-001",
  "status": "confirmed"
}

Response (200):
{
  "message": "Order status update event published successfully",
  "update": {
    "orderId": "ORDER-001",
    "status": "confirmed"
  }
}

Errors:
- 400: status="new" not allowed (reserved for create)
- 400: empty orderId or status
```

#### GET `/health`
Health check endpoint.
  ```json
Response (200):
  {
    "status": "healthy",
    "kafka": "connected"
  }
  ```

---

### **Consumer (Order Service) - Port 8001**

#### GET `/order-details?orderId=ORDER-001`
Retrieve order details with shipping cost.
  ```json
Response (200):
  {
  "orderId": "ORDER-001",
    "customerId": "CUST-ABC123",
  "orderDate": "2026-01-30T10:30:00Z",
  "items": [
    {
      "itemId": "ITEM-XYZ789",
      "quantity": 2,
      "price": 49.99
    }
  ],
  "totalAmount": 99.98,
    "currency": "USD",
  "status": "confirmed",
  "shippingCost": 2.00
}

Errors:
- 404: Order not found
```

#### GET `/getAllOrderIdsFromTopic?topic=order-events`
Get all order IDs successfully processed from a topic.
```json
Response (200):
{
  "topic": "order-events",
  "orderIds": ["ORDER-001", "ORDER-002", "ORDER-003"],
  "count": 3
}

Note: Only includes successfully processed orders (not DLQ'd messages)
```

#### GET `/dlq/stats`
Get Dead Letter Queue statistics.
```json
Response (200):
{
  "total_messages": 5,
  "error_categories": {
    "NO_RETRY_DLQ": 3,
    "RETRY_THEN_DLQ": 2
  },
  "topic": "order-events-dlq"
}
```

#### GET `/dlq/messages?limit=10&offset=0`
Retrieve messages from the DLQ.
  ```json
Response (200):
{
  "messages": [
    {
      "orderId": "ORDER-DUPLICATE",
      "error_category": "NO_RETRY_DLQ",
      "error_message": "Duplicate CREATE request: Order with ID 'ORDER-DUPLICATE' already exists...",
      "retry_count": 0,
      "timestamp": "2026-01-30T15:30:00Z",
      "original_topic": "order-events",
      "original_partition": 0,
      "original_offset": 123,
      "original_value": "base64_encoded_message"
    }
  ],
  "count": 1,
  "limit": 10,
  "offset": 0
}
```

#### GET `/health`
Health check endpoint.
  ```json
Response (200):
  {
    "status": "healthy",
  "consumer": "running",
  "orders_in_memory": 42
  }
  ```

---

## 9. Testing Scenarios

### **Scenario 1: Happy Path - Create and Update**
```bash
# 1. Create order
curl -X POST http://localhost:8000/create-order \
  -H "Content-Type: application/json" \
  -d '{"orderId": "ORDER-001", "itemsCount": 3}'

# 2. Wait 2 seconds for consumer to process

# 3. Get order details (should include shippingCost)
curl http://localhost:8001/order-details?orderId=ORDER-001

# 4. Update order status
curl -X PUT http://localhost:8000/update-order \
  -H "Content-Type: application/json" \
  -d '{"orderId": "ORDER-001", "status": "confirmed"}'

# 5. Get order details again (status should be updated)
curl http://localhost:8001/order-details?orderId=ORDER-001
```

### **Scenario 2: Duplicate CREATE (DLQ)**
```bash
# 1. Create order
curl -X POST http://localhost:8000/create-order \
  -H "Content-Type: application/json" \
  -d '{"orderId": "ORDER-DUP", "itemsCount": 2}'

# 2. Try to create same order again (producer accepts, consumer sends to DLQ)
curl -X POST http://localhost:8000/create-order \
  -H "Content-Type: application/json" \
  -d '{"orderId": "ORDER-DUP", "itemsCount": 5}'

# 3. Check DLQ stats
curl http://localhost:8001/dlq/stats

# 4. View DLQ messages
curl http://localhost:8001/dlq/messages?limit=10
```

### **Scenario 3: Invalid UPDATE (DLQ)**
```bash
# 1. Try to update non-existent order (producer accepts, consumer sends to DLQ)
curl -X PUT http://localhost:8000/update-order \
  -H "Content-Type: application/json" \
  -d '{"orderId": "ORDER-NONEXISTENT", "status": "shipped"}'

# 2. Check DLQ stats
curl http://localhost:8001/dlq/stats

# 3. View DLQ messages
curl http://localhost:8001/dlq/messages?limit=10
```

### **Scenario 4: Validation Errors**
```bash
# Missing orderId (400 Bad Request)
curl -X POST http://localhost:8000/create-order \
  -H "Content-Type: application/json" \
  -d '{"itemsCount": 3}'

# Invalid itemsCount (400 Bad Request)
curl -X POST http://localhost:8000/create-order \
  -H "Content-Type: application/json" \
  -d '{"orderId": "ORDER-999", "itemsCount": -1}'

# status="new" in update (400 Bad Request)
curl -X PUT http://localhost:8000/update-order \
  -H "Content-Type: application/json" \
  -d '{"orderId": "ORDER-001", "status": "new"}'
```

---

## 10. Running the Application

### **Prerequisites**
- Docker and Docker Compose installed
- Ports 8000, 8001, 8081, 9092 available

### **Start All Services**
```bash
cd /Users/eashurov/projects/event-driven-course/task2

# Start all services (Kafka, Zookeeper, Schema Registry, Producer, Consumer)
docker-compose up --build -d

# Check logs
docker-compose logs -f producer
docker-compose logs -f consumer

# Check service health
curl http://localhost:8000/health  # Producer
curl http://localhost:8001/health  # Consumer
```

### **Stop Services**
```bash
docker-compose down
```

### **Rebuild After Code Changes**
```bash
docker-compose down
docker-compose up --build -d
```

---

## 11. Technology Stack

- **Language**: Python 3.11
- **Framework**: FastAPI (async web framework)
- **Message Broker**: Apache Kafka 7.5.0
- **Schema Registry**: Confluent Schema Registry 7.5.0
- **Serialization**: Avro (with Schema Registry integration)
- **Kafka Client**: confluent-kafka-python
- **Containerization**: Docker & Docker Compose

---

## 12. Project Structure

```
task2/
├── producer/
│   ├── app.py              # Cart Service (stateless producer)
│   ├── order.avsc          # Avro schema definition
│   ├── requirements.txt    # Python dependencies
│   └── Dockerfile          # Container configuration
├── consumer/
│   ├── app.py              # Order Service (stateful consumer)
│   ├── order.avsc          # Avro schema definition (same as producer)
│   ├── requirements.txt    # Python dependencies
│   └── Dockerfile          # Container configuration
├── docker-compose.yml      # Multi-service orchestration
└── README.md              # This file
```

---

## 13. Key Learnings and Best Practices

### **Event-Driven Architecture**
✅ **Decoupling**: Producer and consumer are independent services  
✅ **Scalability**: Can scale services independently  
✅ **Resilience**: Failure in one service doesn't affect the other  
✅ **Async Processing**: Producer responds immediately, processing happens asynchronously

### **Kafka Best Practices**
✅ **Message Key**: Use business entity ID (orderId) for ordering guarantee  
✅ **Partitioning**: Balance between parallelism and ordering  
✅ **Offset Management**: Manual commit for exactly-once processing  
✅ **Topic Pre-Creation**: Explicit topic creation with AdminClient  
✅ **Replication**: Configure based on durability requirements

### **Error Handling Philosophy**
✅ **Fail Fast**: Validate early, reject bad data at API boundary  
✅ **Graceful Degradation**: One failure doesn't stop the system  
✅ **Observability**: Log everything, use structured logging  
✅ **Dead Letter Queue**: Never lose data, preserve for analysis  
✅ **Differentiated Errors**: Use HTTP codes to communicate error type

### **Avro Schema Registry**
✅ **Schema Evolution**: Centralized schema management  
✅ **Type Safety**: Compile-time validation of message structure  
✅ **Documentation**: Schema serves as contract between services  
✅ **Efficiency**: Binary serialization is compact

---

## 14. Future Improvements

If this were a production system, consider:

1. **Persistent Storage**: Replace in-memory storage with PostgreSQL/MongoDB
2. **Schema Evolution**: Implement backward/forward compatible schema changes
3. **Monitoring**: Add Prometheus metrics, Grafana dashboards
4. **Distributed Tracing**: Add OpenTelemetry for request tracing
5. **Authentication**: Add API key or OAuth2 authentication
6. **Rate Limiting**: Prevent API abuse
7. **DLQ Replay**: Ability to reprocess DLQ messages
8. **Multi-Region**: Deploy across regions for high availability
9. **CDC (Change Data Capture)**: Sync database changes to Kafka
10. **CQRS**: Separate read and write models for better scalability

---

## Conclusion

This project demonstrates a production-ready event-driven architecture with:
- ✅ **Strong ordering guarantees** via message keys
- ✅ **Comprehensive error handling** with 7 distinct strategies
- ✅ **Data preservation** via Dead Letter Queue
- ✅ **Stateless producer** and **stateful consumer** pattern
- ✅ **Schema-first** approach with Avro
- ✅ **Production best practices** throughout

The system is resilient, observable, and maintainable - ready for real-world workloads.
