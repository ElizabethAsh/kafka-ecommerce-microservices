# Task 2: Event-Driven E-Commerce Platform with Kafka and Avro

## Student Information
- **Full Name**: [REPLACE WITH YOUR NAME]
- **ID Number**: [REPLACE WITH YOUR ID]

---

## Project Overview

This project implements a simplified backend system for an e-commerce platform using Apache Kafka for event streaming and Avro schema registry for message serialization. The system consists of two main services:

1. **Cart Service (Producer)**: Creates and updates orders, publishing events to Kafka
2. **Order Service (Consumer)**: Consumes order events from Kafka, calculates shipping costs, and provides order details

---

## API Endpoints

### Producer Application (Cart Service)

#### 1. Create Order
- **URL**: `http://localhost:8000/create-order`
- **Method**: `POST`
- **Request Body**:
  ```json
  {
    "orderId": "ORDER-123",
    "itemsCount": 3
  }
  ```
- **Response**: 201 Created
  ```json
  {
    "message": "Order created and published successfully.",
    "order": { ... }
  }
  ```

#### 2. Update Order
- **URL**: `http://localhost:8000/update-order`
- **Method**: `PUT`
- **Request Body**:
  ```json
  {
    "orderId": "ORDER-123",
    "status": "confirmed"
  }
  ```
- **Response**: 200 OK
  ```json
  {
    "message": "Order updated and published successfully.",
    "order": { ... }
  }
  ```

#### 3. Health Check
- **URL**: `http://localhost:8000/health`
- **Method**: `GET`
- **Response**: 200 OK
  ```json
  {
    "status": "healthy",
    "kafka": "connected"
  }
  ```

### Consumer Application (Order Service)

#### 1. Get Order Details
- **URL**: `http://localhost:8001/order-details?orderId=ORDER-123`
- **Method**: `GET`
- **Query Parameters**: `orderId` (required, string)
- **Response**: 200 OK
  ```json
  {
    "orderId": "ORDER-123",
    "customerId": "CUST-ABC123",
    "orderDate": "2024-12-15T10:30:00Z",
    "items": [ ... ],
    "totalAmount": 150.50,
    "currency": "USD",
    "status": "new",
    "shippingCost": 3.01
  }
  ```

#### 2. Get All Order IDs from Topic
- **URL**: `http://localhost:8001/getAllOrderIdsFromTopic?topic=order-events`
- **Method**: `GET`
- **Query Parameters**: `topic` (required, string)
- **Response**: 200 OK
  ```json
  {
    "topic": "order-events",
    "orderIds": ["ORDER-123", "ORDER-456"],
    "count": 2
  }
  ```

#### 3. Health Check
- **URL**: `http://localhost:8001/health`
- **Method**: `GET`
- **Response**: 200 OK
  ```json
  {
    "status": "healthy",
    "kafka": "connected"
  }
  ```

---

## Topic Names and Purpose

### Topics Used

1. **`order-events`**
   - **Purpose**: Main topic for all order-related events (both create and update operations)
   - **Used by**: 
     - Producer: Publishes all order events (create and update) to this topic
     - Consumer: Subscribes to this topic to receive and process all order events
   - **Partitioning Strategy**: Messages are partitioned by `orderId` (used as message key) to ensure ordering per order

---

## Message Key Strategy

### Key Used: `orderId` (as string)

**Why `orderId` is used as the message key:**

1. **Ordering Guarantee**: Kafka guarantees that messages with the same key are processed in order within a partition. By using `orderId` as the key, we ensure that all events for a specific order (create, update, etc.) are processed in the correct sequence by the consumer.

2. **Consistency**: This is critical for maintaining data consistency across downstream services. For example, if an order is created and then immediately updated, the consumer must process the create event before the update event to maintain correct state.

3. **Partition Assignment**: Messages with the same `orderId` will always be routed to the same partition, ensuring that:
   - All events for an order are processed by the same consumer instance (in a consumer group)
   - Events are processed sequentially per order
   - No race conditions occur when processing updates for the same order

4. **Business Logic Alignment**: Since the requirement states "updates for any given order must be processed in the correct sequence", using `orderId` as the key directly addresses this requirement at the Kafka infrastructure level.

---

## Error Handling Approaches

The implementation uses multiple error handling strategies to ensure robustness and provide clear feedback:

### 1. **Connection Error Handling**

**Approach**: Retry logic with exponential backoff and connection verification

**Implementation**:
- **Producer**: `ensure_kafka_connection()` function checks if producer is connected before publishing. If not, attempts to reconnect.
- **Consumer**: Continuous retry loop in `process_order_message()` that reinitializes the consumer if connection is lost.

**Why**: Kafka brokers may be temporarily unavailable. Automatic reconnection ensures the service remains resilient without manual intervention.

**Error Codes**:
- `503 Service Unavailable`: Returned when Kafka broker or Schema Registry cannot be reached

### 2. **Schema Registry Errors**

**Approach**: Exception handling during serializer/deserializer initialization

**Implementation**:
- Both producer and consumer catch exceptions during schema registry client initialization
- Errors are logged with detailed messages
- HTTP 503 is returned if schema registry is unavailable

**Why**: Schema Registry is critical for Avro serialization. If it's down, the service cannot function, so we fail fast with a clear error message.

### 3. **Message Serialization/Deserialization Errors**

**Approach**: Try-catch blocks with graceful degradation

**Implementation**:
- **Producer**: Catches serialization errors and returns HTTP 500 with detailed error message
- **Consumer**: Catches deserialization errors, logs them, commits the offset (to avoid reprocessing), and continues with the next message

**Why**: 
- Invalid messages should not crash the entire consumer
- Committing offset prevents infinite reprocessing of bad messages
- Logging helps with debugging data quality issues

### 4. **Kafka Consumer Errors**

**Approach**: Specific error code handling for different Kafka error types

**Implementation**:
- `KafkaError._PARTITION_EOF`: Treated as informational, not an error
- `KafkaError.UNKNOWN_TOPIC_OR_PART`: Retries after a delay
- Other errors: Logged and consumer is reinitialized

**Why**: Different Kafka errors require different handling strategies. Some are transient (network issues), others indicate configuration problems.

### 5. **API Validation Errors**

**Approach**: Custom exception handlers with detailed error messages

**Implementation**:
- `RequestValidationError` handler provides field-specific error messages
- Validates required fields, types, and value constraints
- Returns HTTP 400 with structured error response

**Why**: Clear validation errors help API consumers understand what went wrong and how to fix their requests.

### 6. **Business Logic Errors**

**Approach**: HTTP status codes aligned with error type

**Implementation**:
- `404 Not Found`: Order not found when updating or retrieving
- `400 Bad Request`: Invalid input parameters
- `500 Internal Server Error`: Unexpected errors during processing

**Why**: Standard HTTP status codes provide clear semantics for different error conditions, making the API easier to integrate with.

### 7. **Thread Safety**

**Approach**: Locks for shared data structures

**Implementation**:
- `threading.Lock()` used to protect `order_database` and `topic_order_ids` dictionaries
- All read/write operations are wrapped in lock context

**Why**: FastAPI runs in a multi-threaded environment, and the Kafka consumer runs in a separate thread. Without locks, race conditions could corrupt the in-memory database.

### 8. **Producer Delivery Confirmation**

**Approach**: Callback function for message delivery status

**Implementation**:
- `delivery_callback()` function logs delivery success/failure
- Producer flushes messages with timeout to ensure delivery

**Why**: Provides visibility into message delivery status and helps identify issues with Kafka connectivity or topic configuration.

---

## Technology Stack

- **Language**: Python 3.11
- **Framework**: FastAPI
- **Message Broker**: Apache Kafka
- **Schema Registry**: Confluent Schema Registry
- **Serialization**: Avro (with Schema Registry)
- **Containerization**: Docker & Docker Compose

---

## Running the Application

### Prerequisites
- Docker and Docker Compose installed

### Starting the Producer

```bash
cd task2/producer
docker-compose up --build
```

The producer will be available at `http://localhost:8000`

### Starting the Consumer

```bash
cd task2/consumer
docker-compose up --build
```

The consumer will be available at `http://localhost:8001`

**Note**: Both services include Kafka, Zookeeper, and Schema Registry in their docker-compose files. In a production environment, these would typically be shared infrastructure.

---

## Testing the Application

### 1. Create an Order

```bash
curl -X POST http://localhost:8000/create-order \
  -H "Content-Type: application/json" \
  -d '{
    "orderId": "ORDER-001",
    "itemsCount": 3
  }'
```

### 2. Update an Order

```bash
curl -X PUT http://localhost:8000/update-order \
  -H "Content-Type: application/json" \
  -d '{
    "orderId": "ORDER-001",
    "status": "confirmed"
  }'
```

### 3. Get Order Details

```bash
curl http://localhost:8001/order-details?orderId=ORDER-001
```

### 4. Get All Order IDs from Topic

```bash
curl http://localhost:8001/getAllOrderIdsFromTopic?topic=order-events
```

---

## Architecture Decisions

1. **Single Topic for All Events**: Using one topic (`order-events`) for both create and update events simplifies the architecture while maintaining ordering through message keys.

2. **Avro with Schema Registry**: Provides schema evolution capabilities and ensures type safety across services. The bonus requirement is fully implemented.

3. **In-Memory Storage**: Used for simplicity. In production, this would be replaced with a persistent database.

4. **Manual Offset Committing**: Consumer uses manual offset commits to ensure messages are only marked as processed after successful handling.

5. **Idempotent Producer**: Producer is configured with `enable.idempotence=True` to prevent duplicate messages in case of retries.

---

## Notes

- The producer stores orders in memory to support the `/update-order` endpoint
- The consumer calculates shipping costs (2% of total amount) and stores orders with shipping information
- Both services include health check endpoints for monitoring
- Error handling is comprehensive and follows best practices for production systems

