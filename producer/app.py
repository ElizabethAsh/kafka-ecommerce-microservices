import os
import random
import string
import time
import json
from typing import Optional, List
from pathlib import Path

from confluent_kafka import Producer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import Schema
from confluent_kafka.serialization import SerializationContext, MessageField
from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# Load Avro schema from file
SCHEMA_FILE = Path(__file__).parent / "order.avsc"
try:
    with open(SCHEMA_FILE, 'r') as f:
        ORDER_SCHEMA = f.read()
    
    # Validate it's valid JSON
    json.loads(ORDER_SCHEMA)
    
    if not ORDER_SCHEMA.strip():
        raise ValueError("Schema file is empty")
        
    print(f"[Producer] Successfully loaded Avro schema from {SCHEMA_FILE}")
    
except FileNotFoundError:
    raise FileNotFoundError(
        f"Avro schema file not found at {SCHEMA_FILE}. "
        "Make sure order.avsc exists in the same directory as app.py"
    )
except json.JSONDecodeError as e:
    raise ValueError(
        f"Invalid JSON in schema file {SCHEMA_FILE}: {e}. "
        "Please check the schema file format."
    )
except Exception as e:
    raise RuntimeError(
        f"Failed to load Avro schema from {SCHEMA_FILE}: {e}"
    )

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
ORDER_TOPIC = os.getenv("ORDER_TOPIC", "order-events")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "order-events-dlq")

app = FastAPI(title="Cart Service - Producer")

# Global variables for Kafka producer and schema registry
kafka_producer: Optional[Producer] = None
schema_registry_client: Optional[SchemaRegistryClient] = None
avro_serializer: Optional[AvroSerializer] = None

CURRENCIES = ["USD", "EUR", "ILS", "GBP"]


class CreateOrderRequest(BaseModel):
    orderId: str
    itemsCount: int


class UpdateOrderRequest(BaseModel):
    """
    Update order status request.
    The status field must NOT be 'new' (reserved for create operations).
    """
    orderId: str
    status: str


# ---------- CUSTOM ERROR HANDLERS ---------- #
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle validation errors with specific error messages."""
    error_messages = []
    
    for error in exc.errors():
        field_name = error["loc"][-1] if error["loc"] else "request body"
        error_type = error["type"]
        error_msg = error.get("msg", "")
        
        if error_type == "missing":
            error_messages.append(f"Missing required field: '{field_name}'. This field is mandatory.")
        elif error_type.startswith("type_error"):
            if "str" in error_msg.lower():
                error_messages.append(f"Type error: Field '{field_name}' must be a string, but received a different type.")
            elif "int" in error_msg.lower() or "integer" in error_msg.lower():
                error_messages.append(f"Type error: Field '{field_name}' must be an integer, but received a different type.")
            else:
                error_messages.append(f"Type error: Field '{field_name}' has an invalid type. {error_msg}")
        elif error_type == "value_error":
            error_messages.append(f"Invalid value for field '{field_name}': {error_msg}")
        else:
            error_messages.append(f"Validation error for field '{field_name}': {error_msg}")

    if not error_messages:
        error_messages.append("Request body is invalid. Please check the JSON format and field types.")

    return JSONResponse(
        status_code=400,
        content={
            "message": "Invalid request data.",
            "errors": error_messages,
        },
    )


# ---------- KAFKA SETUP ---------- #
def ensure_topic_exists(admin_client: AdminClient, topic_name: str, num_partitions: int = 3, replication_factor: int = 1):
    """
    Ensure the Kafka topic exists. Create it if it doesn't.
    
    Args:
        admin_client: Kafka AdminClient instance
        topic_name: Name of the topic to create/verify
        num_partitions: Number of partitions (default: 3 for parallelism)
        replication_factor: Replication factor (default: 1 for single broker)
    """
    try:
        # Get existing topics
        metadata = admin_client.list_topics(timeout=10)
        existing_topics = metadata.topics
        
        if topic_name in existing_topics:
            print(f"[Producer] Topic '{topic_name}' already exists")
            return
        
        # Topic doesn't exist, create it
        print(f"[Producer] Creating topic '{topic_name}' with {num_partitions} partitions...")
        
        new_topic = NewTopic(
            topic=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            config={
                'retention.ms': '604800000',  # 7 days in milliseconds
                'min.insync.replicas': '1',    # Minimum replicas that must acknowledge
            }
        )
        
        # Create topic (returns a dict of futures)
        futures = admin_client.create_topics([new_topic])
        
        # Wait for topic creation to complete
        for topic, future in futures.items():
            try:
                future.result()  # Block until topic is created
                print(f"[Producer] Topic '{topic}' created successfully")
            except Exception as e:
                print(f"[Producer] Failed to create topic '{topic}': {e}")
                raise
                
    except Exception as e:
        print(f"[Producer] Error ensuring topic exists: {e}")
        raise


def ensure_topic_exists_dlq(admin_client: AdminClient, topic_name: str, num_partitions: int = 1, replication_factor: int = 1):
    """
    Ensure the DLQ Kafka topic exists. Create it with longer retention if it doesn't.
    
    Args:
        admin_client: Kafka AdminClient instance
        topic_name: Name of the DLQ topic to create/verify
        num_partitions: Number of partitions (default: 1, DLQ doesn't need parallelism)
        replication_factor: Replication factor (default: 1 for single broker)
    """
    try:
        # Get existing topics
        metadata = admin_client.list_topics(timeout=10)
        existing_topics = metadata.topics
        
        if topic_name in existing_topics:
            print(f"[Producer] DLQ Topic '{topic_name}' already exists")
            return
        
        # Topic doesn't exist, create it
        print(f"[Producer] Creating DLQ topic '{topic_name}' with {num_partitions} partition(s)...")
        
        new_topic = NewTopic(
            topic=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            config={
                'retention.ms': '2592000000',  # 30 days in milliseconds (longer retention for DLQ)
                'min.insync.replicas': '1',
            }
        )
        
        # Create topic (returns a dict of futures)
        futures = admin_client.create_topics([new_topic])
        
        # Wait for topic creation to complete
        for topic, future in futures.items():
            try:
                future.result()  # Block until topic is created
                print(f"[Producer] DLQ Topic '{topic}' created successfully")
            except Exception as e:
                print(f"[Producer] Failed to create DLQ topic '{topic}': {e}")
                raise
                
    except Exception as e:
        print(f"[Producer] Error ensuring DLQ topic exists: {e}")
        raise


def init_kafka_producer():
    """Initialize Kafka producer and schema registry client."""
    global kafka_producer, schema_registry_client, avro_serializer
    
    try:
        # Initialize AdminClient first to check/create topics
        admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
        
        # Ensure main topic exists with proper configuration
        ensure_topic_exists(admin_client, ORDER_TOPIC, num_partitions=3, replication_factor=1)
        
        # Create DLQ topic with longer retention (30 days)
        ensure_topic_exists_dlq(admin_client, DLQ_TOPIC, num_partitions=1, replication_factor=1)
        
        # Initialize Schema Registry client
        schema_registry_client = SchemaRegistryClient({
            'url': SCHEMA_REGISTRY_URL
        })
        print(f"[Producer] Connected to Schema Registry at {SCHEMA_REGISTRY_URL}")
        
        # Parse schema string to Schema object
        try:
            schema_obj = Schema(ORDER_SCHEMA, schema_type='AVRO')
            print(f"[Producer] Avro schema validated successfully")
        except Exception as schema_error:
            print(f"[Producer] Invalid Avro schema: {schema_error}")
            raise ValueError(
                f"Failed to parse Avro schema. The schema may be invalid: {schema_error}"
            )
        
        # Create Avro serializer
        avro_serializer = AvroSerializer(
            schema_registry_client,
            schema_obj,
            lambda order, ctx: order  # Serialization function
        )
        
        # Initialize Kafka producer
        producer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id': 'cart-service-producer',
            'acks': 'all',  # Wait for all replicas
            'retries': 3,
            'max.in.flight.requests.per.connection': 1,  # Ensure ordering
            'enable.idempotence': True,  # Ensure exactly-once semantics
        }
        
        kafka_producer = Producer(producer_config)
        
        # Verify connection
        metadata = admin_client.list_topics(timeout=10)
        print(f"[Producer] Connected to Kafka successfully")
        
    except ValueError:
        # Re-raise schema validation errors
        raise
    except Exception as e:
        print(f"[Producer] Failed to initialize Kafka: {e}")
        raise


def ensure_kafka_connection():
    """Ensure Kafka producer is connected, reconnect if needed."""
    global kafka_producer, schema_registry_client, avro_serializer
    
    if kafka_producer is None or schema_registry_client is None or avro_serializer is None:
        try:
            init_kafka_producer()
        except Exception as e:
            print(f"[Producer] Error connecting to Kafka: {e}")
            raise HTTPException(
                status_code=503,
                detail="Service unavailable: could not connect to Kafka broker or schema registry. Please try again later."
            )


def delivery_callback(err, msg):
    """Callback function for message delivery confirmation."""
    if err is not None:
        print(f"[Producer] Message delivery failed: {err}")
    else:
        print(f"[Producer] Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


# ---------- DOMAIN LOGIC ---------- #
def generate_random_order(order_id: str, items_count: int) -> dict:
    """Generate a random order matching the required schema."""
    items = []
    total_amount = 0.0

    for i in range(items_count):
        quantity = random.randint(1, 5)
        price = round(random.uniform(10, 200), 2)
        line_total = round(quantity * price, 2)
        total_amount += line_total

        items.append({
            "itemId": f"ITEM-{''.join(random.choices(string.ascii_uppercase + string.digits, k=6))}",
            "quantity": quantity,
            "price": price,
        })

    currency = random.choice(CURRENCIES)
    customer_id = f"CUST-{''.join(random.choices(string.ascii_uppercase + string.digits, k=8))}"

    return {
        "orderId": order_id,
        "customerId": customer_id,
        "orderDate": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "items": items,
        "totalAmount": round(total_amount, 2),
        "currency": currency,
        "status": "new",
    }


def validate_create_request(body: CreateOrderRequest) -> None:
    """Strict validation for create order request."""
    order_id = (body.orderId or "").strip()
    if not order_id:
        raise HTTPException(
            status_code=400,
            detail="Invalid value: 'orderId' must be a non-empty string (cannot be empty or only whitespace).",
        )

    if not isinstance(body.itemsCount, int):
        raise HTTPException(
            status_code=400,
            detail=f"Type error: 'itemsCount' must be an integer, but received {type(body.itemsCount).__name__}.",
        )

    if body.itemsCount <= 0:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid value: 'itemsCount' must be a positive integer (greater than 0), but received {body.itemsCount}.",
        )


def validate_update_request(body: UpdateOrderRequest) -> None:
    """Strict validation for update order request."""
    order_id = (body.orderId or "").strip()
    if not order_id:
        raise HTTPException(
            status_code=400,
            detail="Invalid value: 'orderId' must be a non-empty string (cannot be empty or only whitespace).",
        )
    
    status = (body.status or "").strip()
    if not status:
        raise HTTPException(
            status_code=400,
            detail="Invalid value: 'status' must be a non-empty string (cannot be empty or only whitespace).",
        )
    
    # Prevent status="new" in updates (that's for creation only)
    if status.lower() == "new":
        raise HTTPException(
            status_code=400,
            detail="Invalid value: 'status' cannot be 'new' for update operations. Status 'new' is reserved for order creation. Use statuses like 'pending', 'confirmed', 'shipped', 'delivered', or 'cancelled'.",
        )


def publish_update_event(update: dict):
    """
    Publish a status update event to Kafka using true Avro union.
    Uses UPDATE event type with only necessary fields (orderId, status).
    
    Raises:
        HTTPException(400): For data/serialization errors
        HTTPException(503): For transient network/broker errors
        HTTPException(500): For unexpected errors
    """
    global kafka_producer, avro_serializer
    
    ensure_kafka_connection()
    
    try:
        # Build UPDATE event message using true union schema
        order_update_message = {
            "eventType": "UPDATE",
            "orderId": update["orderId"],
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "payload": {
                "status": update["status"]
            }
        }
        
        # Use orderId as the key to ensure ordering per order
        key = order_update_message["orderId"].encode('utf-8')
        
        # Serialize using Avro
        try:
            serialized_value = avro_serializer(order_update_message, SerializationContext(ORDER_TOPIC, MessageField.VALUE))
        except (TypeError, ValueError, KeyError) as serialization_error:
            # Data validation or schema mismatch errors
            print(f"[Producer] Serialization error: {serialization_error}")
            raise HTTPException(
                status_code=400,
                detail=f"Bad Request: Failed to serialize update data: {str(serialization_error)}"
            )
        except Exception as serialization_error:
            # Other serialization errors
            print(f"[Producer] Avro serialization error: {serialization_error}")
            raise HTTPException(
                status_code=400,
                detail=f"Bad Request: Invalid update data format: {str(serialization_error)}"
            )
        
        # Produce message
        try:
            kafka_producer.produce(
                topic=ORDER_TOPIC,
                key=key,
                value=serialized_value,
                callback=delivery_callback
            )
            
            # Flush to ensure message is sent
            kafka_producer.flush(timeout=10)
            
        except BufferError as buffer_error:
            # Queue is full - transient error
            print(f"[Producer] Buffer full error: {buffer_error}")
            raise HTTPException(
                status_code=503,
                detail="Service Unavailable: Message queue is full. Please try again in a moment."
            )
        except KafkaException as kafka_error:
            # Kafka-specific errors
            error_code = kafka_error.args[0].code() if kafka_error.args else None
            
            # Check if it's a retriable error
            if error_code in (KafkaError._MSG_TIMED_OUT, KafkaError.REQUEST_TIMED_OUT, 
                             KafkaError._TRANSPORT, KafkaError.NETWORK_EXCEPTION):
                print(f"[Producer] Transient Kafka error: {kafka_error}")
                raise HTTPException(
                    status_code=503,
                    detail=f"Service Unavailable: Kafka broker temporarily unreachable. Error: {str(kafka_error)}"
                )
            else:
                # Non-retriable Kafka error
                print(f"[Producer] Kafka error: {kafka_error}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Internal error: Kafka operation failed: {str(kafka_error)}"
                )
        
    except HTTPException:
        # Re-raise HTTPExceptions as-is
        raise
    except Exception as e:
        # Unexpected errors
        print(f"[Producer] Unexpected error publishing update: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal error: An unexpected error occurred: {str(e)}"
        )


def publish_order_event(order: dict):
    """
    Publish order CREATE event to Kafka using true Avro union.
    Wraps the order data in CREATE event type within the payload union.
    
    Raises:
        HTTPException(400): For data/serialization errors
        HTTPException(503): For transient network/broker errors
        HTTPException(500): For unexpected errors
    """
    global kafka_producer, avro_serializer
    
    ensure_kafka_connection()
    
    try:
        # Wrap order in CREATE event using true union schema
        order_create_message = {
            "eventType": "CREATE",
            "orderId": order["orderId"],
            "timestamp": order["orderDate"],  # Use order creation time as event timestamp
            "payload": {
                "customerId": order["customerId"],
                "orderDate": order["orderDate"],
                "items": order["items"],
                "totalAmount": order["totalAmount"],
                "currency": order["currency"],
                "status": order["status"]
            }
        }
        
        # Use orderId as the key to ensure ordering per order
        key = order_create_message["orderId"].encode('utf-8')
        
        # Serialize order using Avro
        try:
            serialized_value = avro_serializer(order_create_message, SerializationContext(ORDER_TOPIC, MessageField.VALUE))
        except (TypeError, ValueError, KeyError) as serialization_error:
            # Data validation or schema mismatch errors
            print(f"[Producer] Serialization error: {serialization_error}")
            raise HTTPException(
                status_code=400,
                detail=f"Bad Request: Failed to serialize order data. The data does not match the Avro schema: {str(serialization_error)}"
            )
        except Exception as serialization_error:
            # Other serialization errors (schema issues, etc.)
            print(f"[Producer] Avro serialization error: {serialization_error}")
            raise HTTPException(
                status_code=400,
                detail=f"Bad Request: Invalid order data format: {str(serialization_error)}"
            )
        
        # Produce message
        try:
            kafka_producer.produce(
                topic=ORDER_TOPIC,
                key=key,
                value=serialized_value,
                callback=delivery_callback
            )
            
            # Flush to ensure message is sent
            kafka_producer.flush(timeout=10)
            
        except BufferError as buffer_error:
            # Queue is full - transient error
            print(f"[Producer] Buffer full error: {buffer_error}")
            raise HTTPException(
                status_code=503,
                detail="Service Unavailable: Message queue is full. Please try again in a moment."
            )
        except KafkaException as kafka_error:
            # Kafka-specific errors (broker unreachable, etc.)
            error_code = kafka_error.args[0].code() if kafka_error.args else None
            
            # Check if it's a retriable error
            if error_code in (KafkaError._MSG_TIMED_OUT, KafkaError.REQUEST_TIMED_OUT, 
                             KafkaError._TRANSPORT, KafkaError.NETWORK_EXCEPTION):
                print(f"[Producer] Transient Kafka error: {kafka_error}")
                raise HTTPException(
                    status_code=503,
                    detail=f"Service Unavailable: Kafka broker temporarily unreachable. Error: {str(kafka_error)}"
                )
            else:
                # Non-retriable Kafka error
                print(f"[Producer] Kafka error: {kafka_error}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Internal error: Kafka operation failed: {str(kafka_error)}"
                )
        
    except HTTPException:
        # Re-raise HTTPExceptions as-is
        raise
    except Exception as e:
        # Unexpected errors
        print(f"[Producer] Unexpected error publishing message: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal error: An unexpected error occurred while publishing the order event: {str(e)}"
        )


# ---------- API ENDPOINTS ---------- #
@app.on_event("startup")
def on_startup():
    """Initialize Kafka connection on startup."""
    try:
        init_kafka_producer()
        print("[Producer] Kafka producer initialized successfully")
    except Exception as e:
        print(f"[Producer] Failed to connect to Kafka on startup: {e}")
        print("[Producer] Will attempt to reconnect on first request")


@app.on_event("shutdown")
def on_shutdown():
    """Clean up Kafka connection on shutdown."""
    global kafka_producer
    if kafka_producer:
        kafka_producer.flush(timeout=10)
        kafka_producer = None
        print("[Producer] Kafka producer closed")


@app.post("/create-order", status_code=201)
def create_order(body: CreateOrderRequest):
    """
    Create a new order and publish it to Kafka.
    The order will have status='new'.
    
    The producer does not validate if the orderId already exists.
    Duplicate detection is handled by the consumer (Order Service).
    """
    try:
        validate_create_request(body)
        
        # Generate order with status="new"
        order = generate_random_order(body.orderId.strip(), body.itemsCount)
        
        # Publish to Kafka (no local storage - producer is stateless)
        publish_order_event(order)
        
        return {
            "message": "Order creation event published successfully. The order will be validated and processed by the Order Service.",
            "order": order,
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"[Producer] Unexpected error in create_order: {e}")
        raise HTTPException(
            status_code=500,
            detail="Internal error: failed to create order."
        )


@app.put("/update-order", status_code=200)
def update_order(body: UpdateOrderRequest):
    """
    Update an existing order's status and publish the update to Kafka.
    
    The producer does NOT validate if the order exists - it only publishes the intent.
    The consumer (Order Service) will:
    - If orderId doesn't exist and status != 'new' → send to DLQ (invalid update)
    - If orderId exists and status != 'new' → merge the status update with existing order data
    
    Note: Only orderId and status are required. The status field must NOT be 'new'.
    """
    try:
        validate_update_request(body)
        
        # Build minimal update event (only orderId + status)
        # Consumer will merge this with existing order data
        update_event = {
            "orderId": body.orderId.strip(),
            "status": body.status.strip(),
        }
        
        print(f"[Producer] Publishing status update event for order {update_event['orderId']} → {update_event['status']}")
        
        # Publish status update to Kafka
        # Note: We're publishing a partial update, not a full order
        # The consumer must handle merging this with the existing order
        publish_update_event(update_event)
        
        return {
            "message": "Order status update event published successfully. The consumer will validate and apply the update.",
            "update": update_event,
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"[Producer] Unexpected error in update_order: {e}")
        raise HTTPException(
            status_code=500,
            detail="Internal error: failed to update order."
        )


@app.get("/health")
def health_check():
    """Health check endpoint."""
    try:
        ensure_kafka_connection()
        return {"status": "healthy", "kafka": "connected"}
    except:
        return {"status": "unhealthy", "kafka": "disconnected"}

