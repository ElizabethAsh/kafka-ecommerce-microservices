import os
import json
import threading
import time
import base64
from datetime import datetime
from enum import Enum
from typing import Dict, Set, List, Optional
from pathlib import Path

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry import Schema
from confluent_kafka.serialization import SerializationContext, MessageField
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

# Error categorization for DLQ
class ErrorCategory(str, Enum):
    """
    Categories of errors for DLQ handling.
    Determines retry behavior and routing.
    """
    NO_RETRY_DLQ = "NO_RETRY_DLQ"           # Immediate to DLQ (non-retriable errors)
    RETRY_THEN_DLQ = "RETRY_THEN_DLQ"       # Retry with backoff, then DLQ
    RETRY_FOREVER = "RETRY_FOREVER"          # Retry indefinitely (infrastructure errors)

# Load Avro schema from file
SCHEMA_FILE = Path(__file__).parent / "order.avsc"
try:
    with open(SCHEMA_FILE, 'r') as f:
        ORDER_SCHEMA = f.read()
    
    # Validate it's valid JSON
    json.loads(ORDER_SCHEMA)
    
    if not ORDER_SCHEMA.strip():
        raise ValueError("Schema file is empty")
        
    print(f"[Consumer] Successfully loaded Avro schema from {SCHEMA_FILE}")
    
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
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "order-service-consumer")

# DLQ configuration
MAX_RETRIES = 3
RETRY_DELAYS = [2, 5, 10]  # Exponential backoff in seconds

app = FastAPI(title="Order Service - Consumer")

# Global variables for Kafka consumer and schema registry
kafka_consumer: Consumer = None
schema_registry_client: SchemaRegistryClient = None
avro_deserializer: AvroDeserializer = None
dlq_producer: Producer = None  # Producer for sending messages to DLQ

# In-memory storage for orders: orderId -> order data with shipping cost
order_database: Dict[str, dict] = {}
# Track orderIds per topic
topic_order_ids: Dict[str, Set[str]] = {}
# Lock to ensure thread-safe access
db_lock = threading.Lock()


# ---------- CUSTOM ERROR HANDLERS ---------- #
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle validation errors with specific error messages."""
    error_messages = []
    
    for error in exc.errors():
        field_name = error["loc"][-1] if error["loc"] else "request parameter"
        error_type = error["type"]
        error_msg = error.get("msg", "")
        
        if error_type == "missing":
            error_messages.append(f"Missing required parameter: '{field_name}'. This parameter is mandatory.")
        elif error_type.startswith("type_error"):
            error_messages.append(f"Type error: Parameter '{field_name}' has an invalid type. {error_msg}")
        elif error_type == "value_error":
            if "string_too_short" in error_msg.lower():
                error_messages.append(f"Invalid value: Parameter '{field_name}' must not be empty.")
            else:
                error_messages.append(f"Invalid value for parameter '{field_name}': {error_msg}")
        else:
            error_messages.append(f"Validation error for parameter '{field_name}': {error_msg}")

    if not error_messages:
        error_messages.append("Invalid request parameters. Please check the parameter format and types.")

    return JSONResponse(
        status_code=400,
        content={
            "message": "Invalid request parameters.",
            "errors": error_messages,
        },
    )


# ---------- KAFKA SETUP ---------- #
def ensure_topic_exists(admin_client: AdminClient, topic_name: str):
    """
    Verify the Kafka topic exists. Log a warning if it doesn't.
    Consumer shouldn't create topics, but should verify they exist.
    
    Args:
        admin_client: Kafka AdminClient instance
        topic_name: Name of the topic to verify
    """
    try:
        # Get existing topics
        metadata = admin_client.list_topics(timeout=10)
        existing_topics = metadata.topics
        
        if topic_name in existing_topics:
            print(f"[Consumer] Topic '{topic_name}' exists and is ready")
            topic_metadata = existing_topics[topic_name]
            num_partitions = len(topic_metadata.partitions)
            print(f"[Consumer] Topic '{topic_name}' has {num_partitions} partition(s)")
        else:
            print(f"[Consumer] WARNING: Topic '{topic_name}' does not exist yet. It will be created on first producer message.")
                
    except Exception as e:
        print(f"[Consumer] Error checking topic existence: {e}")
        # Don't raise - consumer can still subscribe and wait for topic creation


def init_dlq_producer():
    """Initialize DLQ producer for sending failed messages to Dead Letter Queue."""
    global dlq_producer
    
    try:
        producer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id': 'order-consumer-dlq-producer',
            'acks': 'all',
            'retries': 3,
        }
        
        dlq_producer = Producer(producer_config)
        print(f"[Consumer] DLQ Producer initialized successfully")
        
    except Exception as e:
        print(f"[Consumer] Failed to initialize DLQ producer: {e}")
        raise


def send_to_dlq(original_message, error_category: str, error_message: str, retry_count: int = 0):
    """
    Send a failed message to the Dead Letter Queue.
    
    Args:
        original_message: The original Kafka message that failed
        error_category: Category of the error (from ErrorCategory enum)
        error_message: Description of the error
        retry_count: Number of times the message was retried
    """
    global dlq_producer
    
    try:
        # Ensure DLQ producer is initialized
        if dlq_producer is None:
            init_dlq_producer()
        
        # Extract information from original message
        original_topic = original_message.topic() if original_message else "unknown"
        original_partition = original_message.partition() if original_message else -1
        original_offset = original_message.offset() if original_message else -1
        original_key = original_message.key().decode('utf-8') if original_message and original_message.key() else None
        original_value_bytes = original_message.value() if original_message else b""
        
        # Encode original message value as base64 for safe JSON transport
        original_value_base64 = base64.b64encode(original_value_bytes).decode('utf-8') if original_value_bytes else ""
        
        # Create DLQ message with metadata
        dlq_message = {
            "dlq_metadata": {
                "original_topic": original_topic,
                "original_partition": original_partition,
                "original_offset": original_offset,
                "failed_at": datetime.utcnow().isoformat() + "Z",
                "retry_count": retry_count,
                "error_category": error_category,
                "error_message": error_message,
                "consumer_group": CONSUMER_GROUP_ID
            },
            "original_message": {
                "key": original_key,
                "value_base64": original_value_base64
            }
        }
        
        # Serialize to JSON
        dlq_value = json.dumps(dlq_message).encode('utf-8')
        
        # Use original key or generate one
        dlq_key = original_key.encode('utf-8') if original_key else f"dlq-{original_offset}".encode('utf-8')
        
        # Send to DLQ topic
        dlq_producer.produce(
            topic=DLQ_TOPIC,
            key=dlq_key,
            value=dlq_value,
            callback=lambda err, msg: print(
                f"[Consumer] DLQ delivery {'failed' if err else 'succeeded'}: "
                f"key={original_key}, error_category={error_category}"
            )
        )
        
        # Flush to ensure message is sent
        dlq_producer.flush(timeout=5)
        
        print(f"[Consumer] Message sent to DLQ: key={original_key}, category={error_category}, retries={retry_count}")
        
    except Exception as e:
        print(f"[Consumer] Failed to send message to DLQ: {e}")
        # Don't raise - we don't want DLQ failures to crash the consumer


def init_kafka_consumer():
    """Initialize Kafka consumer and schema registry client."""
    global kafka_consumer, schema_registry_client, avro_deserializer
    
    try:
        # Initialize AdminClient first to verify topic
        admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
        
        # Verify topic exists (consumer doesn't create it)
        ensure_topic_exists(admin_client, ORDER_TOPIC)
        
        # Initialize Schema Registry client
        schema_registry_client = SchemaRegistryClient({
            'url': SCHEMA_REGISTRY_URL
        })
        print(f"[Consumer] Connected to Schema Registry at {SCHEMA_REGISTRY_URL}")
        
        # Parse schema string to Schema object
        try:
            schema_obj = Schema(ORDER_SCHEMA, schema_type='AVRO')
            print(f"[Consumer] Avro schema validated successfully")
        except Exception as schema_error:
            print(f"[Consumer] Invalid Avro schema: {schema_error}")
            raise ValueError(
                f"Failed to parse Avro schema. The schema may be invalid: {schema_error}"
            )
        
        # Create Avro deserializer
        avro_deserializer = AvroDeserializer(
            schema_registry_client,
            schema_obj
        )
        
        # Initialize Kafka consumer
        consumer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': CONSUMER_GROUP_ID,
            'auto.offset.reset': 'earliest',  # Start from beginning if no offset
            'enable.auto.commit': False,  # Manual commit for better control
            'enable.partition.eof': False,
        }
        
        kafka_consumer = Consumer(consumer_config)
        
        # Subscribe to topic
        kafka_consumer.subscribe([ORDER_TOPIC])
        
        # Verify connection
        metadata = admin_client.list_topics(timeout=10)
        print(f"[Consumer] Connected to Kafka successfully, subscribed to topic: {ORDER_TOPIC}")
        
    except ValueError:
        # Re-raise schema validation errors
        raise
    except Exception as e:
        print(f"[Consumer] Failed to initialize Kafka: {e}")
        raise


def ensure_kafka_connection():
    """Ensure Kafka consumer is connected, reconnect if needed."""
    global kafka_consumer, schema_registry_client, avro_deserializer
    
    if kafka_consumer is None or schema_registry_client is None or avro_deserializer is None:
        try:
            init_kafka_consumer()
        except Exception as e:
            print(f"[Consumer] Error connecting to Kafka: {e}")
            raise HTTPException(
                status_code=503,
                detail="Service unavailable: could not connect to Kafka broker or schema registry. Please try again later."
            )


# ---------- DOMAIN LOGIC ---------- #
def calculate_shipping(total: float) -> float:
    """Calculate shipping cost as 2% of the total amount"""
    return round(total * 0.02, 2)


def process_with_retry(msg, order_data: dict, order_id: str) -> bool:
    """
    Process order message with retry logic and exponential backoff.
    Handles both full order creation and status-only updates.
    
    Args:
        msg: Original Kafka message
        order_data: Deserialized order data
        order_id: Order ID from the message
        
    Returns:
        True if processing succeeded, False if failed after retries
    """
    retry_count = 0
    
    while retry_count <= MAX_RETRIES:
        try:
            # Detect if this is a status-only update (minimal message from producer)
            # Status-only updates have empty items array and status != "new"
            is_status_update = (
                len(order_data.get("items", [])) == 0 and 
                order_data.get("status", "") != "new" and
                order_data.get("customerId", "") == "__UPDATE_PLACEHOLDER__"
            )
            
            with db_lock:
                if is_status_update and order_id in order_database:
                    # Status-only update: merge new status with existing order
                    existing_order = order_database[order_id]
                    existing_order["status"] = order_data.get("status")
                    existing_order["orderDate"] = order_data.get("orderDate")  # Update timestamp
                    
                    print(f"[Consumer] Status update applied to order {order_id}: status={existing_order['status']}")
                    print(f"[Consumer] Existing order data preserved (items, amounts, customer, shipping, etc.)")
                    
                    # Track orderId per topic
                    topic_name = msg.topic()
                    if topic_name not in topic_order_ids:
                        topic_order_ids[topic_name] = set()
                    topic_order_ids[topic_name].add(order_id)
                    
                    return True
                
                else:
                    # Full order creation: process normally
                    order_total = order_data.get("totalAmount")
                    
                    # Calculate shipping cost (2% of total)
                    shipping = calculate_shipping(float(order_total))
                    
                    # Add shipping cost to order data
                    order_with_shipping = dict(order_data)
                    order_with_shipping["shippingCost"] = shipping
                    
                    # Store in memory (replaces existing order if any)
                    order_database[order_id] = order_with_shipping
                    
                    # Track orderId per topic
                    topic_name = msg.topic()
                    if topic_name not in topic_order_ids:
                        topic_order_ids[topic_name] = set()
                    topic_order_ids[topic_name].add(order_id)
                    
                    print(f"[Consumer] Order {order_id} processed successfully with shipping cost ${shipping}")
                    return True
            
        except Exception as process_error:
            retry_count += 1
            print(f"[Consumer] Processing error for order {order_id} (attempt {retry_count}/{MAX_RETRIES + 1}): {process_error}")
            
            if retry_count <= MAX_RETRIES:
                # Retry with exponential backoff
                delay = RETRY_DELAYS[retry_count - 1] if retry_count <= len(RETRY_DELAYS) else RETRY_DELAYS[-1]
                print(f"[Consumer] Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                # Max retries exceeded, send to DLQ
                print(f"[Consumer] Max retries exceeded for order {order_id}, sending to DLQ")
                send_to_dlq(
                    original_message=msg,
                    error_category=ErrorCategory.RETRY_THEN_DLQ,
                    error_message=f"Processing failed after {MAX_RETRIES} retries: {str(process_error)}",
                    retry_count=MAX_RETRIES
                )
                return False
    
    return False


def process_order_message():
    """
    Main consumer loop that listens for messages from Kafka.
    Processes orders and stores them with shipping cost.
    Ensures ordering per orderId by processing messages sequentially.
    """
    global kafka_consumer, avro_deserializer
    
    while True:
        try:
            # Ensure connection
            if kafka_consumer is None:
                init_kafka_consumer()
            
            # Poll for messages (with timeout)
            msg = kafka_consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event - not an error
                    print(f"[Consumer] Reached end of partition {msg.partition()}")
                    continue
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    print(f"[Consumer] Topic or partition not found: {msg.error()}")
                    time.sleep(2)
                    continue
                else:
                    print(f"[Consumer] Consumer error: {msg.error()}")
                    raise KafkaException(msg.error())
            
            # Deserialize message using Avro
            try:
                order_data = avro_deserializer(
                    msg.value(),
                    SerializationContext(msg.topic(), MessageField.VALUE)
                )
            except Exception as deserialize_err:
                print(f"[Consumer] Deserialization error: {deserialize_err}")
                
                # Send to DLQ immediately (non-retriable error)
                send_to_dlq(
                    original_message=msg,
                    error_category=ErrorCategory.NO_RETRY_DLQ,
                    error_message=f"Deserialization failed: {str(deserialize_err)}",
                    retry_count=0
                )
                
                # Commit offset to avoid reprocessing the same bad message
                kafka_consumer.commit(message=msg)
                continue
            
            # Get order ID
            order_id = order_data.get("orderId")
            if not order_id:
                print(f"[Consumer] Validation error: Order missing orderId")
                
                # Send to DLQ (data validation error - non-retriable)
                send_to_dlq(
                    original_message=msg,
                    error_category=ErrorCategory.NO_RETRY_DLQ,
                    error_message="Validation failed: Missing required field 'orderId'",
                    retry_count=0
                )
                
                kafka_consumer.commit(message=msg)
                continue
            
            # Check for duplicate CREATE request and invalid UPDATE request
            # Duplicates: orderId exists AND status is "new" (initial creation status)
            # Invalid UPDATE: orderId doesn't exist AND status is NOT "new"
            order_status = order_data.get("status", "")
            
            with db_lock:
                order_exists = order_id in order_database
            
            # Case 1: Duplicate CREATE (order exists + status="new")
            if order_exists and order_status == "new":
                print(f"[Consumer] Duplicate CREATE detected: {order_id} already exists with status='new'")
                print(f"[Consumer] This appears to be a duplicate create-order request, not an update")
                
                # Send to DLQ (duplicate CREATE - non-retriable)
                send_to_dlq(
                    original_message=msg,
                    error_category=ErrorCategory.NO_RETRY_DLQ,
                    error_message=f"Duplicate CREATE request: Order with ID '{order_id}' already exists in the system, "
                                 f"and incoming message has status='new' (initial creation status). "
                                 f"This is likely a duplicate create-order request. "
                                 f"Use PUT /update-order endpoint to modify existing orders.",
                    retry_count=0
                )
                
                kafka_consumer.commit(message=msg)
                continue
            
            # Case 2: Invalid UPDATE (order doesn't exist + status!="new")
            elif not order_exists and order_status != "new":
                print(f"[Consumer] Invalid UPDATE detected: {order_id} doesn't exist but received status='{order_status}'")
                print(f"[Consumer] UPDATE messages should only be sent for existing orders")
                
                # Send to DLQ (invalid UPDATE - non-retriable)
                send_to_dlq(
                    original_message=msg,
                    error_category=ErrorCategory.NO_RETRY_DLQ,
                    error_message=f"Invalid UPDATE request: Received status='{order_status}' for order '{order_id}' "
                                 f"which doesn't exist in the system. "
                                 f"Orders must be created first with status='new' (via POST /create-order) "
                                 f"before they can be updated.",
                    retry_count=0
                )
                
                kafka_consumer.commit(message=msg)
                continue
            
            # Case 3: Valid UPDATE (order exists + status!="new")
            elif order_exists:
                print(f"[Consumer] Update detected for {order_id}: status='{order_status}' (not 'new', so treating as update)")
                # Continue processing - might be status-only update or full update
            
            # Case 4: Valid CREATE (order doesn't exist + status="new")
            # Just continue processing
            
            # Detect if this is a status-only update (minimal message)
            is_status_update = (
                len(order_data.get("items", [])) == 0 and 
                order_status != "new" and
                order_data.get("customerId", "") == "__UPDATE_PLACEHOLDER__"
            )
            
            # Validate totalAmount (skip for status-only updates)
            if not is_status_update:
                order_total = order_data.get("totalAmount")
                if not isinstance(order_total, (int, float)):
                    print(f"[Consumer] Validation error: Invalid totalAmount in order {order_id}")
                    
                    # Send to DLQ (data validation error - non-retriable)
                    send_to_dlq(
                        original_message=msg,
                        error_category=ErrorCategory.NO_RETRY_DLQ,
                        error_message=f"Validation failed: Invalid totalAmount type (expected number, got {type(order_total).__name__})",
                        retry_count=0
                    )
                    
                    kafka_consumer.commit(message=msg)
                    continue
            
            # Log order details
            if is_status_update:
                print(f"[Consumer] Received STATUS UPDATE event:")
                print(f"  Order ID: {order_id}")
                print(f"  New Status: {order_data.get('status', 'N/A')}")
                print(f"  Update Time: {order_data.get('orderDate', 'N/A')}")
                print(f"  Topic: {msg.topic()}")
                print(f"  Partition: {msg.partition()}, Offset: {msg.offset()}")
            else:
                print(f"[Consumer] Received FULL ORDER event:")
                print(f"  Order ID: {order_id}")
                print(f"  Customer ID: {order_data.get('customerId', 'N/A')}")
                print(f"  Order Date: {order_data.get('orderDate', 'N/A')}")
                print(f"  Total Amount: ${order_data.get('totalAmount', 0)}")
                print(f"  Number of Items: {len(order_data.get('items', []))}")
                print(f"  Status: {order_data.get('status', 'N/A')}")
                print(f"  Topic: {msg.topic()}")
                print(f"  Partition: {msg.partition()}, Offset: {msg.offset()}")
            
            # Process order with retry logic
            success = process_with_retry(msg, order_data, order_id)
            
            if not success:
                # Processing failed after retries, already sent to DLQ
                print(f"[Consumer] Order {order_id} processing failed, committed offset")
                kafka_consumer.commit(message=msg)
                continue
            
            # Commit offset after successful processing
            kafka_consumer.commit(message=msg)
            
        except KafkaException as kafka_err:
            print(f"[Consumer] Kafka error in consumer loop: {kafka_err}. Will retry...")
            time.sleep(2)
            # Try to reinitialize consumer
            try:
                if kafka_consumer:
                    kafka_consumer.close()
                kafka_consumer = None
            except:
                pass
        except Exception as err:
            print(f"[Consumer] Error in consumer loop: {err}. Will retry in 2 seconds...")
            time.sleep(2)


# ---------- API ENDPOINTS ---------- #
@app.on_event("startup")
def start_consumer_thread():
    """Start the Kafka consumer and DLQ producer in a background thread when the app starts"""
    try:
        # Initialize DLQ producer
        init_dlq_producer()
    except Exception as e:
        print(f"[Consumer] Failed to initialize DLQ producer on startup: {e}")
        print("[Consumer] Will attempt to reconnect on first DLQ message")
    
    # Start consumer thread
    consumer_thread = threading.Thread(target=process_order_message, daemon=True)
    consumer_thread.start()
    print("[Consumer] Consumer thread started")


@app.get("/order-details")
def get_order_details(orderId: str = Query(..., min_length=1)):
    """
    Retrieve order details including shipping cost by orderId.
    Returns 404 if order is not found.
    """
    try:
        with db_lock:
            order_info = order_database.get(orderId)

        if not order_info:
            raise HTTPException(
                status_code=404,
                detail=f"Order with ID '{orderId}' not found"
            )

        return order_info

    except HTTPException:
        raise
    except Exception as e:
        print(f"[Consumer] Error retrieving order details: {e}")
        raise HTTPException(
            status_code=500,
            detail="Internal error: failed to retrieve order details."
        )


@app.get("/getAllOrderIdsFromTopic")
def get_all_order_ids_from_topic(topic: str = Query(..., min_length=1)):
    """
    Get all orderIds that were received from the specified topic.
    Returns a list of orderIds.
    """
    try:
        with db_lock:
            order_ids = list(topic_order_ids.get(topic, set()))
        
        return {
            "topic": topic,
            "orderIds": sorted(order_ids),
            "count": len(order_ids)
        }
        
    except Exception as e:
        print(f"[Consumer] Error retrieving order IDs from topic: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal error: failed to retrieve order IDs from topic '{topic}'."
        )


@app.get("/dlq/stats")
def get_dlq_stats():
    """
    Get Dead Letter Queue statistics.
    Returns count of failed messages and breakdown by error category.
    """
    try:
        # Create consumer for DLQ topic
        dlq_consumer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'dlq-stats-reader',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }
        
        dlq_consumer = Consumer(dlq_consumer_config)
        dlq_consumer.subscribe([DLQ_TOPIC])
        
        total_messages = 0
        error_categories = {}
        oldest_timestamp = None
        
        # Poll messages quickly (just counting, not processing)
        timeout_count = 0
        max_timeouts = 5
        
        while timeout_count < max_timeouts:
            msg = dlq_consumer.poll(timeout=1.0)
            
            if msg is None:
                timeout_count += 1
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    timeout_count += 1
                    continue
                else:
                    continue
            
            timeout_count = 0  # Reset on successful message
            total_messages += 1
            
            # Parse DLQ message
            try:
                dlq_data = json.loads(msg.value().decode('utf-8'))
                error_category = dlq_data.get('dlq_metadata', {}).get('error_category', 'UNKNOWN')
                failed_at = dlq_data.get('dlq_metadata', {}).get('failed_at')
                
                # Count by category
                error_categories[error_category] = error_categories.get(error_category, 0) + 1
                
                # Track oldest message
                if failed_at:
                    if oldest_timestamp is None or failed_at < oldest_timestamp:
                        oldest_timestamp = failed_at
                        
            except Exception as parse_err:
                print(f"[Consumer] Error parsing DLQ message: {parse_err}")
        
        dlq_consumer.close()
        
        # Calculate oldest message age
        oldest_age_hours = None
        if oldest_timestamp:
            try:
                oldest_dt = datetime.fromisoformat(oldest_timestamp.replace('Z', '+00:00'))
                age_seconds = (datetime.utcnow() - oldest_dt.replace(tzinfo=None)).total_seconds()
                oldest_age_hours = round(age_seconds / 3600, 2)
            except:
                pass
        
        return {
            "dlq_topic": DLQ_TOPIC,
            "total_messages": total_messages,
            "by_error_category": error_categories,
            "oldest_message_age_hours": oldest_age_hours
        }
        
    except Exception as e:
        print(f"[Consumer] Error getting DLQ stats: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve DLQ statistics: {str(e)}"
        )


@app.get("/dlq/messages")
def get_dlq_messages(limit: int = Query(10, ge=1, le=100), offset: int = Query(0, ge=0)):
    """
    Get messages from the Dead Letter Queue.
    Returns a list of failed messages with metadata.
    
    Args:
        limit: Maximum number of messages to return (1-100)
        offset: Number of messages to skip
    """
    try:
        # Create consumer for DLQ topic
        dlq_consumer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': f'dlq-reader-{int(time.time())}',  # Unique group to read from beginning
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }
        
        dlq_consumer = Consumer(dlq_consumer_config)
        dlq_consumer.subscribe([DLQ_TOPIC])
        
        messages = []
        current_offset = 0
        timeout_count = 0
        max_timeouts = 5
        
        while len(messages) < limit and timeout_count < max_timeouts:
            msg = dlq_consumer.poll(timeout=1.0)
            
            if msg is None:
                timeout_count += 1
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    timeout_count += 1
                    continue
                else:
                    continue
            
            timeout_count = 0  # Reset on successful message
            
            # Skip messages until we reach the requested offset
            if current_offset < offset:
                current_offset += 1
                continue
            
            # Parse and add message
            try:
                dlq_data = json.loads(msg.value().decode('utf-8'))
                messages.append({
                    "kafka_offset": msg.offset(),
                    "kafka_partition": msg.partition(),
                    "dlq_data": dlq_data
                })
                current_offset += 1
            except Exception as parse_err:
                print(f"[Consumer] Error parsing DLQ message: {parse_err}")
        
        dlq_consumer.close()
        
        return {
            "dlq_topic": DLQ_TOPIC,
            "count": len(messages),
            "offset": offset,
            "limit": limit,
            "messages": messages
        }
        
    except Exception as e:
        print(f"[Consumer] Error getting DLQ messages: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve DLQ messages: {str(e)}"
        )


@app.get("/health")
def health_check():
    """Health check endpoint."""
    try:
        ensure_kafka_connection()
        return {"status": "healthy", "kafka": "connected"}
    except:
        return {"status": "unhealthy", "kafka": "disconnected"}

