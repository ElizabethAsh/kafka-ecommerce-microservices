import os
import threading
import time
from typing import Dict, Set, List
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry import Schema
from confluent_kafka.serialization import SerializationContext, MessageField
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

# Load Avro schema from file
SCHEMA_FILE = Path(__file__).parent / "order.avsc"
with open(SCHEMA_FILE, 'r') as f:
    ORDER_SCHEMA = f.read()

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
ORDER_TOPIC = os.getenv("ORDER_TOPIC", "order-events")
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "order-service-consumer")

app = FastAPI(title="Order Service - Consumer")

# Global variables for Kafka consumer and schema registry
kafka_consumer: Consumer = None
schema_registry_client: SchemaRegistryClient = None
avro_deserializer: AvroDeserializer = None

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
def init_kafka_consumer():
    """Initialize Kafka consumer and schema registry client."""
    global kafka_consumer, schema_registry_client, avro_deserializer
    
    try:
        # Initialize Schema Registry client
        schema_registry_client = SchemaRegistryClient({
            'url': SCHEMA_REGISTRY_URL
        })
        
        # Parse schema string to Schema object
        schema_obj = Schema(ORDER_SCHEMA, schema_type='AVRO')
        
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
        admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
        metadata = admin_client.list_topics(timeout=10)
        print(f"[Consumer] Connected to Kafka successfully, subscribed to topic: {ORDER_TOPIC}")
        
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
                print(f"[Consumer] Could not deserialize message: {deserialize_err}")
                # Commit offset even if deserialization fails to avoid reprocessing
                kafka_consumer.commit(message=msg)
                continue
            
            # Get order ID
            order_id = order_data.get("orderId")
            if not order_id:
                print(f"[Consumer] Order missing orderId, skipping")
                kafka_consumer.commit(message=msg)
                continue
            
            # Get total amount and validate
            order_total = order_data.get("totalAmount")
            if not isinstance(order_total, (int, float)):
                print(f"[Consumer] Invalid totalAmount in order, skipping")
                kafka_consumer.commit(message=msg)
                continue
            
            # Calculate shipping cost (2% of total)
            shipping = calculate_shipping(float(order_total))
            
            # Add shipping cost to order data
            order_with_shipping = dict(order_data)
            order_with_shipping["shippingCost"] = shipping
            
            # Log order details
            print(f"[Consumer] Received order event:")
            print(f"  Order ID: {order_id}")
            print(f"  Customer ID: {order_data.get('customerId', 'N/A')}")
            print(f"  Order Date: {order_data.get('orderDate', 'N/A')}")
            print(f"  Total Amount: ${order_total}")
            print(f"  Shipping Cost: ${shipping}")
            print(f"  Number of Items: {len(order_data.get('items', []))}")
            print(f"  Status: {order_data.get('status', 'N/A')}")
            print(f"  Topic: {msg.topic()}")
            print(f"  Partition: {msg.partition()}, Offset: {msg.offset()}")
            
            # Store in memory (thread-safe)
            with db_lock:
                # Update order (allows updates to existing orders)
                order_database[order_id] = order_with_shipping
                
                # Track orderId per topic
                topic_name = msg.topic()
                if topic_name not in topic_order_ids:
                    topic_order_ids[topic_name] = set()
                topic_order_ids[topic_name].add(order_id)
                
                print(f"[Consumer] Order {order_id} stored successfully with shipping cost ${shipping}")
            
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
    """Start the Kafka consumer in a background thread when the app starts"""
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


@app.get("/health")
def health_check():
    """Health check endpoint."""
    try:
        ensure_kafka_connection()
        return {"status": "healthy", "kafka": "connected"}
    except:
        return {"status": "unhealthy", "kafka": "disconnected"}

