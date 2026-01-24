import os
import random
import string
import time
import json
from typing import Dict, Optional

from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import Schema
from confluent_kafka.serialization import SerializationContext, MessageField
from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
ORDER_TOPIC = os.getenv("ORDER_TOPIC", "order-events")

app = FastAPI(title="Cart Service - Producer")

# Global variables for Kafka producer and schema registry
kafka_producer: Optional[Producer] = None
schema_registry_client: Optional[SchemaRegistryClient] = None
avro_serializer: Optional[AvroSerializer] = None

# In-memory storage for orders (to support update-order endpoint)
order_database: Dict[str, dict] = {}

CURRENCIES = ["USD", "EUR", "ILS", "GBP"]

# Avro schema for Order
ORDER_SCHEMA = """
{
  "type": "record",
  "name": "Order",
  "namespace": "com.ecommerce",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "customerId", "type": "string"},
    {"name": "orderDate", "type": "string"},
    {
      "name": "items",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "OrderItem",
          "fields": [
            {"name": "itemId", "type": "string"},
            {"name": "quantity", "type": "int"},
            {"name": "price", "type": "double"}
          ]
        }
      }
    },
    {"name": "totalAmount", "type": "double"},
    {"name": "currency", "type": "string"},
    {"name": "status", "type": "string"}
  ]
}
"""


class CreateOrderRequest(BaseModel):
    orderId: str
    itemsCount: int


class UpdateOrderRequest(BaseModel):
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
def init_kafka_producer():
    """Initialize Kafka producer and schema registry client."""
    global kafka_producer, schema_registry_client, avro_serializer
    
    try:
        # Initialize Schema Registry client
        schema_registry_client = SchemaRegistryClient({
            'url': SCHEMA_REGISTRY_URL
        })
        
        # Parse schema string to Schema object
        schema_obj = Schema(ORDER_SCHEMA, schema_type='AVRO')
        
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
        
        # Verify connection by checking if we can get metadata
        admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
        metadata = admin_client.list_topics(timeout=10)
        print(f"[Producer] Connected to Kafka successfully")
        
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


def publish_order_event(order: dict):
    """Publish order event to Kafka using Avro serialization."""
    global kafka_producer, avro_serializer
    
    ensure_kafka_connection()
    
    try:
        # Use orderId as the key to ensure ordering per order
        key = order["orderId"].encode('utf-8')
        
        # Serialize order using Avro
        serialized_value = avro_serializer(order, SerializationContext(ORDER_TOPIC, MessageField.VALUE))
        
        # Produce message
        kafka_producer.produce(
            topic=ORDER_TOPIC,
            key=key,
            value=serialized_value,
            callback=delivery_callback
        )
        
        # Flush to ensure message is sent
        kafka_producer.flush(timeout=10)
        
    except Exception as e:
        print(f"[Producer] Error publishing message: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal error: failed to publish order event to Kafka. Error: {str(e)}"
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
    """
    try:
        validate_create_request(body)
        
        order = generate_random_order(body.orderId.strip(), body.itemsCount)
        
        # Store order in memory for update-order endpoint
        order_database[order["orderId"]] = order
        
        # Publish to Kafka
        publish_order_event(order)
        
        return {
            "message": "Order created and published successfully.",
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
    """
    try:
        validate_update_request(body)
        
        order_id = body.orderId.strip()
        
        # Check if order exists
        if order_id not in order_database:
            raise HTTPException(
                status_code=404,
                detail=f"Order with ID '{order_id}' not found. Cannot update non-existent order."
            )
        
        # Update order status
        order = order_database[order_id].copy()
        order["status"] = body.status.strip()
        order["orderDate"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())  # Update timestamp
        
        # Update in-memory database
        order_database[order_id] = order
        
        # Publish update to Kafka
        publish_order_event(order)
        
        return {
            "message": "Order updated and published successfully.",
            "order": order,
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

