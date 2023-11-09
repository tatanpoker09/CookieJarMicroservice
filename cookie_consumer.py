import json
import sys

import fastavro
from confluent_kafka import Consumer

from database_manager import DatabaseManager

TOPIC = "cookiejar-consumer-"

CONSUMER_CONFIG = {
    "bootstrap.servers": "localhost:29092",
    "group.id": "calculator_group",
    "auto.offset.reset": "earliest",
    "client.id": "calculator_client",
}

PRODUCER_CONFIG = {
    "bootstrap.servers": "localhost:29092",
}


REQUEST_SCHEMA_FILE = "../../orchestrator/src/main/avro/cookie_consumer_request.avsc"
RESPONSE_SCHEMA_FILE = "../../orchestrator/src/main/avro/cookie_consumer_response.avsc"


def handle_request(request):
    req_id = request.get("id")
    action = request.get("action")

    count = int(input(f"How many cookies would you like to {action}? "))
    return {
        "request_id": req_id,
        "count": count,
        "action": action,
    }


def main():
    global TOPIC
    TOPIC += sys.argv[1]

    consumer = Consumer(CONSUMER_CONFIG)

    db_manager = DatabaseManager(
        host="127.0.0.1",
        user="cmudc",
        passwd="cmudc_secure_password",
        db="outbox_events",
    )
    consumer.subscribe([TOPIC])
    print(f"Listening for messages on topic: {TOPIC}")

    request_schema = fastavro.schema.load_schema(REQUEST_SCHEMA_FILE)
    response_schema = fastavro.schema.load_schema(RESPONSE_SCHEMA_FILE)

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
        else:
            # parse request and validate
            request = json.loads(msg.value())
            fastavro.validate(request, request_schema)
            print(f"request parsed: {request}")

            response = handle_request(request)
            fastavro.validate(response, response_schema)
            print("Sending response: ", response)

            db_manager.insert_outbox_event("cookie-consumer-reply", response)

    db_manager.close()
    consumer.close()


if __name__ == "__main__":
    main()
