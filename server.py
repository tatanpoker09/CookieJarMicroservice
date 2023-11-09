import json

import fastavro
from confluent_kafka import Consumer

from cookiejar import CookieJar, CookieOverdrawException
from database_manager import DatabaseManager

TOPIC = "cookiejar"

CONSUMER_CONFIG = {
    "bootstrap.servers": "localhost:29092",
    "group.id": "calculator_group",
    "auto.offset.reset": "earliest",
    "client.id": "calculator_client",
}

PRODUCER_CONFIG = {
    "bootstrap.servers": "localhost:29092",
}
COOKIEJAR_INSTANCE = None

REQUEST_SCHEMA_FILE = "../../orchestrator/src/main/avro/cookiejar_request.avsc"

RESPONSE_SCHEMA_FILE = "../../orchestrator/src/main/avro/cookiejar_response.avsc"


ERROR_SCHEMA_FILE = "../../SAGAFramework/src/main/avro/error_response.avsc"


def handle_request(request):
    global COOKIEJAR_INSTANCE
    req_id = request.get("id")
    action = request.get("action")
    count = request.get("count")

    if action == "initialize":
        COOKIEJAR_INSTANCE = CookieJar(count)
    elif action == "add" and COOKIEJAR_INSTANCE:
        COOKIEJAR_INSTANCE.add(count)
    elif action == "take" and COOKIEJAR_INSTANCE:
        COOKIEJAR_INSTANCE.take(count)

    return {
        "request_id": req_id,
        "count": COOKIEJAR_INSTANCE.count() if COOKIEJAR_INSTANCE else 0,
    }


def main():
    consumer = Consumer(CONSUMER_CONFIG)

    db_manager = DatabaseManager(host="127.0.0.1", user="cmudc", passwd="cmudc_secure_password", db="outbox_events")
    consumer.subscribe([TOPIC])
    print(f"Listening for messages on topic: {TOPIC}")

    request_schema = fastavro.schema.load_schema(REQUEST_SCHEMA_FILE)
    response_schema = fastavro.schema.load_schema(RESPONSE_SCHEMA_FILE)
    error_schema = fastavro.schema.load_schema(ERROR_SCHEMA_FILE)

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
            try:
                response = handle_request(request)
                fastavro.validate(response, response_schema)
            except CookieOverdrawException as e:
                req_id = request.get("id")
                response = {
                            "request_id": req_id,
                            "errorMessage": str(e),
                            "errorCode": 1,
                            "errorType": "CookieOverdrawException"
                            }
                fastavro.validate(response, error_schema)

            print("Sending response: ", response)
            db_manager.insert_outbox_event("cookiejar-reply", response)

    db_manager.close()
    consumer.close()


if __name__ == "__main__":
    main()
