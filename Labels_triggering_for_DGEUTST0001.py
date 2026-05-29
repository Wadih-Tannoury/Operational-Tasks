import json
import logging
import os

import requests
from google.cloud import bigquery
from google.oauth2 import service_account


BQ_CREDENTIALS_SECRET = "TIL_BIGQUERY_CREDS"

LOGIC_APP_URL = (
    "https://prod-09.northeurope.logic.azure.com/workflows/"
    "8fca928224de4819aa279973e08f9008/"
    "triggers/request/paths/invoke"
)

LOGIC_APP_PARAMS = {
    "api-version": "2016-10-01",
    "sp": "/triggers/request/run",
    "sv": "1.0",
    "sig": "5Ay_LV4SS9h3gw9zCfftJ0LabeCnfuig7BgkH9fcAU4",
}

QUERY = """
SELECT DISTINCT shipmentOrderNumber
FROM `tlg-wlfs-prd`.til.prd_tilEvents
WHERE eventType = 'FATV_RESULT'
  AND requestPayload LIKE '%"locationCode": "DGEUTST0001"%'
  AND eventTimestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10000 MINUTE)
  AND shipmentOrderNumber IS NOT NULL
"""


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


def load_json_from_secret(secret_name: str) -> dict:
    value = os.environ.get(secret_name)

    if not value:
        raise RuntimeError(f"Missing required environment variable: {secret_name}")

    return json.loads(value)


def get_bigquery_client():
    creds_info = load_json_from_secret(BQ_CREDENTIALS_SECRET)
    credentials = service_account.Credentials.from_service_account_info(creds_info)

    return bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )


def get_shipment_order_numbers(client):
    rows = client.query(QUERY).result()
    return [row["shipmentOrderNumber"] for row in rows]


def post_order(shipment_order_number: str):
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    body = {
        "orderNumber": shipment_order_number,
        "weight": None,
        "packagesNumber": 1,
    }

    response = requests.post(
        LOGIC_APP_URL,
        params=LOGIC_APP_PARAMS,
        headers=headers,
        json=body,
        timeout=30,
    )

    if not response.ok:
        logging.error("Status code: %s", response.status_code)
        logging.error("Response body: %s", response.text)
        logging.error("Final URL: %s", response.url)

    response.raise_for_status()
    return response.text


def main():
    client = get_bigquery_client()
    shipment_order_numbers = get_shipment_order_numbers(client)

    logging.info("Found %s shipmentOrderNumber(s)", len(shipment_order_numbers))

    processed = 0
    failed = 0

    for shipment_order_number in shipment_order_numbers:
        try:
            logging.info("Posting shipmentOrderNumber %s", shipment_order_number)
            post_order(shipment_order_number)
            processed += 1

        except Exception:
            failed += 1
            logging.exception(
                "Failed posting shipmentOrderNumber %s",
                shipment_order_number,
            )

    logging.info("Done. Processed=%s Failed=%s", processed, failed)


if __name__ == "__main__":
    main()
