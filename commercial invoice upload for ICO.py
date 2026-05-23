import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone

import requests
from google.cloud import bigquery
from google.oauth2 import service_account


API_CREDENTIALS_SECRET = "KIBO_API_CREDENTIALS"
BQ_CREDENTIALS_SECRET = "TIL_BIGQUERY_CREDS"

HOST = "tp3.mozu.com"
AUTH_URL = "https://home.mozu.com/api/platform/applications/authtickets/oauth"

COUNTRY_CODES = {"US", "UK", "GB"}
NOTE_MARKER = "_IUS"
IUS_NOTE_PATTERN = re.compile(r"(?:^|[^A-Z0-9])_?IUS[-_][A-Z0-9]+", re.IGNORECASE)
NOTE_TIME_TOLERANCE = timedelta(minutes=5)

QUERY = """
SELECT
  *,
  COALESCE(
    JSON_VALUE(requestPayload, '$.data.fatvResult.shipmentOrderNumber'),
    JSON_VALUE(requestPayload, '$.shipmentOrderNumber'),
    shipmentOrderNumber,
    orderNumber
  ) AS fatv_shipment_order_number,
  JSON_VALUE(requestPayload, '$.data.fatvResult.invoiceNumber') AS fatv_invoice_number,
  JSON_VALUE(requestPayload, '$.data.fatvResult.locationCode') AS fatv_location_code
FROM `tlg-wlfs-prd`.til.prd_tilEvents
WHERE eventType = 'FATV_RESULT'
  AND eventTimestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 MINUTE)
  AND COALESCE(orderNumber, '') NOT LIKE 'DJ%'
  AND (
    JSON_VALUE(requestPayload, '$.data.fatvResult.locationCode') LIKE '%ITST%'
    OR JSON_VALUE(requestPayload, '$.data.fatvResult.locationCode') = 'CVITWHRCLA'
    OR JSON_VALUE(requestPayload, '$.data.fatvResult.locationCode') LIKE '%FRST%'
    OR JSON_VALUE(requestPayload, '$.data.fatvResult.locationCode') LIKE '%ESST%'
  )
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


def parse_payload(payload):
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, str):
        return json.loads(payload)
    raise ValueError(f"Unsupported requestPayload type: {type(payload)}")


def get_extended_property(payload: dict, key: str):
    for item in payload.get("extendedProperties", []):
        if isinstance(item, dict) and item.get("key") == key:
            return item.get("value")
    return None


def get_first_value(payload: dict, *keys):
    for key in keys:
        value = payload.get(key)
        if value:
            return value

        data = payload.get("data") or {}
        if isinstance(data, dict):
            value = data.get(key)
            if value:
                return value

            for nested_value in data.values():
                if isinstance(nested_value, dict) and nested_value.get(key):
                    return nested_value.get(key)

        value = get_extended_property(payload, key)
        if value:
            return value

    return None


def parse_datetime(value):
    if not value:
        return None

    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def authenticate() -> str:
    creds = load_json_from_secret(API_CREDENTIALS_SECRET)

    response = requests.post(
        AUTH_URL,
        json={
            "grant_type": "client_credentials",
            "client_id": creds["client_id"],
            "client_secret": creds["client_secret"],
        },
        timeout=30,
    )
    response.raise_for_status()

    data = response.json()
    token = data.get("access_token") or data.get("accessToken")

    if not token:
        raise RuntimeError(f"No access token found in auth response: {data}")

    return token


def get_bigquery_client():
    creds_info = load_json_from_secret(BQ_CREDENTIALS_SECRET)

    credentials = service_account.Credentials.from_service_account_info(creds_info)

    return bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )


def get_bigquery_rows(client):
    return client.query(QUERY).result()


def get_order(token: str, tenant_id: str, site_id: str, order_id: str) -> dict:
    url = f"https://t{tenant_id}.{HOST}/api/commerce/orders/{order_id}"

    headers = {
        "Authorization": f"Bearer {token}",
        "x-vol-tenant": str(tenant_id),
        "x-vol-site": str(site_id),
        "Accept": "application/json",
    }

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    return response.json()


def get_shipment(
    token: str,
    tenant_id: str,
    site_id: str,
    oms_shipment_number: str,
) -> dict:
    url = (
        f"https://t{tenant_id}.{HOST}"
        f"/api/commerce/shipments?filter=shipmentNumber=={oms_shipment_number}"
    )

    headers = {
        "Authorization": f"Bearer {token}",
        "x-vol-tenant": str(tenant_id),
        "x-vol-site": str(site_id),
        "Accept": "application/json",
    }

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    return response.json()


def normalize_note_text(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def iter_shipment_records(shipment_response):
    if isinstance(shipment_response, list):
        return shipment_response

    if not isinstance(shipment_response, dict):
        return []

    embedded_shipments = shipment_response.get("_embedded", {}).get("shipments", [])
    if isinstance(embedded_shipments, list):
        return embedded_shipments

    for key in ("items", "shipments", "data"):
        value = shipment_response.get(key)
        if isinstance(value, list):
            return value

    return [shipment_response]


def iter_shipment_notes(shipment: dict):
    notes = shipment.get("shipmentNotes", [])
    if isinstance(notes, dict):
        notes = [notes]
    return notes or []


def shipment_note_exists(
    token: str,
    tenant_id: str,
    site_id: str,
    oms_shipment_number: str,
    note_text: str,
) -> bool:
    shipment_response = get_shipment(token, tenant_id, site_id, oms_shipment_number)
    target_note_text = normalize_note_text(note_text)

    for shipment in iter_shipment_records(shipment_response):
        if not isinstance(shipment, dict):
            continue

        for note in iter_shipment_notes(shipment):
            if not isinstance(note, dict):
                continue

            existing_note_text = normalize_note_text(note.get("noteText"))

            if existing_note_text == target_note_text:
                logging.info(
                    "Shipment %s already contains the same note. Skipping post.",
                    oms_shipment_number,
                )
                return True

    return False


def post_shipment_note(
    token: str,
    tenant_id: str,
    site_id: str,
    oms_shipment_number: str,
    note_text: str,
):
    url = (
        f"https://t{tenant_id}.{HOST}"
        f"/api/commerce/shipments/{oms_shipment_number}/notes"
    )

    headers = {
        "Authorization": f"Bearer {token}",
        "x-vol-tenant": str(tenant_id),
        "x-vol-site": str(site_id),
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    response = requests.post(
        url,
        headers=headers,
        json={"noteText": note_text},
        timeout=30,
    )
    response.raise_for_status()

    return response.json() if response.content else None


def extract_country_code(order: dict):
    return (
        order.get("fulfillmentInfo", {})
        .get("fulfillmentContact", {})
        .get("address", {})
        .get("countryCode")
    )


def find_note_containing(order: dict, value: str):
    if not value:
        return None

    for note in iter_order_notes(order):
        text = note.get("text", "") if isinstance(note, dict) else str(note)
        if value in text:
            return text

    return None


def is_ius_note_text(text: str) -> bool:
    if not text:
        return False

    upper_text = str(text).upper()

    if any(
        marker in upper_text
        for marker in ("_IUS-", "_IUS_", "/IUS-", "/IUS_", "-IUS-", "-IUS_")
    ):
        return True

    return bool(IUS_NOTE_PATTERN.search(upper_text))


def iter_order_notes(order: dict):
    notes = order.get("notes", [])

    if isinstance(notes, dict):
        notes = [notes]

    return notes or []


def find_ius_notes(order: dict):
    matching_notes = []

    for note in iter_order_notes(order):
        if isinstance(note, str):
            text = note
            note_obj = {"text": note}
        elif isinstance(note, dict):
            text = note.get("text", "")
            note_obj = note
        else:
            continue

        if is_ius_note_text(text):
            matching_notes.append(note_obj)

    return matching_notes


def note_create_date(note: dict):
    return parse_datetime(
        (note.get("auditInfo") or {}).get("createDate")
        or note.get("createDate")
        or note.get("auditCreateDate")
    )


def find_ius_note(order: dict, fatv_event_timestamp=None):
    matching_notes = find_ius_notes(order)

    if not matching_notes:
        logging.info("No notes with an IUS marker were found on the order")
        return None

    logging.info("Found %s note(s) with an IUS marker", len(matching_notes))

    if len(matching_notes) == 1:
        return matching_notes[0].get("text")

    if not fatv_event_timestamp:
        logging.info(
            "Found multiple IUS notes, but no FATV_RESULT eventTimestamp was found for time matching"
        )
        return None

    for note in matching_notes:
        created_at = note_create_date(note)

        if created_at and abs(created_at - fatv_event_timestamp) <= NOTE_TIME_TOLERANCE:
            return note.get("text")

    logging.info(
        "Found IUS notes, but none has auditInfo.createDate within +/- %s minutes of FATV_RESULT eventTimestamp %s",
        int(NOTE_TIME_TOLERANCE.total_seconds() / 60),
        fatv_event_timestamp,
    )

    return None


def main():
    token = authenticate()
    logging.info("Authenticated successfully")

    client = get_bigquery_client()
    rows = get_bigquery_rows(client)

    processed = 0
    skipped = 0
    failed = 0

    for row in rows:
        try:
            payload = parse_payload(row["requestPayload"])

            tenant_id = (
                payload.get("x-vol-tenant")
                or get_first_value(payload, "tenantId")
            )

            site_id = (
                payload.get("x-vol-site")
                or get_first_value(payload, "siteId")
            )

            order_id = get_first_value(payload, "orderId")

            oms_shipment_number = (
                payload.get("entityId")
                or get_first_value(payload, "shipmentNumber")
            )

            shipment_order_number = (
                row.get("fatv_shipment_order_number")
                or get_first_value(payload, "shipmentOrderNumber", "orderNumber")
                or row.get("shipmentOrderNumber")
                or row.get("orderNumber")
            )

            fatv_invoice_number = row.get("fatv_invoice_number")

            fatv_event_timestamp = parse_datetime(
                payload.get("eventTimestamp") or row.get("eventTimestamp")
            )

            if not all(
                [
                    tenant_id,
                    site_id,
                    order_id,
                    oms_shipment_number,
                    shipment_order_number,
                ]
            ):
                skipped += 1
                logging.warning(
                    "Skipping row because required values are missing: "
                    "tenant=%s site=%s order=%s shipment=%s shipmentOrderNumber=%s",
                    tenant_id,
                    site_id,
                    order_id,
                    oms_shipment_number,
                    shipment_order_number,
                )
                continue

            order = get_order(token, tenant_id, site_id, order_id)
            country_code = extract_country_code(order)

            if country_code not in COUNTRY_CODES:
                skipped += 1
                logging.info(
                    "Skipping order %s because countryCode is %s",
                    order_id,
                    country_code,
                )
                continue

            if str(shipment_order_number).startswith("CV"):
                if not fatv_invoice_number:
                    skipped += 1
                    logging.info(
                        "Skipping CV shipment %s because no FATV invoiceNumber was found",
                        shipment_order_number,
                    )
                    continue

                note_text = find_note_containing(order, fatv_invoice_number)

                if not note_text:
                    skipped += 1
                    logging.info(
                        "Skipping CV shipment %s because no note containing FATV invoiceNumber %s was found",
                        shipment_order_number,
                        fatv_invoice_number,
                    )
                    continue

            else:
                note_text = find_ius_note(order, fatv_event_timestamp)

                if not note_text:
                    skipped += 1
                    logging.info(
                        "Skipping order %s because no valid note containing %s was found",
                        order_id,
                        NOTE_MARKER,
                    )
                    continue

            if shipment_note_exists(
                token=token,
                tenant_id=tenant_id,
                site_id=site_id,
                oms_shipment_number=oms_shipment_number,
                note_text=note_text,
            ):
                skipped += 1
                logging.info(
                    "Skipping shipment %s because the same note already exists",
                    oms_shipment_number,
                )
                continue

            post_shipment_note(
                token=token,
                tenant_id=tenant_id,
                site_id=site_id,
                oms_shipment_number=oms_shipment_number,
                note_text=note_text,
            )

            processed += 1
            logging.info(
                "Posted note for order %s / shipment %s",
                order_id,
                oms_shipment_number,
            )

        except Exception as e:
            failed += 1
            logging.exception("Failed processing row: %s", e)

    logging.info(
        "Done. Processed=%s Skipped=%s Failed=%s",
        processed,
        skipped,
        failed,
    )


if __name__ == "__main__":
    main()
