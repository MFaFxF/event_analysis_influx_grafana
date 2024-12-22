from datetime import datetime, timedelta

import ijson
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import csv
import os
import time
import re

# Constants
TOKEN = "eY5VzP0FdEOth91SAwpzgTTUxf9FUN9TgECaBv8h5mmaTC1NGZkZNhfuJF6hHWUzrJBvYPnUp4__fC6jnSEzUQ==" # replace with new token from InfluxDB UI -> API Tokens
ORG = "admin"
URL = "http://localhost:8086"
BUCKET = "events"

# Filepaths
# Base directory (e.g., where your script is running or a predefined root)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
#
# # Full paths
CSV_TABLE = os.path.join(BASE_DIR, 'data', 'product_data', 'product_data.csv')
CONTENT_EVENT_DIR = os.path.join(BASE_DIR, 'data', 'events', 'content-events')
PURCHASE_EVENT_DIR = os.path.join(BASE_DIR, 'data', 'events', 'purchase-events')

CONTENT_EVENT_FILE_1 = os.path.join(CONTENT_EVENT_DIR,'content-events.json')
PURCHASE_EVENT_FILE_1 = os.path.join(PURCHASE_EVENT_DIR, 'purchase-events.json')

VERSION = "A"

DAY = 86400000
TIME_WINDOW = 7 * DAY  # defines the time window for each batch
BATCH_SIZE = 1000  # number of points to write in each batch

TIME_LABEL = f"{int(TIME_WINDOW/86400000)}d"

FIELD_LABELS = [
    "count",
    "itemCount",
    "itemValue",
    "mixedItemCount",
    "mixedItemValue",
    "referralItemCount",
    "referralItemValue",
    "amount",
    "total"
]

EVENT_LABELS = [
    "bereich",
    "version",
    "type",
    "devicetype",
    "Artikelcode",
    "digits_Artikelcode",
    "isfound",
    "referralProduct",
    "referralOrder",
    "time_window"
]

# Connect to InfluxDB
client = InfluxDBClient(url=URL, token=TOKEN, org=ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

def get_all_file_paths(directory):
    """Generate a list of all file paths in the specified directory."""
    return [os.path.join(directory, filename) for filename in os.listdir(directory) if os.path.isfile(os.path.join(directory, filename))]


def print_progress_message(start_time, current_batch, total_batches):
    print(f"Day {current_batch}/{total_batches}")

    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    hours, rem = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(rem, 60)

    # Display elapsed time
    if hours >= 1:
        print(f"Elapsed time: {int(hours)}h {int(minutes)}m {int(seconds)}s.")
    elif minutes >= 1:
        print(f"Elapsed time: {int(minutes)}m {int(seconds)}s.")
    else:
        print(f"Elapsed time: {int(seconds)}s.")

    # Calculate and display estimated remaining time
    if current_batch > 0:  # Avoid division by zero
        avg_time_per_batch = elapsed_time / current_batch
        remaining_batches = total_batches - current_batch
        remaining_time = avg_time_per_batch * remaining_batches
        rem_hours, rem_rem = divmod(remaining_time, 3600)
        rem_minutes, rem_seconds = divmod(rem_rem, 60)

        if rem_hours >= 1:
            print(f"Estimated remaining time: {int(rem_hours)}h {int(rem_minutes)}m {int(rem_seconds)}s.")
        elif rem_minutes >= 1:
            print(f"Estimated remaining time: {int(rem_minutes)}m {int(rem_seconds)}s.")
        else:
            print(f"Estimated remaining time: {int(rem_seconds)}s.")


def load_csv_as_dict(filepath, key_field):
    try:
        with open(filepath) as f:
            reader = csv.DictReader(f)
            return {
                row[key_field]: {k: v for k, v in row.items() if k != key_field}
                for row in reader
            }
    except FileNotFoundError:
        print(f"File not found: {filepath}")
        return {}


def get_product_data(product_id):
    try:
        if len(product_id) == 22:  # just in case
            sku = product_id[:14]
            return product_data_by_mastersku.get(sku, {})
        elif len(product_id) == 14:  # always use mastersku
            return product_data_by_mastersku.get(product_id, {})
    except:
        return {}
    return {}


def parse_attributes(attribute_str):
    if not attribute_str:
        return {}
    attributes_dict = {}
    for attr in attribute_str.strip('"{}').split(','):
        if '-' in attr:
            attr_name, attr_value = attr.split('-', 1)
            attributes_dict.setdefault(attr_name, []).append(attr_value)
    return {k: sorted(v) for k, v in attributes_dict.items()}


def filter_out_attributes(attributes_dict, excluded_keys, only_keys):
    if only_keys:
        return {k: v for k, v in attributes_dict.items() if k in only_keys}
    return {k: v for k, v in attributes_dict.items() if k not in excluded_keys}


def get_attributes(product_id, excluded_keys=[], only_keys=["bereich"]):
    product = get_product_data(product_id)
    attributes = parse_attributes(product.get('attributes', ''))
    return filter_out_attributes(attributes, excluded_keys, only_keys)


def get_bereich(product_id):
    try:
        return get_attributes(product_id, only_keys=["bereich"])["bereich"][0] # there should be only one bereich
    except KeyError:
        return 'not_found'


def get_artikelcode(product_id, digits):
    product_data = get_product_data(product_id)
    artikelcode = product_data.get('Artikelcode', '')
    return artikelcode[:digits]


def get_first_and_last_timestamps(file_path):
    """Extract the first and last timestamps using regex."""
    start_time = end_time = None

    with open(file_path, 'r') as file:
        for line in file:
            if "processedPurchase" in line:
                time_key = "timestamp"
            else:
                time_key = "time"
            time_pattern = re.compile(rf'"{time_key}":(\d+)')
            match = time_pattern.search(line)
            if match:
                timestamp = int(match.group(1))
                if start_time is None:
                    start_time = timestamp  # Set the first timestamp
                end_time = timestamp  # Update the last timestamp (last match)

    return start_time, end_time


def sort_files_by_first_timestamp(filepaths):
    return sorted(filepaths, key=lambda x: get_first_and_last_timestamps(x)[0])


def get_total_timeframe(filepaths):
    total_timeframe = get_first_and_last_timestamps(filepaths[0])
    for filepath in filepaths[1:]:
        timeframe = get_first_and_last_timestamps(filepath)
        total_timeframe = ( min(timeframe[0], total_timeframe[0]), max(timeframe[1], total_timeframe[1]))
    return total_timeframe


def trim_timeframe(timeframe):
    """
    cuts off corners so timeframe starts and ends at 00:00h german time
    :param timeframe:
    :return:
    """
    t0 = timeframe[0] + 86400000 - (timeframe[0] % 86400000) + 3600000
    t1 = timeframe[1] - timeframe[1] % 86400000 + 3600000
    return t0, t1


def normalize_time(time):
    """Normalize a timestamp to next 00:00 german time"""
    return time - (time % 86400000) + 86400000 + 3600000


def convert_timestamp_to_date(timestamp):
    """Convert a timestamp in milliseconds to a human-readable date in UTC+1."""
    return (datetime.utcfromtimestamp(timestamp / 1000) + timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')


def matching_labels(entry1, entry2, labels_to_match, ignore_labels=[]):
    for label in labels_to_match:
        if label in ignore_labels:
            continue
        if entry1.get(label, "") != entry2.get(label, ""):
            return False
    return True


def event_generator(filepaths):
    last_timestamp = 0
    for filepath in filepaths:
        with open(filepath) as content_json:
            for event in ijson.items(content_json, 'item'):
                # avoid counting events twice in case of file overlap
                try:
                    if event["timestamp"] < last_timestamp:
                        continue
                    last_timestamp = event["timestamp"]
                except KeyError:
                    if event["time"] < last_timestamp:
                        continue
                    last_timestamp = event["time"]
                yield event


def parse_content_events(content_event_generator, time_window, article_code_digits=3):
    print("parsing content events...")
    event_data = []

    for event in content_event_generator:
    # for i, event in enumerate(ijson.items(content_json, 'item'))[10000:]:
    #     print(i)
        if event["time"] < time_window[0]:
            continue
        if event["time"] > time_window[1]:
            return event_data

        trigger_sku = event["data"]["widget"]["sku"]

        entry = {
            "count": 1,
            "time": time_window[1],
            "type": event["type"],
            "devicetype": event["meta"]["devicetype"],
            "bereich": get_bereich(trigger_sku),
            "Artikelcode": get_artikelcode(trigger_sku, article_code_digits),
            "article_code_digits": article_code_digits
                }

        if entry["type"] == "loadSet":
            entry["isfound"] = event["data"]["content"]["isfound"]

        if entry["type"] == "viewSet":
            pass

        if entry["type"] == "viewMerchantDetails":
            pass
            # labels specific to this event, might be useful at some point
            # try:
            #     entry["istriggerproduct"] = event["data"]["products"][0]["istriggerproduct"]
            #     entry["productpositionwithinslot"] = event["data"]["products"][0]["productpositionwithinslot"]
            #     entry["slotposition"] = event["data"]["products"][0]["slotposition"]
            #     entry["referral_bereich"] = get_bereich(event["data"]["products"][0]["sku"])
            # except KeyError:
            #     continue

        # combine events into buckets of identical label combinations, count events for each bucket
        initial_entry = True
        for j, bucket in enumerate(event_data):
            if matching_labels(entry, bucket, EVENT_LABELS):
                event_data[j]["count"] += 1
                initial_entry = False

        if initial_entry:
            event_data.append(entry)

    print("content event buckets: ", len(event_data))
    return event_data


def parse_purchase_events(purchase_event_generator, time_window: list, article_code_digits=3):
    print("parsing purchase events...")
    event_data = []

    for event in purchase_event_generator:
        # skip events outside of current time window
        if event["timestamp"] < time_window[0]:
            continue
        if event["timestamp"] > time_window[1]:
            return event_data

        # skip empty purchases
        if not event["processedPurchase"]["data"]["products"]: 
            continue

        devicetype = event["processedPurchase"]["meta"]["devicetype"]

        # count products
        for product in event["processedPurchase"]["data"]["products"]:
            sku = product["sku"]

            entry = {
                "count": 1,
                "time": time_window[1],
                "amount": product["amount"],
                "total": product["total"],
                "type": "purchased_product",
                "referralProduct": True if product.get("referralProduct") else False,
                "bereich": get_bereich(sku),
                "Artikelcode": get_artikelcode(sku, article_code_digits),
                "article_code_digits": article_code_digits,
                "devicetype": devicetype,
            }

            product_fields = [
                "count",
                "amount",
                "total"
            ]

            initial_entry = True
            for j, bucket in enumerate(event_data):
                if matching_labels(entry, bucket, labels_to_match=EVENT_LABELS):
                    event_data[j]["count"] += entry["count"]
                    event_data[j]["amount"] += product["amount"]
                    event_data[j]["total"] += product["total"]
                    initial_entry = False

            if initial_entry:
                event_data.append(entry)

            # count referrals for this product
            if product["referralProduct"] == 1:
                # keep track of referrals for purchased products => conversion rate
                for referral in product["referrals"]:
                    sku = referral["data"]["products"][0]["properties"]["parentSku"]
                    devicetype = referral["meta"]["devicetype"]

                    entry = {
                        "count": 1,
                        "type": "referral",
                        "devicetype": devicetype,
                        "bereich": get_bereich(sku),
                        "Artikelcode": get_artikelcode(sku, article_code_digits),
                        "article_code_digits": article_code_digits,
                        "time": normalize_time(referral["time"]) # referral time mostly outside of current time window
                    }

                    initial_entry = True
                    for j, bucket in enumerate(event_data): # fill buckets with identical label combinations
                        if matching_labels(entry, bucket, labels_to_match=entry.keys(), ignore_labels=["count"]):  # we don't ignore time here
                            event_data[j]["count"] += entry["count"]
                            initial_entry = False

                    if initial_entry:
                        event_data.append(entry)

        # count current purchase
        entry = {
            "count": 1,
            "time": time_window[1],
            "type": "purchase",
            "itemCount": event["itemCount"],
            "itemValue": event["itemValue"],
            "mixedItemCount": event["mixedItemCount"],
            "mixedItemValue": event["mixedItemValue"],
            "referralItemCount": event["referralItemCount"],
            "referralItemValue": event["referralItemValue"],
            "referralOrder": True if event["referralOrder"] == 1 else False,
            "devicetype": devicetype,
                }

        purchase_fields = [
            "count",
            "itemCount",
            "itemValue",
            "mixedItemCount",
            "mixedItemValue",
            "referralItemCount",
            "referralItemValue"
        ]

        initial_entry = True
        for j, bucket in enumerate(event_data):
            if matching_labels(entry, bucket, labels_to_match=entry.keys(), ignore_labels=purchase_fields + ["time"]):
                # we add all fields here
                for field in purchase_fields:
                    event_data[j][field] += entry[field]
                initial_entry = False

        if initial_entry:
            event_data.append(entry)

    print("purchase event buckets: ", len(event_data))
    return event_data


product_data_by_mastersku = load_csv_as_dict(CSV_TABLE, 'mastersku')

if __name__ == '__main__':
    t0, t1 = get_first_and_last_timestamps(CONTENT_EVENT_FILE_1)
    print(t0, t1)
    t0, t1 = get_first_and_last_timestamps(PURCHASE_EVENT_FILE_1)
    print(t0, t1)
    exit()
    content_events = parse_content_events(CONTENT_EVENT_FILE_1, [t0, t0 + DAY])
    purchase_events = parse_purchase_events(PURCHASE_EVENT_FILE_1, [t0, t0 + DAY])

    print("content event sample: \n", content_events[0:5])
    print("individual content event buckets: ", len(content_events))
    print("purchase event sample: \n", purchase_events[0:5])
    print("individual purchase event buckets: ", len(purchase_events))
