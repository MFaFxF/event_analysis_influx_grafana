from _lib import *
import logging

def load_events(time_step:int=TIME_WINDOW, version=VERSION, article_code_digits=3):
    """
    get parsed event data and load to influx as points
    :return:
    """

    # specify file paths
    purchase_event_files = get_all_file_paths(PURCHASE_EVENT_DIR)
    content_event_files = get_all_file_paths(CONTENT_EVENT_DIR)

    content_event_generator = event_generator(content_event_files)
    purchase_event_generator = event_generator(purchase_event_files)

    # get the total timeframe of the data
    print("Getting total timeframe of data...")
    total_time_frame = get_total_timeframe(purchase_event_files + content_event_files)

    # normalize time frame borders to 00:00H
    total_time_frame = trim_timeframe(total_time_frame)

    print(f"Found events from "
          f"{convert_timestamp_to_date(total_time_frame[0])}"
          f" to "
          f"{convert_timestamp_to_date(total_time_frame[1])}")
    
    # get execution start time and total batch number for progress messages
    start_time = time.time()
    print(f"Time window: {int(time_step/86400000)}")
    total_batches = (total_time_frame[1] - total_time_frame[0]) // time_step

    # initialize loop variables
    current_time_window = (total_time_frame[0], total_time_frame[0] + time_step)
    current_batch = 0
    batch = []

    # specify fields for point generation
    product_fields = [
        "amount",
        "total"
    ]

    purchase_fields = [
        "itemCount",
        "itemValue",
        "mixedItemCount",
        "mixedItemValue",
        "referralItemCount",
        "referralItemValue"
    ]

    while current_time_window[1] < total_time_frame[1]:
        current_time_window = (current_time_window[0] + time_step, current_time_window[1] + time_step)
        print("--------------------------------------------------"
            f"\nProcessing events from {convert_timestamp_to_date(current_time_window[0])}"
            f" to "
            f"{convert_timestamp_to_date(current_time_window[1])}")
        print_progress_message(start_time, current_batch, total_batches)

        current_batch += 1

        # get content events from all content event files for current window
        content_events = parse_content_events(content_event_generator, current_time_window, article_code_digits)

        # convert to influx points
        for event in content_events:
            point = (
                Point("event_data")
                .tag("bereich", event["bereich"])
                .tag("version", version)
                .tag("type", event["type"])
                .tag("devicetype", event["devicetype"])
                .tag("Artikelcode", event["Artikelcode"])
                .tag("article_code_digits", event["article_code_digits"])
                .tag("time_window", TIME_LABEL)
                .field("count", event["count"])
                .time(event["time"], WritePrecision.MS)
            )

            if event["type"] == "loadSet":
                point.tag("isfound", event["isfound"])

            batch.append(point)

            if len(batch) > BATCH_SIZE:
                try:
                    write_api.write(bucket=BUCKET, org=ORG, record=batch)
                    batch = []
                except Exception as e:
                    print(f"Error writing batch to InfluxDB: {e}")

        # get purchase events for current window
        purchase_events = parse_purchase_events(purchase_event_generator, current_time_window, article_code_digits)

        # convert to influx points
        for event in purchase_events:
            point = (
                Point("event_data")
                .tag("version", version)
                .tag("type", event["type"])
                .tag("devicetype", event["devicetype"])
                .tag("time_window", TIME_LABEL)
                .field("count", event["count"])
                .time(event["time"], WritePrecision.MS)
            )

            if event["type"] == "purchased_product":
                for product_field in product_fields:
                    point.field(product_field, event[product_field])
                for product_tag in ["referralProduct", "bereich", "Artikelcode", "article_code_digits"]:
                    point.tag(product_tag, event[product_tag])

            if event["type"] == "purchase":
                for purchase_field in purchase_fields:
                    point.field(purchase_field, event[purchase_field])
                for purchase_tag in ["referralOrder"]:
                    point.tag(purchase_tag, event[purchase_tag])

            if event["type"] == "referral":
                for referral_tag in ["bereich", "Artikelcode", "article_code_digits"]:
                    point.tag(referral_tag, event[referral_tag])

            batch.append(point)

            if batch and len(batch) > BATCH_SIZE:
                try:
                    write_api.write(bucket=BUCKET, org=ORG, record=batch)
                    batch = []
                except Exception as e:
                    print(f"Error writing batch to InfluxDB: {e}")
    
    if batch:
        write_api.write(bucket=BUCKET, org=ORG, record=batch)
        batch = []

    print("Events uploaded successfully!")


if __name__ == "__main__":
    load_events(article_code_digits=4, time_step=1*DAY, version="B")
    # load_events(article_code_digits=4)
