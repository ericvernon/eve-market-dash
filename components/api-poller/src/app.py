from datetime import datetime, timezone
from pathlib import Path
import json
import os
import io

import dateutil.parser
import requests
import polars as pl
import boto3

MIN_TIME_PER_PAGE_IN_SECONDS = 1



def poll_headers(region_id: str) -> dict:
    url = f'https://esi.evetech.net/markets/{region_id}/orders'
    r = requests.head(url)
    r.raise_for_status()
    return {
        'pages': int(r.headers['X-Pages']),
        'expires': dateutil.parser.parse(r.headers['expires']),
        'last_modified': dateutil.parser.parse(r.headers['last-modified']),
    }


def poll_order_book(region_id: str, options: dict) -> pl.DataFrame:
    url = f'https://esi.evetech.net/markets/{region_id}/orders'

    expiry = options['expires']
    now = datetime.now(timezone.utc)
    seconds_to_expiry = (expiry - now).total_seconds()
    last_modified = options['last_modified']
    n_pages = options['pages']

    if seconds_to_expiry < (n_pages * MIN_TIME_PER_PAGE_IN_SECONDS):
        raise Exception(f'Only {seconds_to_expiry} seconds left to cache expiry, but {n_pages} pages. Aborting.')

    orders = []
    for page in range(1, n_pages + 1):
        params = {
            'page': page,
        }
        r = requests.get(url, params=params)
        r.raise_for_status()
        if dateutil.parser.parse(r.headers['last-modified']) != last_modified:
            raise Exception(f'last-modified mismatch fetching page {page}, aborting...')
        orders.extend(r.json())

    df = pl.from_dicts(orders).with_columns(
        pl.lit(last_modified.replace(tzinfo=None))
        .alias('timestamp')
    )
    return df


def lambda_handler(event, context):
    # 1. Configuration
    # Support SQS trigger (EventBridge → SQS → Lambda) as well as direct invocation.
    # When triggered via SQS, the payload is in event['Records'][0]['body'] as a JSON string.
    if 'Records' in event:
        body = json.loads(event['Records'][0]['body'])
        region_id = body.get('region_id', '10000015')
    else:
        region_id = event.get('region_id', '10000015')
    bucket_name = os.environ.get("RAW_DATA_BUCKET")
    
    if not bucket_name:
        raise ValueError("RAW_DATA_BUCKET environment variable is missing.")

    # 2. Scrape the data
    header_info = poll_headers(region_id)
    df = poll_order_book(region_id, header_info)
    
    # 3. Convert to Parquet entirely in-memory
    parquet_buffer = io.BytesIO()
    df.write_parquet(parquet_buffer)
    
    # 4. Upload to S3
    s3_client = boto3.client('s3')
    timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    
    # Data Lake partitioning best practice
    object_key = f"raw/region_id={region_id}/orders_{timestamp_str}.parquet"
    
    s3_client.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=parquet_buffer.getvalue()
    )
    
    return {
        "statusCode": 200,
        "body": f"Successfully uploaded {len(df)} orders to {object_key}"
    }
