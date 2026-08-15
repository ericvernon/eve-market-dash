from datetime import datetime, timezone
from pathlib import Path

import dateutil.parser
import requests
import polars as pl

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


def download_orders(region_id: str, output_path: Path) -> None:
    header_info = poll_headers(region_id)
    df = poll_order_book(region_id, header_info)
    df.write_parquet(output_path)


def main():
    region_id = '10000015'  # Venal
    download_orders(region_id, Path('orders.parquet'))


if __name__ == '__main__':
    main()
