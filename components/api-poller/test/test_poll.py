import datetime
import itertools

import pytest
import time_machine
from dateutil.tz import tzutc
from polars.testing import assert_frame_equal

from src.app import poll_headers, poll_order_book, download_orders
import responses
import polars as pl

REGION_ID = '20241027'
BASE_URL = f'https://esi.evetech.net/markets/{REGION_ID}/orders'
DEFAULT_MOCK_RESPONSE_HEADERS = {
    'Expires': 'Sun, 27 Oct 2024 11:18:00 GMT',
    'Last-Modified': 'Sun, 27 Oct 2024 11:11:11 GMT',
    'X-Pages': '3',
}
DEFAULT_ORDER_BOOK_OPTIONS = {
    'expires': datetime.datetime(2024, 10, 27, 11, 18, 0, tzinfo=tzutc()),
    'last_modified': datetime.datetime(2024, 10, 27, 11, 11, 11, tzinfo=tzutc()),
    'pages': 3,
}
TEST_TIME = datetime.datetime(2024, 10, 27, 11, 15, 0, tzinfo=tzutc())

ORDERS = [
    [
        {
            "duration": 90,
            "is_buy_order": False,
            "issued": "2026-07-08T18:22:08Z",
            "location_id": 60013261,
            "min_volume": 1,
            "order_id": 7374577663,
            "price": 4999000.0,
            "range": "region",
            "system_id": 30001290,
            "type_id": 593,
            "volume_remain": 3,
            "volume_total": 3
        },
        {
            "duration": 90,
            "is_buy_order": False,
            "issued": "2026-07-19T07:28:19Z",
            "location_id": 60012577,
            "min_volume": 1,
            "order_id": 7382106122,
            "price": 3000.0,
            "range": "region",
            "system_id": 30001324,
            "type_id": 32442,
            "volume_remain": 20000,
            "volume_total": 20000
        },
    ],
    [
        {
            "duration": 90,
            "is_buy_order": False,
            "issued": "2026-07-20T16:02:24Z",
            "location_id": 60012568,
            "min_volume": 1,
            "order_id": 7383154702,
            "price": 2000000.0,
            "range": "region",
            "system_id": 30001269,
            "type_id": 2404,
            "volume_remain": 61,
            "volume_total": 63
        },
        {
            "duration": 90,
            "is_buy_order": False,
            "issued": "2026-07-20T16:27:04Z",
            "location_id": 60012568,
            "min_volume": 1,
            "order_id": 7383171089,
            "price": 30000000.0,
            "range": "region",
            "system_id": 30001269,
            "type_id": 12044,
            "volume_remain": 9,
            "volume_total": 9
        },
    ],
    [
        {
            "duration": 365,
            "is_buy_order": True,
            "issued": "2026-05-21T00:03:44Z",
            "location_id": 60012559,
            "min_volume": 1,
            "order_id": 911464523,
            "price": 30000.0,
            "range": "station",
            "system_id": 30001291,
            "type_id": 15996,
            "volume_remain": 1136,
            "volume_total": 1136
        },
        {
            "duration": 365,
            "is_buy_order": True,
            "issued": "2026-07-18T10:05:37Z",
            "location_id": 60012562,
            "min_volume": 1,
            "order_id": 911464524,
            "price": 30000.0,
            "range": "station",
            "system_id": 30001277,
            "type_id": 15996,
            "volume_remain": 1136,
            "volume_total": 1136
        },
    ]
]


def setup_head_mock(status: int = 200):
    responses.add(
        responses.Response(
            method='HEAD',
            url=BASE_URL,
            status=status,
            headers=DEFAULT_MOCK_RESPONSE_HEADERS,
        )
    )


def setup_pagination_mocks(total_pages: int, error_on_page: int = None, rollover_on_page: int = None):
    for page in range(1, total_pages + 1):
        status = 429 if page == error_on_page else 200

        headers = DEFAULT_MOCK_RESPONSE_HEADERS.copy()
        if page == rollover_on_page:
            headers['Last-Modified'] = 'Sun, 27 Oct 2024 11:14:00 GMT'

        responses.add(
            responses.GET,
            f"{BASE_URL}?page={page}",
            status=status,
            headers=headers,
            json=ORDERS[page - 1] if status == 200 else {"error": "rate limited"}
        )


@responses.activate
def test_poll_headers_fail_http_error():
    setup_head_mock(status=429)

    with pytest.raises(Exception, match="429"):
        poll_headers(region_id=REGION_ID)


@responses.activate
def test_poll_headers():
    setup_head_mock()
    headers = poll_headers(region_id=REGION_ID)
    assert headers == DEFAULT_ORDER_BOOK_OPTIONS


@responses.activate
@time_machine.travel(TEST_TIME)
def test_poll_order_book_fail_http_error_during_pagination():
    setup_pagination_mocks(total_pages=3, error_on_page=3)

    with pytest.raises(Exception, match="429"):
        poll_order_book(region_id=REGION_ID, options=DEFAULT_ORDER_BOOK_OPTIONS)


@responses.activate
@time_machine.travel(TEST_TIME)
def test_poll_order_book_fail_cache_rollover():
    setup_pagination_mocks(total_pages=3, rollover_on_page=2)
    with pytest.raises(Exception, match="last-modified"):
        poll_order_book(region_id=REGION_ID, options=DEFAULT_ORDER_BOOK_OPTIONS)


@responses.activate
@time_machine.travel(TEST_TIME)
def test_poll_order_book():
    setup_pagination_mocks(total_pages=3)

    order_book = poll_order_book(region_id=REGION_ID, options=DEFAULT_ORDER_BOOK_OPTIONS)
    orders_flat = list(itertools.chain.from_iterable(ORDERS))
    orders_flat_df = pl.from_dicts(orders_flat).with_columns(
        pl.lit(DEFAULT_ORDER_BOOK_OPTIONS['last_modified'].replace(tzinfo=None))
        .alias('timestamp')
    )
    assert_frame_equal(order_book, orders_flat_df)


@responses.activate
@time_machine.travel(TEST_TIME)
def test_download_orders(tmp_path):
    setup_head_mock()
    setup_pagination_mocks(total_pages=3)
    test_file = tmp_path / "test_orders.parquet"
    download_orders(region_id=REGION_ID, output_path=test_file)
    assert test_file.exists()
