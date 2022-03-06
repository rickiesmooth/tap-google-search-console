"""Custom client handling, including GoogleSearchConsoleStream base class."""
import pendulum
import logging

from typing import Any, Dict, Optional, Iterable
from singer_sdk import typing as th
from google.oauth2 import service_account
from googleapiclient.discovery import build, Resource

from singer_sdk.streams import Stream
import json

default_dimensions = ["date", "page", "device", "query", "country"]


class GoogleSearchConsoleStream(Stream):
    """Stream class for GoogleSearchConsole streams."""

    name = "google-search-console"
    replication_key = "date"
    dimensions = default_dimensions

    schema = th.PropertiesList(
        th.Property("date", th.DateTimeType),
        th.Property("page", th.StringType),
        th.Property("device", th.StringType),
        th.Property("query", th.StringType),
        th.Property("country", th.StringType),
        th.Property("clicks", th.IntegerType),
        th.Property("impressions", th.IntegerType),
        th.Property("ctr", th.NumberType),
        th.Property("position", th.NumberType),
    ).to_dict()

    def start_date(self, context: dict) -> str:
        start_date = self.get_starting_timestamp(context).add(days=1).format("YYYY-MM-DD")
        logging.info(f"Starting from date: {start_date}")
        return start_date

    def end_date(self) -> str:
        return pendulum.now().subtract(days=1).format("YYYY-MM-DD")

    @property
    def google_service(self) -> Resource:
        """Create a connection to the Google Search Console API and return service object.

        Args:
            key (string): Google Search Console JSON client secrets path.

        Returns:
            service (Resource): Google Search Console service object.
        """

        service_account_object = json.loads(self.config['service_account_key'], strict=False)
        credentials = service_account.Credentials.from_service_account_info(
            service_account_object,
            scopes=['https://www.googleapis.com/auth/webmasters'],
        )

        service = build('webmasters', 'v3', credentials=credentials)

        return service

    def query(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        response = (
            self.google_service.searchanalytics()
            .query(siteUrl=self.config['site_url'], body=payload)
            .execute()
        )

        return response

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        query_payload = {
            'startDate': self.start_date(context=context),
            'endDate': self.end_date(),
            'dimensions': self.dimensions,
            'rowLimit': 25000,
            'startRow': 0,
        }
        response = self.query(query_payload)

        for row in response['rows']:
            data = {}

            for i in range(len(query_payload['dimensions'])):
                data[query_payload['dimensions'][i]] = row['keys'][i]

            data['clicks'] = row['clicks']
            data['impressions'] = row['impressions']
            data['ctr'] = round(row['ctr'] * 100, 2)
            data['position'] = round(row['position'], 2)

            yield data
