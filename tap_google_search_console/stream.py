"""Custom client handling, including GoogleSearchConsoleStream base class."""
import pendulum
import logging
import sys
import copy
import json

from typing import Any, Dict, Optional, Iterable
from singer_sdk import typing as th
from google.oauth2 import service_account
from googleapiclient.discovery import build, Resource
from singer_sdk.streams import Stream
from pathlib import Path
from datetime import datetime


default_dimensions = ["date", "page", "query"]
row_limit = 25000

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

        # validate that the client_secrets.json file exists and load it
        if Path(self.config['service_account_key']).is_file():
            try:
                service_account_object = load_json(self.config['service_account_key'])
            except ValueError:
                logging.error("tap-google-analytics: The JSON definition in '{}' has errors".format(self.config['service_account_key']))
                sys.exit(1)
        else:
            logging.error("tap-google-analytics: '{}' file not found".format(self.config['service_account_key']))
            sys.exit(1)

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

    def _get_next_page_token(self, response: dict, previous_token: Any) -> Any:
        """Return token identifying next page or None if all records have been read.
        Args:
        ----
            response: A dict object.
        Return:
        ------
            Reference value to retrieve next page.
        .. _requests.Response:
            https://docs.python-requests.org/en/latest/api/#requests.Response
        """

        if len(response['rows']) == row_limit:
            return previous_token + row_limit


    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        next_page_token = 0
        finished = False

        while not finished:
            query_payload = {
                'startDate': self.start_date(context=context),
                'endDate': self.end_date(),
                'dimensions': self.dimensions,
                'rowLimit': row_limit,
                'startRow': next_page_token,
            }
            resp = self.query(query_payload)

            logging.info("resp length: '{}'".format(len(resp['rows'])))
            logging.info("startRow: '{}'".format(query_payload['startRow']))

            for row in resp['rows']:
                data = {}

                for i in range(len(query_payload['dimensions'])):
                    data[query_payload['dimensions'][i]] = row['keys'][i]

                data[query_payload['dimensions'][0]] = datetime.strptime(row['keys'][0], '%Y-%m-%d')

                data['clicks'] = row['clicks']
                data['impressions'] = row['impressions']
                data['ctr'] = round(row['ctr'] * 100, 2)
                data['position'] = round(row['position'], 2)

                yield data
            
            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self._get_next_page_token(response=resp, previous_token=previous_token)

            
            if next_page_token and next_page_token == previous_token:
                raise RuntimeError(
                    f"Loop detected in pagination. "
                    f"Pagination token {next_page_token} is identical to prior token."
                )
            # Cycle until get_next_page_token() no longer returns a value
            finished = not next_page_token
def load_json(path):
    with open(path) as f:
        return json.load(f)
