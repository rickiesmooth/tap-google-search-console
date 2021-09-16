"""Custom client handling, including GoogleSearchConsoleStream base class."""

import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable
from singer_sdk import typing as th  # JSON schema typing helpers
from google.oauth2 import service_account
from googleapiclient.discovery import build, Resource

from singer_sdk.streams import Stream

default_dimensions = [
    "date",
    "page", 
    "device", 
    "query", 
    "country"
]

class GoogleSearchConsoleStream(Stream):
    """Stream class for GoogleSearchConsole streams."""
    name = 'google_search_console'

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

    dimensions = default_dimensions

    primary_keys = default_dimensions

    @property
    def google_service(self) -> Resource:
        """Create a connection to the Google Search Console API and return service object.
        
        Args:
            key (string): Google Search Console JSON client secrets path.
        
        Returns:
            service (Resource): Google Search Console service object.
        """
        
        scope = ['https://www.googleapis.com/auth/webmasters']
        credentials = service_account.Credentials.from_service_account_file(
            self.config['key_file_location'], 
            scopes=scope
        )

        service = build(
            'webmasters',
            'v3',
            credentials=credentials
        )
    
        return service

    @property
    def query_payload(self) -> Dict[str, Any]:
        """Return a dictionary of query parameters.

        The Google Search Console API expects the payload to be a dictionary of query
        parameters.

        Returns:
            payload (Dict[str, Any]): Dictionary of query parameters.
        """
        
        payload = {
            'startDate': "2019-01-01",
            'endDate': "2021-09-15",
            'dimensions': self.dimensions,
            'rowLimit': 25000,
            'startRow': 0
        }

        return payload

    def query(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        response = (self.google_service
                        .searchanalytics()
                        .query(siteUrl=self.config['site_url'], body=payload)
                        .execute())

        return response

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.
        """
        query_payload = self.query_payload
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
