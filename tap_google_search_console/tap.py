"""GoogleSearchConsole tap class."""
from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_google_search_console.stream import GoogleSearchConsoleStream


class TapGoogleSearchConsole(Tap):
    """GoogleSearchConsole tap class."""

    name = "tap-google-search-console"

    config_jsonschema = th.PropertiesList(
        th.Property("site_url", th.StringType, required=True),
        th.Property("key_file_location", th.StringType, required=True),
        th.Property("start_date", th.DateTimeType, required=True),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        stream = GoogleSearchConsoleStream(tap=self)

        return [stream]
