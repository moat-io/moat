import math
from urllib.parse import urlencode

from models import AttributeDto
from pydantic import BaseModel, Field

DEFAULT_PAGE_SIZE: int = 20
PAGE_SIZE_OPTIONS: list[int] = [10, 20, 50, 100]

ACTIVE_ANY: str = "any"
ACTIVE_TRUE: str = "true"
ACTIVE_FALSE: str = "false"

# query parameters that describe *what* is selected rather than *how much* of it
# is shown. These are the ones a CSV download has to replay to match the table.
FILTER_FIELD_NAMES: list[str] = [
    "search_term",
    "scope",
    "attributes",
    "source_type",
    "platform",
    "object_type",
    "active",
]


class TableQueryVm(BaseModel):
    @property
    def page_count(self) -> int:
        return math.ceil(float(self.record_count) / self.page_size)

    @property
    def previous_page_number(self) -> int:
        return 0 if self.page_number == 0 else self.page_number - 1

    @property
    def next_page_number(self) -> int:
        return min(self.page_number + 1, max(self.page_count - 1, 0))

    @property
    def page_start_record(self) -> int:
        return self.page_number * self.page_size + 1

    @property
    def page_end_record(self) -> int:
        return min((self.page_number + 1) * self.page_size, self.record_count)

    @property
    def next_page_disabled(self) -> bool:
        return self.page_end_record == self.record_count

    @property
    def previous_page_disabled(self) -> bool:
        return self.page_number == 0

    @property
    def attribute_dtos(self) -> list[AttributeDto] | None:
        """
        Attribute filters arrive as 'key:value' strings. The value may itself
        contain a colon, so only the first one separates key from value.
        """
        if not self.attributes:
            return None

        attribute_dtos: list[AttributeDto] = []
        for attr_str in self.attributes:
            attribute_key, separator, attribute_value = attr_str.partition(":")
            if not separator:
                continue
            attribute_dtos.append(
                AttributeDto(
                    attribute_key=attribute_key, attribute_value=attribute_value
                )
            )

        return attribute_dtos or None

    @property
    def active_filter(self) -> bool | None:
        """None means 'do not filter on active at all'."""
        if self.active == ACTIVE_TRUE:
            return True
        if self.active == ACTIVE_FALSE:
            return False
        return None

    @property
    def filter_count(self) -> int:
        """Number of advanced filters applied, for the badge on the filter button."""
        count: int = len(self.attributes or [])
        count += len(self.source_type or [])
        count += len(self.platform or [])
        count += len(self.object_type or [])
        if self.active_filter is not None:
            count += 1
        return count

    @property
    def filter_chips(self) -> list[dict[str, str]]:
        """
        Applied filters, flattened for display as removable chips.

        'name'/'value' identify the form input to clear when the chip is
        dismissed; 'label'/'display' are what the user reads.
        """
        chips: list[dict[str, str]] = []

        for field_name, label in (
            ("source_type", "Source"),
            ("platform", "Platform"),
            ("object_type", "Type"),
        ):
            for value in getattr(self, field_name) or []:
                chips.append(
                    {
                        "name": field_name,
                        "value": value,
                        "label": label,
                        "display": value,
                    }
                )

        for attribute in self.attributes or []:
            attribute_key, separator, attribute_value = attribute.partition(":")
            if not separator:
                continue
            chips.append(
                {
                    "name": "attributes",
                    "value": attribute,
                    "label": attribute_key,
                    "display": attribute_value,
                }
            )

        if self.active_filter is not None:
            chips.append(
                {
                    "name": "active",
                    "value": self.active,
                    "label": "Status",
                    "display": "Active" if self.active_filter else "Inactive",
                }
            )

        return chips

    @property
    def filter_query_string(self) -> str:
        """
        The filter portion of the query as a URL query string. Deliberately
        excludes pagination so a download returns every matching row, not just
        the page on screen.
        """
        params: list[tuple[str, str]] = []
        for field_name in FILTER_FIELD_NAMES:
            value = getattr(self, field_name, None)
            if value is None or value == "":
                continue
            # 'any' is the absence of a status filter, so leave it out of the URL
            if field_name == "active" and value == ACTIVE_ANY:
                continue
            if isinstance(value, list):
                params.extend((field_name, str(item)) for item in value)
            else:
                params.append((field_name, str(value)))

        # sort order is part of what is on screen, so carry it into the download
        if self.sort_key:
            params.append(("sort_key", self.sort_key))
            params.append(("sort_ascending", str(self.sort_ascending).lower()))

        return urlencode(params)

    @property
    def view_query_string(self) -> str:
        """
        The full state of the view, pagination included. Pushed into the address
        bar so a filtered view can be bookmarked, shared and refreshed - unlike
        `filter_query_string`, which drops pagination because a download is
        always the whole result set.
        """
        params: list[tuple[str, str]] = [("page_number", str(self.page_number))]
        if self.page_size != DEFAULT_PAGE_SIZE:
            params.append(("page_size", str(self.page_size)))

        query_string: str = self.filter_query_string
        return "&".join(part for part in [query_string, urlencode(params)] if part)

    def download_url(self, base_url: str) -> str:
        query_string: str = self.filter_query_string
        return f"{base_url}?{query_string}" if query_string else base_url

    def view_url(self, base_url: str) -> str:
        query_string: str = self.view_query_string
        return f"{base_url}?{query_string}" if query_string else base_url

    search_term: str = Field(default="")  # TODO validate to avoid sql injection
    sort_key: str = Field(default=None)
    sort_ascending: bool = Field(default=True)
    page_number: int = Field(default=0)
    page_size: int = Field(default=DEFAULT_PAGE_SIZE)
    record_count: int = Field(default=0)
    scope: str = Field(default=None)
    attributes: list[str] = Field(default=None)
    source_type: list[str] = Field(default=None)
    platform: list[str] = Field(default=None)
    object_type: list[str] = Field(default=None)
    active: str = Field(default=ACTIVE_ANY)
