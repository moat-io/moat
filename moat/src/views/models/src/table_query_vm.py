import math

from models import AttributeDto
from pydantic import BaseModel, Field


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
        if self.attributes is None:
            return None

        return [
            AttributeDto(
                attribute_key=attr_str.split(":")[0],
                attribute_value=attr_str.split(":")[1],
            )
            for attr_str in self.attributes
        ]

    search_term: str = Field(default="")  # TODO validate to avoid sql injection
    sort_key: str = Field(default=None)
    page_number: int = Field(default=0)
    page_size: int = Field(default=20)
    record_count: int = Field(default=0)
    scope: str = Field(default=None)
    attributes: list[str] = Field(
        default=None
    )  # TODO regex a colon in this to avoid the split breaking
