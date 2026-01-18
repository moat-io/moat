from pydantic import BaseModel, Field


class AttributeListVm(BaseModel):
    @property
    def attribute_list(self) -> list[str]:
        if self.attributes is None:
            return []
        return (
            self.attributes if isinstance(self.attributes, list) else [self.attributes]
        )

    attributes: list[str] | str = Field(default=None)
