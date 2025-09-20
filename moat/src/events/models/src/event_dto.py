from pydantic import BaseModel, Field
from typing import Dict, Any


class EventDto(BaseModel):
    asset: str = Field()
    action: str = Field()
    log: str = Field(default="")
    context: Dict[str, Any] = Field(default_factory=dict)
