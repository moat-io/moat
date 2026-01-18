from pydantic import BaseModel, Field


class PolicyTRVm(BaseModel):
    policy_id: int | None = Field()
    name: str = Field()
    description: str = Field()
    policy_type: str
    author: str
    publisher: str | None
    status: str
    allowed_actions: list[str]
