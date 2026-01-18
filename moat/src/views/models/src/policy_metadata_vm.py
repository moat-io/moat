from pydantic import BaseModel, Field


class PolicyMetadataVm(BaseModel):
    policy_id: int | None = Field()
    name: str = Field()
    description: str = Field()
