from pydantic import BaseModel, Field


class PolicyDslVm(BaseModel):
    policy_dsl: str = Field()
