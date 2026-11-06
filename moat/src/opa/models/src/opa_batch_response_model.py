from pydantic import BaseModel, Field


class OpaBatchResponseModel(BaseModel):
    # The indices of the resources in the request's filterResources array that
    # the policy allowed. See opa/trino/batch_mode.rego for the contract.
    result: list[int] = Field(default_factory=list)
