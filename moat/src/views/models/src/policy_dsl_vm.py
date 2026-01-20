import hashlib
from pydantic import BaseModel, Field


class PolicyDslVm(BaseModel):
    @property
    def id(self) -> str:
        return hashlib.sha1(self.file_path.encode()).hexdigest()

    @property
    def policy_name(self) -> str:
        filename = self.file_path.split("/")[-1]
        name_part = filename.replace(".rego", "")
        return name_part.replace("_", " ").title()

    file_path: str = Field()
    policy_dsl: str = Field()
