from pydantic import BaseModel


class BreadcrumbsVm(BaseModel):
    items: list[str]
