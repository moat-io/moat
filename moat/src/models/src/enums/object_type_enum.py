from enum import Enum


class ObjectTypeEnum(Enum):
    RESOURCE = "resource"
    RESOURCE_ATTRIBUTE = "resource_attribute"
    PRINCIPAL = "principal"
    PRINCIPAL_ATTRIBUTE = "principal_attribute"
    PRINCIPAL_GROUP = "principal_group"
