from jsonpath_ng.ext import parse
from app_logger import Logger, get_logger
from .scim_config import ScimConfig

logger: Logger = get_logger("scim2.service")
scim_config = ScimConfig.load()


class ScimServiceBase:
    SOURCE_TYPE = "scim"

    @staticmethod
    def _get_jsonpath_attribute(
        content: dict, expression: str
    ) -> str | dict | list[str] | None:
        try:
            jsonpath_expression = parse(expression)
            matches = jsonpath_expression.find(content)

            if len(matches) == 1:
                return matches[0].value
            if len(matches) > 1:
                return [m.value for m in matches]
        except Exception as e:
            logger.error(f"Error parsing jsonpath expression: {e}")
        return None
