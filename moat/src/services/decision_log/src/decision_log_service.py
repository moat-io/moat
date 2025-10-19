from app_logger import Logger, get_logger
from events import EventLogger

logger: Logger = get_logger("services.decision_log")


class DecisionLogService:
    @staticmethod
    def process_decision_logs(decision_logs: list[dict]) -> list[dict]:
        return [
            DecisionLogService.parse_decision_log(decision_log=decision_log)
            for decision_log in decision_logs
        ]

    @staticmethod
    def parse_decision_log(decision_log: dict) -> dict:
        _input: dict = decision_log.get("input", {})
        action: dict = _input.get("action", {})
        operation: str = action.get("operation", "")
        context: dict = _input.get("context", {})
        resource: dict = action.get("resource", {})
        labels: dict = decision_log.get("labels", {})

        logger.info(f"Received decision log: {operation}")
        logger.debug(f"Decision log: {decision_log}")

        context: dict = {
            "decision_id": decision_log.get("decision_id"),
            "path": decision_log.get("path", ""),
            "operation": operation,
            "username": context.get("identity", {}).get("user", ""),
            "timestamp": decision_log.get("timestamp"),
            "labels": labels,
            "metrics": decision_log.get("metrics"),
        }

        try:
            # TODO just use the suffix allow or batch
            if not decision_log.get("path").endswith("allow"):
                context["resource_count"] = action.get("resourceCount", None)
                return context

            if operation in (
                "GetColumnMask",
                "SelectFromColumns",
                "FilterColumns",
                "CreateSchema",
                "DropSchema",
                "CreateTable",
                "CreateView",
                "DropTable",
                "DropView",
                "ShowCreateTable",
                "InsertIntoTable",
                "UpdateTableColumns",
                "DeleteFromTable",
            ):
                data_object: dict = (
                    resource.get("column", {})
                    | resource.get("table", {})
                    | resource.get("schema", {})
                )
                context["database"] = data_object.get("catalogName", "")
                context["schema"] = data_object.get("schemaName", "")
                context["table"] = data_object.get("tableName", "")
                context["columns"] = data_object.get("columnName", None) or ", ".join(
                    data_object.get("columns", [])
                )

                context["result"] = decision_log.get("result", None)

            elif operation in ["ExecuteQuery"]:
                context["result"] = decision_log.get("result", None)

            elif operation in ["AccessCatalog"]:
                context["database"] = resource.get("catalog", {}).get("name", "")
                context["result"] = decision_log.get("result", None)

            elif operation in ["FilterCatalogs"]:
                context["database"] = ",".join(
                    [
                        c.get("catalog").get("name")
                        for c in action.get("filterResources", [])
                    ]
                )
                context["result"] = ",".join(
                    str(s) for s in decision_log.get("result", [])
                )

            elif operation in ["FilterSchemas"]:
                context["schema"] = ",".join(
                    [
                        f"{c.get('schema').get('catalogName')}.{c.get('schema').get('schemaName')}"
                        for c in action.get("filterResources", [])
                    ]
                )
                context["result"] = ",".join(
                    str(s) for s in decision_log.get("result", [])
                )

            elif operation == "ImpersonateUser":
                context["value"] = resource.get("user", {}).get("user")
                context["result"] = decision_log.get("result", None)

            elif operation == "SetSystemSessionProperty":
                context["value"] = resource.get("systemSessionProperty", {}).get("name")
                context["result"] = decision_log.get("result", None)

            return context

        except Exception as e:
            logger.error(
                f"Failed to parse OPA decision log: {decision_log} with error: {e}"
            )
        return context
