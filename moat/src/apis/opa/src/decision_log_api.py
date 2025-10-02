import gzip
import json

from app_logger import Logger, get_logger
from apis.common import authenticate
from apis.models import ApiConfig
from flask import Blueprint, g, request

logger: Logger = get_logger("opa.decision_log_api")
bp = Blueprint("opa_decision", __name__, url_prefix="/api/v1/opa/decision")
api_config: ApiConfig = ApiConfig.load_by_api_name(api_name="opa")


@bp.route("", methods=["POST"])
@authenticate(api_config=api_config, scope=api_config.oauth2_write_scope)
def create():
    body = gzip.decompress(request.data)
    decision_logs: list[dict] = json.loads(body.decode("utf-8"))

    for decision_log in decision_logs:
        logger.info(f"decision_log: {decision_log}")
        context = _parse_single_decision_log(decision_log=decision_log)
        g.event_logger.log_event(asset="opa", action="decision_log", context=context)
    return "ok"


def _parse_decision_log(decision_log: dict) -> dict:
    context: dict = {}
    try:
        if decision_log.get("path") in ["moat/trino/allow", "moat/trino/batch"]:
            return _parse_single_decision_log(decision_log=decision_log)
        else:
            return _parse_batch_decision_log(decision_log=decision_log)
    except Exception as e:
        logger.error(f"Failed to log decision log: {decision_log} with error: {e}")
    return context


def _parse_single_decision_log(decision_log: dict) -> dict:
    _input: dict = decision_log.get("input", {})
    action: dict = _input.get("action", {})
    operation: str = action.get("operation", "")
    context: dict = _input.get("context", {})
    resource: dict = action.get("resource", {})
    labels: dict = decision_log.get("labels", {})

    context: dict = {
        "decision_id": decision_log.get("decision_id"),
        "path": decision_log.get("path", ""),
        "operation": operation,
        "username": context.get("identity", {}).get("user", ""),
        "timestamp": decision_log.get("timestamp"),
        "labels": labels,
    }

    if operation in (
        "GetColumnMask",
        "SelectFromColumns",
        "FilterTables",
        "FilterColumns",
        "CreateSchema",
        "DropSchema",
        "CreateTable",
        "DropTable",
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
            [c.get("catalog").get("name") for c in action.get("filterResources", [])]
        )
        context["result"] = ",".join(str(s) for s in decision_log.get("result", []))

    elif operation in ["FilterSchemas"]:
        context["schema"] = ",".join(
            [
                f"{c.get('schema').get('catalogName')}.{c.get('schema').get('schemaName')}"
                for c in action.get("filterResources", [])
            ]
        )
        context["result"] = ",".join(str(s) for s in decision_log.get("result", []))

    return context


def _parse_batch_decision_log(decision_log: dict) -> dict:
    _input: dict = decision_log.get("input", {})
    action: dict = _input.get("action", {})
    operation: str = action.get("operation", "")
    context: dict = _input.get("context", {})
    resource: dict = action.get("resource", {})
    labels: dict = decision_log.get("labels", {})

    context: dict = {
        "decision_id": decision_log.get("decision_id"),
        "path": decision_log.get("path", ""),
        "operation": operation,
        "username": context.get("identity", {}).get("user", ""),
        "timestamp": decision_log.get("timestamp"),
        "labels": labels,
    }

    return context
