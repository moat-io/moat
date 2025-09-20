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

    # just logs an event. not retained at this stage
    context_keys: list[str] = [
        "labels",
        "decision_id",
        "bundles",
        "path",
        "input",
        "result",
        "timestamp",
        "req_id",
    ]

    for decision_log in decision_logs:
        logger.info(f"decision_log: {decision_log}")
        context: dict = {key: decision_log.get(key) for key in context_keys}
        g.event_logger.log_event(asset="opa", action="decision_log", context=context)
    return "ok"
