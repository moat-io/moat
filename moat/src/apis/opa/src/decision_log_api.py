import gzip
import json

from app_logger import Logger, get_logger
from apis.common import authenticate
from apis.models import ApiConfig
from services.decision_log import DecisionLogService
from flask import Blueprint, g, request


logger: Logger = get_logger("opa.decision_log_api")
bp = Blueprint("opa_decision", __name__, url_prefix="/api/v1/opa/decision")
api_config: ApiConfig = ApiConfig.load_by_api_name(api_name="opa")


@bp.route("", methods=["POST"])
@authenticate(api_config=api_config, scope=api_config.oauth2_write_scope)
def create():
    body = gzip.decompress(request.data)
    decision_logs: list[dict] = json.loads(body.decode("utf-8"))

    conformed_events: list[dict] = DecisionLogService.process_decision_logs(
        decision_logs=decision_logs
    )

    for conformed_event in conformed_events:
        logger.debug(f"Conformed decision_log: {conformed_event}")
        g.event_logger.log_event(
            asset="opa", action="decision_log", context=conformed_event
        )
    return "ok"
