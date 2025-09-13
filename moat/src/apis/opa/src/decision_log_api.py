import gzip
import json

from app_logger import Logger, get_logger
from apis.common import authenticate
from apis.models import ApiConfig
from flask import Blueprint, g, request
from repositories import DecisionLogRepository

logger: Logger = get_logger("opa.decision_log_api")
bp = Blueprint("opa_decision", __name__, url_prefix="/api/v1/opa/decision")
api_config: ApiConfig = ApiConfig.load_by_api_name(api_name="opa")


@bp.route("", methods=["POST"])
@authenticate(api_config=api_config, scope=api_config.oauth2_write_scope)
def create():
    body = gzip.decompress(request.data)
    logger.info(f"request: {body}")
    decision_logs: list[dict] = json.loads(body.decode("utf-8"))
    with g.database.Session.begin() as session:
        DecisionLogRepository.create_bulk(session=session, decision_logs=decision_logs)
        session.commit()

    return "ok"
