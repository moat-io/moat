from app_logger import Logger, get_logger
from flask import Blueprint, g, request
from apis.models import ApiConfig
from apis.common import authenticate

bp = Blueprint("opa_status", __name__, url_prefix="/api/v1/opa/status")
logger: Logger = get_logger("opa.status_api")
api_config: ApiConfig = ApiConfig.load_by_api_name(api_name="opa")


@bp.route("", methods=["POST"])
@authenticate(api_config=api_config, scope=api_config.oauth2_write_scope)
def post() -> str:
    # just logs an event. not retained at this stage
    context_keys: list[str] = ["labels", "bundles", "decision_logs"]
    context: dict = {key: request.json.get(key) for key in context_keys}

    g.event_logger.log_event(asset="opa", action="status_update", context=context)
    return "ok"
