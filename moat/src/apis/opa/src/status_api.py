from app_logger import Logger, get_logger
from flask import Blueprint
from apis.models import ApiConfig
from apis.common import authenticate

bp = Blueprint("opa_status", __name__, url_prefix="/api/v1/opa/status")
logger: Logger = get_logger("opa.status_api")
api_config: ApiConfig = ApiConfig.load_by_api_name(api_name="opa")


@bp.route("", methods=["POST"])
@authenticate(api_config=api_config, scope=api_config.oauth2_write_scope)
def post() -> str:
    # logger.info(f"Received status update from OPA: {request.json}")
    return "ok"
