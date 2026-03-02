from app_logger import Logger, get_logger
from flask import Blueprint, Response, g, jsonify, make_response
from apis.models import ApiConfig
from apis.common import authenticate

logger: Logger = get_logger("api.healthcheck")

bp = Blueprint("healthcheck_api_v1", __name__, url_prefix="/api/v1/healthcheck")

api_config: ApiConfig = ApiConfig.load_by_api_name(api_name="healthcheck")


@bp.route("", methods=["GET"])
@authenticate(api_config=api_config, scope=api_config.oauth2_read_scope)
def index():
    response: Response = make_response(jsonify({"status": "ok"}))
    return response
