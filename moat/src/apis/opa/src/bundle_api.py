from app_logger import Logger, get_logger
from flask import send_from_directory
from flask_pydantic import validate
from apis.common import authenticate
from apis.models import ApiConfig

from flask import Blueprint, g
from opa import BundleGenerator

logger: Logger = get_logger("opa.bundle_api")
bp = Blueprint("opa_bundle", __name__, url_prefix="/api/v1/opa/bundle")
api_config: ApiConfig = ApiConfig.load_by_api_name(api_name="opa")


@bp.route("/<platform>", methods=["GET"])
@validate()
@authenticate(api_config=api_config, scope=api_config.oauth2_read_scope)
def index(platform: str):
    with g.database.Session.begin() as session:
        with BundleGenerator(
            session=session, platform=platform, bundle_name="trino"
        ) as bundle:
            return send_from_directory(
                directory=bundle.directory,
                path=bundle.filename,
                mimetype="application/octet-stream",
            )
