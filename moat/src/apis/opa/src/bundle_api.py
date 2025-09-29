from urllib import request

from app_logger import Logger, get_logger
from flask import send_from_directory
from flask_pydantic import validate
from apis.common import authenticate
from apis.models import ApiConfig

from flask import Blueprint, g, request, make_response, Response, abort
from services.bundle import BundleService
from models import OpaBundleDbo

logger: Logger = get_logger("opa.bundle_api")
bp = Blueprint("opa_bundle", __name__, url_prefix="/api/v1/opa/bundle")
api_config: ApiConfig = ApiConfig.load_by_api_name(api_name="opa")


@bp.route("/<platform>", methods=["GET"])
@validate()
@authenticate(api_config=api_config, scope=api_config.oauth2_read_scope)
def index(platform: str):
    with g.database.Session.begin() as session:
        # get etag from request headers
        etag: str = request.headers.get("If-None-Match")
        logger.info(f"Request Etag: {etag}")

        # get latest bundle metadata from DB
        opa_bundle_dbo: OpaBundleDbo = BundleService.get_current_bundle_metadata(
            session=session, platform=platform
        )

        # if doesn't exist, return 404 Not Found
        if not opa_bundle_dbo:
            logger.info(f"No bundle found for platform: {platform}")
            abort(404, f"No bundle found for platform: {platform}")

        # check if current bundle matches etag for this platform, and return 304 Not Modified if so
        if opa_bundle_dbo and opa_bundle_dbo.e_tag == etag:
            logger.info(
                f"ETag matches for platform: {platform}, returning 304 Not Modified"
            )
            return "", 304

        # if false return the new bundle
        response: Response = make_response(
            send_from_directory(
                directory=opa_bundle_dbo.bundle_directory,
                path=opa_bundle_dbo.bundle_filename,
                mimetype="application/octet-stream",
            )
        )
        logger.info(
            f"ETag does not match for platform: {platform}, returning new bundle"
        )

        response.headers["Etag"] = opa_bundle_dbo.e_tag
        return response
