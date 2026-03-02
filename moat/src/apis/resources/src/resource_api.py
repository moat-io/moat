from app_logger import Logger, get_logger
from flask import Blueprint, g, request, Response, make_response, jsonify
from models import ResourceDbo

bp = Blueprint("resources", __name__, url_prefix="/api/v1/resources")
logger: Logger = get_logger("api.resources")


@bp.route("", methods=["GET"])
def get() -> Response:
    logger.info(f"Request GET from {request.remote_addr}")
    with g.database.Session.begin() as session:
        resources = session.query(ResourceDbo).limit(10).all()

        response: Response = make_response(
            jsonify(
                [
                    {
                        "fq_name": r.fq_name,
                        "platform": r.platform,
                        "object_type": r.object_type,
                    }
                    for r in resources
                ]
            )
        )
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response
