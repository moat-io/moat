from app_logger import Logger, get_logger
from flask import Blueprint, jsonify, make_response, request, Response, g
import uuid
from apis.common import authenticate
from apis.models import ApiConfig

from repositories import PrincipalRepository
from models import PrincipalDbo
from api_services.scim2 import ScimUsersService

logger: Logger = get_logger("scim2.users_api")
bp = Blueprint("scim2_users", __name__, url_prefix="/api/scim/v2/Users")
api_config: ApiConfig = ApiConfig.load_by_api_name(api_name="scim")


def create_id() -> str:
    return str(uuid.uuid4())


@bp.route("", methods=["GET"])
@authenticate(api_config=api_config, scope=api_config.oauth2_read_scope)
def get_users():
    # TODO validate these
    start_index: int = int(request.args.get("startIndex", 1))
    count: int = int(request.args.get("count", 10000))

    with g.database.Session.begin() as session:
        user_count, users = ScimUsersService.get_users(
            session=session,
            offset=start_index - 1,
            count=count,
        )

        users = {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],
            "totalResults": user_count,
            "startIndex": start_index,
            "itemsPerPage": count,
            "Resources": [u.scim_payload for u in users],
        }

        response: Response = make_response(
            jsonify(users),
        )
    response.headers["Content-Type"] = "application/scim+json"
    return response


@bp.route("/<user_id>", methods=["GET"])
@authenticate(api_config=api_config, scope=api_config.oauth2_read_scope)
def get_user(user_id):
    with g.database.Session.begin() as session:
        principal = ScimUsersService.get_user_by_id(session=session, source_uid=user_id)
        if not principal:
            response: Response = make_response(
                jsonify(
                    {
                        "schemas": ["urn:ietf:params:scim:api:messages:2.0:Error"],
                        "detail": f"User with ID {user_id} not found",
                        "status": 404,
                    }
                ),
            )
            response.status_code = 404

        else:
            response: Response = make_response(
                jsonify(principal.scim_payload),
            )
    response.headers["Content-Type"] = "application/scim+json"
    return response


@bp.route("", methods=["POST"])
@authenticate(api_config=api_config, scope=api_config.oauth2_write_scope)
def create_user():
    scim_payload = request.json
    source_uid = scim_payload.get(
        "id", create_id()
    )  # respect the source ID if present or create a new one
    scim_payload = scim_payload | {"id": source_uid}

    with g.database.Session.begin() as session:
        if ScimUsersService.user_exists(session=session, source_uid=source_uid):
            response: Response = make_response(
                jsonify(
                    {
                        "schemas": ["urn:ietf:params:scim:api:messages:2.0:Error"],
                        "detail": f"User with ID {source_uid} already exists",
                        "status": 409,
                    }
                ),
            )
            response.status_code = 409

        else:
            scim_payload: dict = ScimUsersService().create_user(
                session=session, scim_payload=scim_payload
            )
            session.commit()
            response: Response = make_response(jsonify(scim_payload), 201)
    response.headers["Content-Type"] = "application/scim+json"
    return response


@bp.route("/<user_id>", methods=["PUT"])
@authenticate(api_config=api_config, scope=api_config.oauth2_write_scope)
def update_user(user_id):
    source_uid = user_id
    scim_payload = request.json | {"id": source_uid}

    with g.database.Session.begin() as session:
        principal: PrincipalDbo = PrincipalRepository.get_by_source_uid(
            session=session, source_uid=source_uid
        )
        if not principal:
            response: Response = make_response(
                jsonify(
                    {
                        "schemas": ["urn:ietf:params:scim:api:messages:2.0:Error"],
                        "detail": f"User with ID {user_id} not found",
                        "status": 404,
                    }
                ),
            )
            response.status_code = 404

        else:
            ScimUsersService.update_user(
                session=session,
                scim_payload=scim_payload,
                principal=principal,
            )
            response: Response = make_response(
                jsonify(principal.scim_payload),
            )
            session.commit()
    response.headers["Content-Type"] = "application/scim+json"
    return response


@bp.route("/<user_id>", methods=["DELETE"])
@authenticate(api_config=api_config, scope=api_config.oauth2_write_scope)
def delete_user(user_id):
    source_uid = user_id
    with g.database.Session.begin() as session:
        principal: PrincipalDbo = PrincipalRepository.get_by_source_uid(
            session=session, source_uid=source_uid
        )
        if not principal:
            response: Response = make_response(
                jsonify(
                    {
                        "schemas": ["urn:ietf:params:scim:api:messages:2.0:Error"],
                        "detail": f"User with ID {user_id} not found",
                        "status": 404,
                    }
                ),
            )
            response.status_code = 404
        else:
            session.delete(principal)
            session.commit()
            response: Response = make_response()
            response.status_code = 204

    response.headers["Content-Type"] = "application/scim+json"
    return response
