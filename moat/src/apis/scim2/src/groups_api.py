from app_logger import Logger, get_logger
from flask import Blueprint, jsonify, make_response, request, Response, g
import uuid
from apis.common import authenticate
from apis.models import ApiConfig
from models import PrincipalGroupDbo
from api_services.scim2 import ScimGroupsService

logger: Logger = get_logger("scim2.groups_api")
bp = Blueprint("scim2_groups", __name__, url_prefix="/api/scim/v2/Groups")
api_config: ApiConfig = ApiConfig.load_by_api_name(api_name="scim")


def create_id() -> str:
    return str(uuid.uuid4())


@bp.route("", methods=["GET"])
@authenticate(api_config=api_config, scope=api_config.oauth2_read_scope)
def get_groups():
    # TODO validate these
    start_index: int = int(request.args.get("startIndex", 1))
    count: int = int(request.args.get("count", 10000))

    with g.database.Session.begin() as session:
        group_count, principal_groups = ScimGroupsService.get_groups(
            session=session,
            offset=start_index - 1,
            count=count,
        )

        groups = {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],
            "totalResults": group_count,
            "startIndex": start_index,
            "itemsPerPage": count,
            "Resources": [pg.scim_payload for pg in principal_groups],
        }

        response: Response = make_response(
            jsonify(groups),
        )
    response.headers["Content-Type"] = "application/scim+json"
    return response


@bp.route("/<group_id>", methods=["GET"])
@authenticate(api_config=api_config, scope=api_config.oauth2_read_scope)
def get_group(group_id):
    with g.database.Session.begin() as session:
        principal_group: PrincipalGroupDbo = ScimGroupsService.get_group_by_id(
            session=session, source_uid=group_id
        )
        if not principal_group:
            response: Response = make_response(
                jsonify(
                    {
                        "schemas": ["urn:ietf:params:scim:api:messages:2.0:Error"],
                        "detail": f"Group with ID {group_id} not found",
                        "status": 404,
                    }
                ),
            )
            response.status_code = 404
        else:
            response: Response = make_response(
                jsonify(principal_group.scim_payload),
            )
    response.headers["Content-Type"] = "application/scim+json"
    return response


@bp.route("", methods=["POST"])
@authenticate(api_config=api_config, scope=api_config.oauth2_write_scope)
def create_group():
    scim_payload = request.json
    source_uid = scim_payload.get(
        "id", create_id()
    )  # respect the source ID if present or create a new one
    scim_payload = scim_payload | {"id": source_uid}

    with g.database.Session.begin() as session:
        if ScimGroupsService.group_exists(session=session, source_uid=source_uid):
            response: Response = make_response(
                jsonify(
                    {
                        "schemas": ["urn:ietf:params:scim:api:messages:2.0:Error"],
                        "detail": f"Group with ID {source_uid} already exists",
                        "status": 409,
                    }
                ),
            )
            response.status_code = 409

        else:
            ScimGroupsService.create_group(
                session=session,
                scim_payload=scim_payload,
            )
            session.commit()
            response: Response = make_response(jsonify(scim_payload), 201)
    response.headers["Content-Type"] = "application/scim+json"
    return response


@bp.route("/<group_id>", methods=["PUT"])
@authenticate(api_config=api_config, scope=api_config.oauth2_write_scope)
def update_group(group_id):
    """
    Update a Group.

    This endpoint updates an existing Group.
    """
    source_uid = group_id
    scim_payload = request.json | {"id": source_uid}

    with g.database.Session.begin() as session:
        principal_group: PrincipalGroupDbo = ScimGroupsService.get_group_by_id(
            session=session, source_uid=group_id
        )
        if not principal_group:
            response: Response = make_response(
                jsonify(
                    {
                        "schemas": ["urn:ietf:params:scim:api:messages:2.0:Error"],
                        "detail": f"Group with ID {group_id} not found",
                        "status": 404,
                    }
                ),
            )
            response.status_code = 404
        else:
            ScimGroupsService.update_group(
                scim_payload=scim_payload,
                principal_group=principal_group,
            )
            response: Response = make_response(
                jsonify(principal_group.scim_payload),
            )
            session.commit()

    response.headers["Content-Type"] = "application/scim+json"
    return response


@bp.route("/<group_id>", methods=["DELETE"])
@authenticate(api_config=api_config, scope=api_config.oauth2_write_scope)
def delete_group(group_id):
    """
    Delete a Group.

    This endpoint deletes a Group.
    """
    with g.database.Session.begin() as session:
        principal_group: PrincipalGroupDbo = ScimGroupsService.get_group_by_id(
            session=session, source_uid=group_id
        )
        if not principal_group:
            response: Response = make_response(
                jsonify(
                    {
                        "schemas": ["urn:ietf:params:scim:api:messages:2.0:Error"],
                        "detail": f"Group with ID {group_id} not found",
                        "status": 404,
                    }
                ),
            )
            response.status_code = 404
        else:
            response: Response = make_response(
                jsonify(principal_group.scim_payload), 204
            )
            session.delete(principal_group)
            session.commit()

    response.headers["Content-Type"] = "application/scim+json"
    return response
