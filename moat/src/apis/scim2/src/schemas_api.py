import json
from app_logger import Logger, get_logger
from flask import Blueprint, jsonify, make_response, request, Response
from services.scim2 import ScimConfig

logger: Logger = get_logger("scim2.schemas_api")
scim_config = ScimConfig.load()
bp = Blueprint("scim2_schemas", __name__, url_prefix="/api/scim/v2/Schemas")


@bp.route("", methods=["GET"])
def get_schemas():
    """
    Get the Schemas.

    This endpoint returns information about the schemas supported by the SCIM service provider.
    """
    with open(scim_config.user_schema_filepath) as f:
        user_schema: dict = json.load(f)

    with open(scim_config.group_schema_filepath) as f:
        group_schema: dict = json.load(f)

    schema_list: list[dict] = [user_schema, group_schema]
    schemas = {
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],
        "totalResults": len(schema_list),
        "Resources": schema_list,
    }

    response: Response = make_response(
        jsonify(schemas),
    )
    response.headers["Content-Type"] = "application/scim+json"
    return response


@bp.route("/urn:ietf:params:scim:schemas:core:2.0:User", methods=["GET"])
def get_user_schema():
    """
    Get the User Schema.

    This endpoint returns information about the User schema.
    """
    with open(scim_config.user_schema_filepath) as f:
        user_schema: dict = json.load(f)

    response: Response = make_response(
        jsonify(user_schema),
    )
    response.headers["Content-Type"] = "application/scim+json"
    return response


@bp.route("/urn:ietf:params:scim:schemas:core:2.0:Group", methods=["GET"])
def get_group_schema():
    """
    Get the Group Schema.

    This endpoint returns information about the Group schema.
    """
    with open(scim_config.group_schema_filepath) as f:
        group_schema: dict = json.load(f)

    response: Response = make_response(
        jsonify(group_schema),
    )
    response.headers["Content-Type"] = "application/scim+json"
    return response
