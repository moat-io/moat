from datetime import datetime

from apis.common import authenticate
from apis.models import ApiConfig
from flask import g
from flask_smorest import Blueprint, abort
from marshmallow import Schema, fields, validate
from repositories import PolicyEvaluatorResultRepository

api_config: ApiConfig = ApiConfig.load_by_api_name(api_name="acls")

acls_api_bp = Blueprint(
    "acls_api",
    __name__,
    url_prefix="/api/acls/v1",
    description="Paginated access control list API",
)


class AclResultSchema(Schema):
    platform = fields.String(required=True)
    principal = fields.String(attribute="principal_fq_name", required=True)
    resource = fields.String(attribute="resource_fq_name", required=True)
    access = fields.Boolean(attribute="active", required=True)
    granted_at = fields.DateTime(
        attribute="record_created_date", format="iso", required=True
    )


class PaginatedAclSchema(Schema):
    page = fields.Int(required=True)
    page_size = fields.Int(required=True)
    total = fields.Int(required=True)
    results = fields.List(fields.Nested(AclResultSchema), required=True)


class AclQuerySchema(Schema):
    page = fields.Int(load_default=1, validate=validate.Range(min=1))
    page_size = fields.Int(
        load_default=api_config.default_page_size,
        metadata={"minimum": 1, "maximum": api_config.max_page_size},
    )
    sort_by = fields.String(
        load_default="granted_at",
        validate=validate.OneOf(
            ["platform", "principal", "resource", "access", "granted_at"]
        ),
    )
    sort_order = fields.String(
        load_default="desc", validate=validate.OneOf(["asc", "desc"])
    )
    platform = fields.String(load_default="")
    resource_prefix = fields.String(load_default="")
    granted_after = fields.DateTime(format="iso", load_default=None, allow_none=True)


SORT_COLUMNS: dict[str, str] = {
    "platform": "platform",
    "principal": "principal_fq_name",
    "resource": "resource_fq_name",
    "access": "active",
    "granted_at": "record_created_date",
}


@acls_api_bp.route("", methods=["GET"])
@acls_api_bp.doc(tags=["acls"])
@acls_api_bp.arguments(AclQuerySchema, location="query")
@acls_api_bp.response(200, PaginatedAclSchema)
@authenticate(api_config=api_config, scope=api_config.oauth2_read_scope)
def list_acls(query_args: dict):
    """Return a paginated, filterable list of recorded access decisions."""
    page: int = query_args["page"]
    page_size: int = query_args["page_size"]
    if page_size < 1 or page_size > api_config.max_page_size:
        abort(
            400,
            message=f"page_size must be between 1 and {api_config.max_page_size}",
        )

    sort_by: str = query_args["sort_by"]
    granted_after: datetime | None = query_args["granted_after"]
    with g.database.Session.begin() as session:
        total, records = (
            PolicyEvaluatorResultRepository.get_all_with_search_and_pagination(
                session=session,
                sort_col_name=SORT_COLUMNS[sort_by],
                sort_ascending=query_args["sort_order"] == "asc",
                page_number=page - 1,
                page_size=page_size,
                platform=query_args["platform"] or None,
                resource_prefix=query_args["resource_prefix"] or None,
                newer_than=granted_after,
            )
        )
        results = [
            {
                "platform": record.platform,
                "principal_fq_name": record.principal_fq_name,
                "resource_fq_name": record.resource_fq_name,
                "active": record.active,
                "record_created_date": record.record_created_date,
            }
            for record in records
        ]

        return {
            "page": page,
            "page_size": page_size,
            "total": total,
            "results": results,
        }
