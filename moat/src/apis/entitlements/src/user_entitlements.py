from flask_smorest import Blueprint, abort
from flask import g
from repositories import PrincipalRepository
from marshmallow import Schema, fields

bp = Blueprint(
    "user_entitlements",
    __name__,
    url_prefix="/api/entitlements/v1/users",
    description="User entitlement API (paginated)",
)


class ResourceSchema(Schema):
    attribute_value = fields.String(required=True)
    attribute_key = fields.String(required=True)


class EntitlementSchema(Schema):
    username = fields.String(required=True)
    fq_name = fields.String(required=True)
    resources = fields.List(fields.Nested(ResourceSchema), required=True)


class PaginatedEntitlementSchema(Schema):
    page = fields.Int(required=True)
    page_size = fields.Int(required=True)
    total = fields.Int(required=True)
    results = fields.List(fields.Nested(EntitlementSchema), required=True)


class UserEntitlementQuerySchema(Schema):
    page = fields.Int(load_default=1, metadata={"minimum": 1})
    page_size = fields.Int(load_default=20, metadata={"minimum": 1, "maximum": 100})
    search = fields.String(load_default="")
    source = fields.String(load_default="")
    format = fields.String(load_default="json", metadata={"enum": ["json", "excel"]})


def _serialize_entitlement(entitlement):
    """Return a stable response shape for both string and attribute-object entitlements."""
    return {
        "attribute_value": getattr(entitlement, "attribute_value", str(entitlement)),
        "attribute_key": getattr(entitlement, "attribute_key", None),
    }


@bp.route("", methods=["GET"])
@bp.doc(tags=["mcp"])
@bp.arguments(UserEntitlementQuerySchema, location="query")
@bp.response(200, PaginatedEntitlementSchema)
def list_user_entitlements(query_args):
    """Paginated list of user entitlements."""
    page = query_args["page"]
    page_size = query_args["page_size"]
    response_format = query_args["format"].strip().lower()
    search = query_args["search"]
    source = query_args["source"].strip()
    if page < 1 or page_size < 1 or page_size > 100:
        abort(400, message="Invalid page or page_size")
    if response_format not in {"json", "excel"}:
        abort(400, message="Invalid format. Supported formats are json and excel")

    with g.database.Session.begin() as session:
        # Use search if provided
        count, principals = PrincipalRepository.get_all_with_search_and_pagination(
            session=session,
            sort_col_name="user_name",
            page_number=page - 1,
            page_size=page_size,
            search_term=search,
            source_type=source or None,
        )
        results = [
            {
                "username": p.user_name,
                "fq_name": p.fq_name,
                "resources": [
                    _serialize_entitlement(entitlement)
                    for entitlement in (list(getattr(p, "attributes", None) or []))
                ],
            }
            for p in principals
        ]

        return {
            "page": page,
            "page_size": page_size,
            "total": count,
            "results": results,
        }
