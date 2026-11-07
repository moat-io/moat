from flask import g
from flask_smorest import Blueprint
from marshmallow import Schema, fields, validate
from repositories import PolicyEvaluatorResultRepository

bp = Blueprint(
    "acl_api",
    __name__,
    url_prefix="/api/acl/v1/grants",
    description="Evaluated ACL API (paginated)",
)


class GrantSchema(Schema):
    principal_fq_name = fields.String(required=True)
    resource_fq_name = fields.String(required=True)
    platform = fields.String(required=True)
    action = fields.String(required=True)
    active = fields.Boolean(required=True)
    record_updated_date = fields.DateTime(required=True)


class PaginatedGrantSchema(Schema):
    page = fields.Int(required=True)
    page_size = fields.Int(required=True)
    total = fields.Int(required=True)
    results = fields.List(fields.Nested(GrantSchema), required=True)


class GrantQuerySchema(Schema):
    page = fields.Int(load_default=1, validate=validate.Range(min=1))
    page_size = fields.Int(load_default=20, validate=validate.Range(min=1, max=100))
    search = fields.String(load_default="")
    sort_key = fields.String(load_default=None)
    sort_ascending = fields.Boolean(load_default=True)
    # a withdrawn grant is kept rather than deleted, so it can be asked for
    active = fields.Boolean(load_default=True)


@bp.route("", methods=["GET"])
@bp.arguments(GrantQuerySchema, location="query")
@bp.response(200, PaginatedGrantSchema)
def list_grants(query_args):
    """Paginated list of the ACL the policy evaluator produced."""
    page = query_args["page"]
    page_size = query_args["page_size"]

    with g.database.Session.begin() as session:
        count, grants = PolicyEvaluatorResultRepository.get_grants_paginated(
            session=session,
            page_number=page - 1,
            page_size=page_size,
            search_term=query_args["search"],
            sort_col_name=query_args["sort_key"],
            sort_ascending=query_args["sort_ascending"],
            active=query_args["active"],
        )

        # read inside the session, as the instances are expired on commit
        results = [
            {
                "principal_fq_name": grant.principal_fq_name,
                "resource_fq_name": grant.resource_fq_name,
                "platform": grant.platform,
                "action": grant.action,
                "active": grant.active,
                "record_updated_date": grant.record_updated_date,
            }
            for grant in grants
        ]

    return {
        "page": page,
        "page_size": page_size,
        "total": count,
        "results": results,
    }
