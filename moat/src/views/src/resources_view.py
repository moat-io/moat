from flask import Blueprint, Response, g, make_response, render_template
from flask import session as flask_session
from flask_pydantic import validate
from views.controllers import ResourcesController
from views.models import BreadcrumbsVm, TableQueryVm

bp = Blueprint("resources", __name__, url_prefix="/resources")

TABLES_SORT_KEY: str = "fq_name"
SCHEMAS_SORT_KEY: str = "schemas.schema_name"
SCOPE_TABLES: str = "tables"
SCOPE_SCHEMAS: str = "schemas"


@bp.route("/tables", methods=["GET"])
def index_tables():
    query_state: TableQueryVm = TableQueryVm(
        sort_key=TABLES_SORT_KEY, scope=SCOPE_TABLES
    )
    breadcrumbs: BreadcrumbsVm = BreadcrumbsVm(items=["Resources", "All Resources"])
    return render_template(
        "partials/resources/resources-search.html",
        query_state=query_state,
        breadcrumbs=breadcrumbs,
    )


@bp.route("/table", methods=["GET"])
@validate()
def table(query: TableQueryVm):
    logged_in_user: str = flask_session.get("userinfo", {}).get(
        "preferred_username", ""
    )

    query_function = (
        ResourcesController.get_schemas_paginated_with_access
        if query.scope == SCOPE_SCHEMAS
        else ResourcesController.get_tables_paginated_with_access
    )

    with g.database.Session.begin() as session:
        table_count, tables = query_function(
            session=session,
            logged_in_user=logged_in_user,
            sort_col_name=query.sort_key,
            page_number=query.page_number,
            page_size=query.page_size,
            search_term=query.search_term,
            attributes=query.attribute_dtos,
        )

        query.record_count = table_count
        response: Response = make_response(
            render_template(
                template_name_or_list="partials/resources/resources-table.html",
                tables=tables,
                table_count=table_count,
                query_state=query,
                compact=False,
            )
        )
    response.headers.set("HX-Trigger-After-Swap", "initialiseFlowbite")
    return response
