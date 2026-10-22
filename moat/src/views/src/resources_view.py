from flask import (
    Blueprint,
    Response,
    g,
    make_response,
    render_template,
    request,
)
from flask import session as flask_session
from flask import (
    url_for,
)
from flask_pydantic import validate
from views.controllers import ResourcesController
from views.models import PAGE_SIZE_OPTIONS, BreadcrumbsVm, TableQueryVm

from .csv_response import format_attributes, make_csv_response
from .shell import render_view

bp = Blueprint("resources", __name__, url_prefix="/resources")

TABLES_SORT_KEY: str = "fq_name"
SCHEMAS_SORT_KEY: str = "schemas.schema_name"
SCOPE_TABLES: str = "tables"
SCOPE_SCHEMAS: str = "schemas"

CSV_HEADER: list[str] = ["fq_name", "platform", "object_type", "active", "attributes"]


@bp.route("/tables", methods=["GET"])
@validate()
def index_tables(query: TableQueryVm):
    """
    The filter state is read back off the query string, so a bookmarked or shared
    URL reopens the same view. A browser hitting this directly gets the whole
    page; htmx gets just the fragment.
    """
    query.sort_key = query.sort_key or TABLES_SORT_KEY
    query.scope = query.scope or SCOPE_TABLES

    with g.database.Session.begin() as session:
        filter_options: dict[str, list[str]] = ResourcesController.get_filter_options(
            session=session
        )

    return render_view(
        "partials/resources/resources-search.html",
        query_state=query,
        breadcrumbs=BreadcrumbsVm(items=["Resources", "All Resources"]),
        filter_options=filter_options,
        page_size_options=PAGE_SIZE_OPTIONS,
        initial_load=True,
    )


@bp.route("/attribute-values", methods=["GET"])
def attribute_values():
    """Values for the attribute key currently chosen in the filter panel."""
    attribute_key: str = request.args.get("attribute_key", "").strip()

    with g.database.Session.begin() as session:
        values: list[str] = (
            ResourcesController.get_attribute_values(
                session=session, attribute_key=attribute_key
            )
            if attribute_key
            else []
        )

    return render_template(
        "partials/common/filter-attribute-values.html",
        attribute_key=attribute_key,
        values=values,
        target_prefix="resources",
    )


@bp.route("/download", methods=["GET"])
@validate()
def download_csv(query: TableQueryVm):
    """
    CSV of every resource matching the filters currently applied in the UI.
    Pagination is deliberately ignored - the download is the whole result set,
    not the page on screen.
    """
    with g.database.Session.begin() as session:
        _, resources = ResourcesController.get_resources_matching_filter(
            session=session,
            sort_col_name=query.sort_key or TABLES_SORT_KEY,
            sort_ascending=query.sort_ascending,
            search_term=query.search_term,
            platform=query.platform,
            object_type=query.object_type,
            active=query.active_filter,
            attributes=query.attribute_dtos,
        )

        rows: list[list[str]] = [
            [
                resource.fq_name,
                resource.platform,
                resource.object_type,
                resource.active,
                format_attributes(resource.attributes),
            ]
            for resource in resources
        ]

    return make_csv_response(
        file_name="resources.csv",
        header=CSV_HEADER,
        rows=rows,
    )


@bp.route("/table", methods=["GET"])
@validate()
def table(query: TableQueryVm):
    logged_in_user: str = flask_session.get("userinfo", {}).get(
        "preferred_username", ""
    )

    with g.database.Session.begin() as session:
        table_count, tables = ResourcesController.get_tables_paginated_with_access(
            session=session,
            logged_in_user=logged_in_user,
            sort_col_name=query.sort_key or TABLES_SORT_KEY,
            sort_ascending=query.sort_ascending,
            page_number=query.page_number,
            page_size=query.page_size,
            search_term=query.search_term,
            attributes=query.attribute_dtos,
            platform=query.platform,
            object_type=query.object_type,
            active=query.active_filter,
        )

        query.record_count = table_count
        response: Response = make_response(
            render_template(
                template_name_or_list="partials/resources/resources-table.html",
                tables=tables,
                table_count=table_count,
                query_state=query,
                compact=False,
                page_size_options=PAGE_SIZE_OPTIONS,
            )
        )
    response.headers.set("HX-Trigger-After-Swap", "initialiseFlowbite")
    # keep the address bar in step with the filters, so refresh and back work
    response.headers.set(
        "HX-Push-Url", query.view_url(url_for("resources.index_tables"))
    )
    return response
