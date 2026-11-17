from flask import Blueprint, Response, g, make_response, render_template, url_for
from flask_pydantic import validate
from views.controllers import AclController
from views.models import PAGE_SIZE_OPTIONS, BreadcrumbsVm, TableQueryVm

from .shell import render_view

bp = Blueprint("acls", __name__, url_prefix="/acls")

DEFAULT_SORT_KEY: str = "record_created_date"


@bp.route("/", methods=["GET"])
@validate()
def index(query: TableQueryVm):
    query.sort_key = query.sort_key or DEFAULT_SORT_KEY
    return render_view(
        "partials/acls/acls-search.html",
        query_state=query,
        breadcrumbs=BreadcrumbsVm(items=["Admin", "Access Control Lists"]),
        page_size_options=PAGE_SIZE_OPTIONS,
        initial_load=True,
    )


@bp.route("/table", methods=["GET"])
@validate()
def table(query: TableQueryVm):
    with g.database.Session.begin() as session:
        acl_count, acls = AclController.get_acls_paginated(
            session=session,
            sort_col_name=query.sort_key or DEFAULT_SORT_KEY,
            sort_ascending=query.sort_ascending,
            page_number=query.page_number,
            page_size=query.page_size,
            search_term=query.search_term,
        )

        query.record_count = acl_count
        response: Response = make_response(
            render_template(
                "partials/acls/acls-table.html",
                acls=acls,
                acl_count=acl_count,
                query_state=query,
                page_size_options=PAGE_SIZE_OPTIONS,
            )
        )
    response.headers.set("HX-Trigger-After-Swap", "initialiseFlowbite")
    response.headers.set("HX-Push-Url", query.view_url(url_for("acls.index")))
    return response
