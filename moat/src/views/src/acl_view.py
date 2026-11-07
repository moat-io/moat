from flask import Blueprint, Response, g, make_response, render_template, url_for
from flask_pydantic import validate
from views.controllers import AclController
from views.models import PAGE_SIZE_OPTIONS, BreadcrumbsVm, TableQueryVm

from .shell import render_view

bp = Blueprint("acl", __name__, url_prefix="/acl")

DEFAULT_SORT_KEY: str = "principal_fq_name"


@bp.route("/", methods=["GET"])
@validate()
def index(query: TableQueryVm):
    query.sort_key = query.sort_key or DEFAULT_SORT_KEY

    return render_view(
        "partials/acl/acl-search.html",
        query_state=query,
        breadcrumbs=BreadcrumbsVm(items=["Access", "ACL"]),
        page_size_options=PAGE_SIZE_OPTIONS,
        initial_load=True,
    )


@bp.route("/table", methods=["GET"])
@validate()
def table(query: TableQueryVm):
    with g.database.Session.begin() as session:
        grant_count, grants = AclController.get_grants_paginated(
            session=session,
            sort_col_name=query.sort_key or DEFAULT_SORT_KEY,
            sort_ascending=query.sort_ascending,
            page_number=query.page_number,
            page_size=query.page_size,
            search_term=query.search_term,
        )

        query.record_count = grant_count
        response: Response = make_response(
            render_template(
                template_name_or_list="partials/acl/acl-table.html",
                grants=grants,
                query_state=query,
                page_size_options=PAGE_SIZE_OPTIONS,
            )
        )
    response.headers.set("HX-Trigger-After-Swap", "initialiseFlowbite")
    # keep the address bar in step with the search, so refresh and back work
    response.headers.set("HX-Push-Url", query.view_url(url_for("acl.index")))
    return response
