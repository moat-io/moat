from flask import Blueprint, Response, g, make_response, render_template
from flask_pydantic import validate

from views.controllers import BundlesController
from views.models import BreadcrumbsVm, TableQueryVm

bp = Blueprint("bundles", __name__, url_prefix="/bundles")

DEFAULT_SORT_KEY: str = "record_updated_date"
SCOPE_PLATFORM: str = "platform"
SCOPE_ALL: str = "all"


@bp.route("/", methods=["GET"])
def index():
    query_state: TableQueryVm = TableQueryVm(
        sort_key=DEFAULT_SORT_KEY, scope=SCOPE_PLATFORM
    )
    breadcrumbs: BreadcrumbsVm = BreadcrumbsVm(items=["Bundles"])
    return render_template(
        "partials/bundles/bundles-search.html",
        query_state=query_state,
        breadcrumbs=breadcrumbs,
    )


@bp.route("/table", methods=["GET"])
@validate()
def bundles_table(query: TableQueryVm):
    scope = query.scope if query.scope in {SCOPE_PLATFORM, SCOPE_ALL} else SCOPE_PLATFORM
    query.scope = scope

    with g.database.Session.begin() as session:
        bundle_count, bundles = BundlesController.get_bundles_paginated(
            session=session,
            sort_col_name=query.sort_key,
            page_number=query.page_number,
            page_size=query.page_size,
            search_term=query.search_term,
            scope=scope,
        )

    query.record_count = bundle_count
    response: Response = make_response(
        render_template(
            template_name_or_list="partials/bundles/bundles-table.html",
            bundles=bundles,
            bundle_count=bundle_count,
            query_state=query,
        )
    )
    return response
