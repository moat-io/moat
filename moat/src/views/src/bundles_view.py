from flask import (
    Blueprint,
    Response,
    g,
    make_response,
    render_template,
    send_file,
    abort,
)
from flask_pydantic import validate

from views.controllers import BundlesController
from views.models import BreadcrumbsVm, TableQueryVm

bp = Blueprint("bundles", __name__, url_prefix="/bundles")

DEFAULT_SORT_KEY: str = "record_updated_date"
SCOPE_PLATFORM: str = "platform"
SCOPE_ALL: str = "all"


@bp.route("/", methods=["GET"])
def index():
    query_state: TableQueryVm = TableQueryVm(sort_key=DEFAULT_SORT_KEY, scope=SCOPE_ALL)
    breadcrumbs: BreadcrumbsVm = BreadcrumbsVm(items=["Bundles"])
    return render_template(
        "partials/bundles/bundles-search.html",
        query_state=query_state,
        breadcrumbs=breadcrumbs,
    )


@bp.route("/table", methods=["GET"])
@validate()
def bundles_table(query: TableQueryVm):
    query.scope = (
        query.scope if query.scope in {SCOPE_PLATFORM, SCOPE_ALL} else SCOPE_ALL
    )

    with g.database.Session.begin() as session:
        if query.scope == SCOPE_PLATFORM:
            bundle_count, bundles = BundlesController.get_latest_by_platform_paginated(
                session=session,
                page_number=query.page_number,
                page_size=query.page_size,
                search_term=query.search_term,
            )
        else:
            bundle_count, bundles = BundlesController.get_all_bundles_paginated(
                session=session,
                sort_col_name=query.sort_key,
                page_number=query.page_number,
                page_size=query.page_size,
                search_term=query.search_term,
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
    response.headers.set("HX-Trigger-After-Swap", "initialiseFlowbite")
    return response


@bp.route("/<int:bundle_id>/download", methods=["GET"])
def download(bundle_id: int):
    with g.database.Session.begin() as session:
        bundle = BundlesController.get_bundle_by_id(
            session=session, bundle_id=bundle_id
        )

        if bundle and bundle.bundle_path:
            return send_file(
                bundle.bundle_path,
                as_attachment=True,
                download_name=f"{bundle.e_tag}.tar.gz",
            )

        abort(404, "Bundle not found")
