from flask import Blueprint, g, render_template, make_response, Response
from flask_pydantic import validate
from views.controllers import OpaBundlesController
from views.models import TableQueryVm

bp = Blueprint("opa_bundles", __name__, url_prefix="/opa_bundles")


@bp.route("/", methods=["GET"])
def index():
    query_state: TableQueryVm = TableQueryVm(sort_key="platform")

    return render_template(
        "partials/opa_bundles/opa-bundles-search.html", query_state=query_state
    )


@bp.route("/table", methods=["GET"])
@validate()
def opa_bundles_table(query: TableQueryVm):
    with g.database.Session.begin() as session:
        bundle_count, bundles = (
            OpaBundlesController.get_all_opa_bundles_with_search_and_pagination(
                session=session,
                sort_col_name=query.sort_key,
                page_number=query.page_number,
                page_size=query.page_size,
                search_term=query.search_term,
            )
        )
        query.record_count = bundle_count
        response: Response = make_response(
            render_template(
                template_name_or_list="partials/opa_bundles/opa-bundles-table.html",
                bundles=bundles,
                bundle_count=bundle_count,
                query_state=query,
            )
        )

    response.headers.set("HX-Trigger-After-Swap", "initialiseFlowbite")
    return response
