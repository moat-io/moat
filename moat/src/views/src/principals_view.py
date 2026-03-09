from flask import Blueprint, g, render_template, abort, make_response, Response
from flask_pydantic import validate
from views.controllers import PrincipalsController
from views.models import TableQueryVm

bp = Blueprint("principals", __name__, url_prefix="/principals")


@bp.route("/", methods=["GET"])
def index():
    query_state: TableQueryVm = TableQueryVm(sort_key="user_name")

    return render_template(
        "partials/principals/principals-search.html", query_state=query_state
    )


@bp.route("/table", methods=["GET"])
@validate()
def principals_table(query: TableQueryVm):
    with g.database.Session.begin() as session:
        principal_count, principals = (
            PrincipalsController.get_all_principals_with_search_pagination_and_attr_filter(
                session=session,
                sort_col_name=query.sort_key,  # TODO is this SQL injection?
                page_number=query.page_number,
                page_size=query.page_size,
                search_term=query.search_term,
                attributes=query.attribute_dtos,
            )
        )
        query.record_count = principal_count
        response: Response = make_response(
            render_template(
                template_name_or_list="partials/principals/principals-table.html",
                principals=principals,
                principal_count=principal_count,
                query_state=query,
            )
        )

    response.headers.set("HX-Trigger-After-Swap", "initialiseFlowbite")
    return response


@bp.route("/<principal_id>/history-modal", methods=["GET"])
@validate()
def get_policy_modal(principal_id: int):
    with g.database.Session.begin() as session:
        history = PrincipalsController.get_principal_attribute_history(
            session=session, principal_id=principal_id
        )

        response: Response = make_response(
            render_template(
                template_name_or_list="partials/principals/principal-history-modal.html",
                history=history,
            )
        )

    response.headers.set("HX-Trigger-After-Swap", "initialiseFlowbite")
    return response
