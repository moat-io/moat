import json
from app_logger import Logger, get_logger
from flask import (
    Blueprint,
    Response,
    abort,
    g,
    make_response,
    render_template,
)
from flask import session as flask_session
from flask_pydantic import validate
from views.controllers import PoliciesController
from views.models import (
    AttributeListVm,
    PolicyDslVm,
    TableQueryVm,
)

bp = Blueprint("policies", __name__, url_prefix="/policies")

logger: Logger = get_logger("views.policy")

DEFAULT_SORT_KEY: str = "name"


@bp.route("/", methods=["GET"])
def index():
    query_state: TableQueryVm = TableQueryVm(sort_key=DEFAULT_SORT_KEY)

    response: Response = make_response(
        render_template(
            "partials/policies/policies-search.html",
            query_state=query_state,
        )
    )
    return response


@bp.route("/table", methods=["GET"])
@validate()
def policies_table(query: TableQueryVm):
    policies = PoliciesController.get_all_policies()

    query.record_count = len(policies)
    response: Response = make_response(
        render_template(
            template_name_or_list="partials/policies/policies-table.html",
            policies=policies,
            policy_count=len(policies),
            query_state=query,
        )
    )
    response.headers.set("HX-Trigger-After-Swap", "initialiseFlowbite")
    return response


@bp.route("/<policy_id>/detail-modal", methods=["GET"])
@validate()
def get_policy_modal(policy_id: str):
    policy = PoliciesController.get_by_id(policy_id=policy_id)

    if not policy:
        abort(404, "Policy not found")

    response: Response = make_response(
        render_template(
            template_name_or_list="partials/policies/policy-detail-modal.html",
            policy=policy,
        )
    )
    response.headers.set(
        "HX-Trigger-After-Settle",
        json.dumps({"load_codemirror": {"readonly": str(policy.policy_dsl)}}),
    )

    return response
