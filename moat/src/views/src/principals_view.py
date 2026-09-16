from flask import (
    Blueprint,
    Response,
    abort,
    g,
    make_response,
    render_template,
    request,
    url_for,
)
from flask_pydantic import validate
from views.controllers import PrincipalsController
from views.models import PAGE_SIZE_OPTIONS, BreadcrumbsVm, TableQueryVm

from .csv_response import format_attributes, make_csv_response
from .shell import render_view

bp = Blueprint("principals", __name__, url_prefix="/principals")

DEFAULT_SORT_KEY: str = "user_name"

CSV_HEADER: list[str] = [
    "user_name",
    "first_name",
    "last_name",
    "email",
    "source_type",
    "active",
    "attributes",
]


@bp.route("/", methods=["GET"])
@validate()
def index(query: TableQueryVm):
    """
    The filter state is read back off the query string, so a bookmarked or shared
    URL reopens the same view. A browser hitting this directly gets the whole
    page; htmx gets just the fragment.
    """
    query.sort_key = query.sort_key or DEFAULT_SORT_KEY

    with g.database.Session.begin() as session:
        filter_options: dict[str, list[str]] = PrincipalsController.get_filter_options(
            session=session
        )

    return render_view(
        "partials/principals/principals-search.html",
        query_state=query,
        filter_options=filter_options,
        breadcrumbs=BreadcrumbsVm(items=["Subjects", "Principals"]),
        page_size_options=PAGE_SIZE_OPTIONS,
        initial_load=True,
    )


@bp.route("/table", methods=["GET"])
@validate()
def principals_table(query: TableQueryVm):
    with g.database.Session.begin() as session:
        principal_count, principals = (
            PrincipalsController.get_all_principals_with_search_pagination_and_attr_filter(
                session=session,
                sort_col_name=query.sort_key or DEFAULT_SORT_KEY,
                sort_ascending=query.sort_ascending,
                page_number=query.page_number,
                page_size=query.page_size,
                search_term=query.search_term,
                attributes=query.attribute_dtos,
                source_type=query.source_type,
                active=query.active_filter,
            )
        )
        query.record_count = principal_count
        response: Response = make_response(
            render_template(
                template_name_or_list="partials/principals/principals-table.html",
                principals=principals,
                principal_count=principal_count,
                query_state=query,
                page_size_options=PAGE_SIZE_OPTIONS,
            )
        )

    response.headers.set("HX-Trigger-After-Swap", "initialiseFlowbite")
    # keep the address bar in step with the filters, so refresh and back work
    response.headers.set("HX-Push-Url", query.view_url(url_for("principals.index")))
    return response


@bp.route("/attribute-values", methods=["GET"])
def attribute_values():
    """Values for the attribute key currently chosen in the filter panel."""
    attribute_key: str = request.args.get("attribute_key", "").strip()

    with g.database.Session.begin() as session:
        values: list[str] = (
            PrincipalsController.get_attribute_values(
                session=session, attribute_key=attribute_key
            )
            if attribute_key
            else []
        )

    return render_template(
        "partials/common/filter-attribute-values.html",
        attribute_key=attribute_key,
        values=values,
        target_prefix="principals",
    )


@bp.route("/download", methods=["GET"])
@validate()
def download_csv(query: TableQueryVm):
    """
    CSV of every principal matching the filters currently applied in the UI.
    Pagination is deliberately ignored - the download is the whole result set,
    not the page on screen.
    """
    with g.database.Session.begin() as session:
        _, principals = PrincipalsController.get_principals_matching_filter(
            session=session,
            sort_col_name=query.sort_key or DEFAULT_SORT_KEY,
            sort_ascending=query.sort_ascending,
            search_term=query.search_term,
            attributes=query.attribute_dtos,
            source_type=query.source_type,
            active=query.active_filter,
        )

        rows: list[list[str]] = [
            [
                principal.user_name,
                principal.first_name,
                principal.last_name,
                principal.email,
                principal.source_type,
                principal.active,
                format_attributes(principal.attributes),
            ]
            for principal in principals
        ]

    return make_csv_response(
        file_name="principals.csv",
        header=CSV_HEADER,
        rows=rows,
    )


@bp.route("/<int:principal_id>/detail", methods=["GET"])
def principal_detail(principal_id: int):
    """Contents of the row detail drawer: the principal, its attributes and history."""
    with g.database.Session.begin() as session:
        principal = PrincipalsController.get_principal_by_id(
            session=session, principal_id=principal_id
        )
        if principal is None:
            abort(404, "Principal not found")

        history = PrincipalsController.get_principal_attribute_history(
            session=session, principal_id=principal_id
        )

        response: Response = make_response(
            render_template(
                template_name_or_list="partials/principals/principal-detail.html",
                principal=principal,
                history=history,
            )
        )

    response.headers.set("HX-Trigger-After-Swap", "initialiseFlowbite")
    return response
