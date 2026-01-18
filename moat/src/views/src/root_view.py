from flask import Blueprint, abort, g, render_template
from flask import session as flask_session
from views.controllers import PrincipalsController

bp = Blueprint("root", __name__, url_prefix="")


@bp.route("/")
def main():
    return render_template(
        "views/main.html",
    )
