from flask import Blueprint, render_template

from .shell import shell_context

bp = Blueprint("root", __name__, url_prefix="")


@bp.route("/")
def main():
    return render_template("views/main.html", **shell_context())
