import os
from datetime import datetime
from flask import Blueprint, render_template, g

bp = Blueprint("root", __name__, url_prefix="")

VERSION_FILE_PATH = "version.txt"


@bp.route("/")
def main():
    year: int = datetime.now().year
    opa_version: str = os.getenv("OPA_VERSION", "dev")
    version: str = (
        open("version.txt").read().strip()
        if os.path.isfile(VERSION_FILE_PATH)
        else "dev"
    )

    return render_template(
        "views/main.html",
        year=year,
        opa_version=opa_version,
        version=version,
        environment=g.environment,
    )
