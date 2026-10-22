"""
Rendering a view either as a bare htmx fragment or as a full page.

Every list view is normally swapped into `#main` by htmx, so the route returns
just the fragment. But now that filter state lives in the URL, those same URLs
have to survive being bookmarked, shared or refreshed - and a browser asking for
one directly gets no `HX-Request` header. In that case the fragment is wrapped in
the application shell, which then loads the view back through htmx with the
original query string intact.
"""

import os
from datetime import datetime

from flask import g, render_template, request

VERSION_FILE_PATH = "version.txt"

SHELL_TEMPLATE = "views/main.html"


def is_htmx_request() -> bool:
    return request.headers.get("HX-Request") == "true"


def shell_context() -> dict:
    """Context the application shell needs, independent of which view is inside it."""
    version: str = (
        open(VERSION_FILE_PATH).read().strip()
        if os.path.isfile(VERSION_FILE_PATH)
        else "dev"
    )

    return {
        "year": datetime.now().year,
        "opa_version": os.getenv("OPA_VERSION", "dev"),
        "version": version,
        "environment": g.environment,
    }


def render_view(template_name: str, **context) -> str:
    """
    Render a list view. htmx gets the fragment; a browser gets the whole page with
    the fragment's own URL queued up as the initial view.
    """
    if is_htmx_request():
        return render_template(template_name, **context)

    return render_template(
        SHELL_TEMPLATE,
        initial_view_url=request.full_path.rstrip("?"),
        **shell_context(),
    )
