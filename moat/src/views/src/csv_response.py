import csv
from io import StringIO
from typing import Iterable

from flask import Response


def make_csv_response(
    file_name: str, header: list[str], rows: Iterable[list[str]]
) -> Response:
    buffer: StringIO = StringIO()
    writer = csv.writer(buffer)
    writer.writerow(header)
    writer.writerows(rows)

    return Response(
        buffer.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={file_name}"},
    )


def format_attributes(attributes) -> str:
    return "; ".join(
        f"{a.attribute_key}={a.attribute_value}"
        for a in sorted(attributes, key=lambda a: a.attribute_key)
    )
