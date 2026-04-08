from flask.testing import FlaskClient

from database import Database
from models import OpaBundleDbo


def test_bundle_download(flask_test_client: FlaskClient, database_empty: Database):
    # bundle not found
    response = flask_test_client.get("/bundles/999/download")
    assert response.status_code == 404

    # create bundle
    with database_empty.Session.begin() as session:
        opa_bundle_dbo: OpaBundleDbo = OpaBundleDbo()
        opa_bundle_dbo.platform = "trino"
        opa_bundle_dbo.bundle_filename = "bundle.tar.gz"
        opa_bundle_dbo.bundle_directory = "views/test"
        opa_bundle_dbo.e_tag = "553ae7da92f5505a92bbb8c9d47be76ab9f65bc2"
        session.add(opa_bundle_dbo)
        session.flush()
        bundle_id = opa_bundle_dbo.opa_bundle_id
        session.commit()

    # bundle found
    response = flask_test_client.get(f"/bundles/{bundle_id}/download")
    assert response.status_code == 200
    assert (
        response.headers.get("Content-Disposition")
        == "attachment; filename=553ae7da92f5505a92bbb8c9d47be76ab9f65bc2.tar.gz"
    )
    assert response.data == b"fake tarball"
