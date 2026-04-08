from models import OpaBundleDbo
from repositories import RepositoryBase


class BundleRepository(RepositoryBase):

    @staticmethod
    def get_bundle_by_id(session, bundle_id: int) -> OpaBundleDbo | None:
        return session.query(OpaBundleDbo).filter(OpaBundleDbo.opa_bundle_id == bundle_id).first()
