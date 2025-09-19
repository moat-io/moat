from textwrap import dedent

from ..src.repository_base import RepositoryBase


class PrincipalDboMock:
    __tablename__ = "principals"


class PrincipalStagingDboMock:
    __tablename__ = "principals_staging"


def test_get_merge_statement():
    assert (
        dedent(
            """
    merge into principals as tgt
    using (
        select * from principals_staging
    ) src
    on src.source_uid = tgt.source_uid and src.id = tgt.id
    when matched 
        and src.first_name <> tgt.first_name or src.last_name <> tgt.last_name or src.user_name <> tgt.user_name or src.email <> tgt.email
    then
        update set first_name = src.first_name, last_name = src.last_name, user_name = src.user_name, email = src.email, ingestion_process_id = 1234
    when not matched then
        insert (source_uid, id, first_name, last_name, user_name, email, ingestion_process_id)
        values (src.source_uid, src.id, src.first_name, src.last_name, src.user_name, src.email, 1234)
    """
        )
        == RepositoryBase._get_merge_statement(
            source_model=PrincipalStagingDboMock,
            target_model=PrincipalDboMock,
            merge_keys=["source_uid", "id"],
            update_cols=["first_name", "last_name", "user_name", "email"],
            ingestion_process_id=1234,
        )
    )


def test_get_merge_deactivate_statement():
    assert (
        dedent(
            """
            update principals tgt
            set ingestion_process_id = 1, active = false
            from (
                select source_uid, id from principals
                except
                select source_uid, id from principals_staging
            ) src
            where tgt.source_uid = src.source_uid and tgt.id = src.id
        """
        )
        == RepositoryBase._get_merge_deactivate_statement(
            source_model=PrincipalStagingDboMock,
            target_model=PrincipalDboMock,
            merge_keys=["source_uid", "id"],
            ingestion_process_id=1,
        )
    )
