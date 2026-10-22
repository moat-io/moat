from textwrap import dedent

from ..src.repository_base import RepositoryBase


class PrincipalDboMock:
    __tablename__ = "principals"


class PrincipalStagingDboMock:
    __tablename__ = "principals_staging"


def test_get_merge_insert_statement():
    assert (
        dedent(
            """
        insert into principals (source_uid, id, first_name, last_name, user_name, email, ingestion_process_id)
        select src.source_uid, src.id, src.first_name, src.last_name, src.user_name, src.email, 1234
        from principals_staging src
            left join principals tgt on tgt.source_uid = src.source_uid and tgt.id = src.id
            where src.source_uid is not null and tgt.source_uid is null and src.id is not null and tgt.id is null
        """
        )
        == RepositoryBase._get_merge_insert_statement(
            source_model=PrincipalStagingDboMock,
            target_model=PrincipalDboMock,
            merge_keys=["source_uid", "id"],
            update_cols=["first_name", "last_name", "user_name", "email"],
            ingestion_process_id=1234,
        )
    )


def test_get_merge_update_statement_mysql():
    assert (
        dedent(
            """
            UPDATE principals tgt
            JOIN principals_staging src ON tgt.source_uid = src.source_uid and tgt.id = src.id
            SET tgt.first_name = src.first_name, tgt.last_name = src.last_name, tgt.user_name = src.user_name, tgt.email = src.email, tgt.active = true, tgt.ingestion_process_id = 1234
            WHERE tgt.first_name <> src.first_name or tgt.last_name <> src.last_name or tgt.user_name <> src.user_name or tgt.email <> src.email or tgt.active is not true
        """
        )
        == RepositoryBase._get_merge_update_statement(
            source_model=PrincipalStagingDboMock,
            target_model=PrincipalDboMock,
            merge_keys=["source_uid", "id"],
            update_cols=["first_name", "last_name", "user_name", "email"],
            ingestion_process_id=1234,
            dialect="mysql",
        )
    )


def test_get_merge_update_statement_postgres():
    assert (
        dedent(
            """
            update principals tgt
            set first_name = src.first_name, last_name = src.last_name, user_name = src.user_name, email = src.email, active = true, ingestion_process_id = 1234
            from principals_staging src
            where tgt.source_uid = src.source_uid and tgt.id = src.id and (tgt.first_name <> src.first_name or tgt.last_name <> src.last_name or tgt.user_name <> src.user_name or tgt.email <> src.email or tgt.active is not true)
            """
        )
        == RepositoryBase._get_merge_update_statement(
            source_model=PrincipalStagingDboMock,
            target_model=PrincipalDboMock,
            merge_keys=["source_uid", "id"],
            update_cols=["first_name", "last_name", "user_name", "email"],
            ingestion_process_id=1234,
        )
    )


def test_get_merge_deactivate_statement_postgres():
    assert (
        dedent(
            """
            delete from principals tgt
            where not exists (select 1 from principals_staging src where (src.source_uid = tgt.source_uid or (src.source_uid is null and tgt.source_uid is null)) and (src.id = tgt.id or (src.id is null and tgt.id is null)))
            """
        )
        == RepositoryBase._get_merge_deactivate_statement(
            source_model=PrincipalStagingDboMock,
            target_model=PrincipalDboMock,
            merge_keys=["source_uid", "id"],
            ingestion_process_id=1,
        )
    )


def test_get_merge_deactivate_statement_mysql():
    assert (
        dedent(
            """
            delete tgt from principals tgt
            where not exists (select 1 from principals_staging src where (src.source_uid = tgt.source_uid or (src.source_uid is null and tgt.source_uid is null)) and (src.id = tgt.id or (src.id is null and tgt.id is null)))
            """
        )
        == RepositoryBase._get_merge_deactivate_statement(
            source_model=PrincipalStagingDboMock,
            target_model=PrincipalDboMock,
            merge_keys=["source_uid", "id"],
            ingestion_process_id=1,
            dialect="mysql",
        )
    )
