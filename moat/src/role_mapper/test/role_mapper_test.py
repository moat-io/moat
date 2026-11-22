from unittest.mock import MagicMock

from repositories import RoleRepository

from ..src.role_mapper import RoleMapper
from ..src.role_mapper_config import RoleMapperConfig


def test_map_roles_reconciles_using_configured_limits(monkeypatch) -> None:
    config = RoleMapperConfig()
    config.max_users_per_role = 23
    config.max_resources_per_role = 47
    monkeypatch.setattr(RoleMapperConfig, "load", lambda self: config)

    session = MagicMock()
    database = MagicMock()
    database.Session.begin.return_value.__enter__.return_value = session
    monkeypatch.setattr(RoleMapper, "_get_database", lambda self: database)

    reconcile = MagicMock(
        return_value=MagicMock(inserted_or_updated=4, deleted=1)
    )
    monkeypatch.setattr(
        RoleRepository,
        "reconcile",
        reconcile,
    )

    RoleMapper().map_roles()

    reconcile.assert_called_once_with(
        session=session,
        max_users_per_role=23,
        max_resources_per_role=47,
    )
