from models.src import (
    PrincipalDbo,
    PrincipalGroupDbo,
    PrincipalAttributeDbo,
    PrincipalHistoryDbo,
    PrincipalAttributeHistoryDbo,
)
from database import Database


def test_principal_history_table(database_empty: Database) -> None:
    with database_empty.Session.begin() as session:
        # Create a principal
        principal = PrincipalDbo()
        principal.fq_name = "test.principal"
        principal.first_name = "Test"
        principal.last_name = "User"
        principal.user_name = "testuser"
        principal.email = "test@example.com"
        principal.source_type = "test_source"
        principal.source_uid = "test_uid"
        principal.entitlements = ["test_entitlement"]
        session.add(principal)

        # Get the ID for later use
        session.flush()
        principal_id = principal.principal_id
        session.commit()

    # Verify the history tables have the insert records
    with database_empty.Session.begin() as session:
        principals_history = (
            session.query(PrincipalHistoryDbo)
            .filter(PrincipalHistoryDbo.principal_id == principal_id)
            .all()
        )

        assert len(principals_history) == 1
        assert principals_history[0].history_change_operation == "I"  # Insert operation
        assert principals_history[0].fq_name == "test.principal"
        assert principals_history[0].first_name == "Test"
        assert principals_history[0].last_name == "User"
        assert principals_history[0].user_name == "testuser"
        assert principals_history[0].email == "test@example.com"

    # Update the principal
    with database_empty.Session.begin() as session:
        # Update the principal
        principal = (
            session.query(PrincipalDbo).filter_by(principal_id=principal_id).one()
        )
        principal.first_name = "Updated"
        principal.last_name = "Name"
        principal.email = "updated@example.com"
        session.commit()

    # Verify the history tables have the update records
    with database_empty.Session.begin() as session:
        # Check principals_history
        principals_history = (
            session.query(PrincipalHistoryDbo)
            .filter(PrincipalHistoryDbo.principal_id == principal_id)
            .order_by(PrincipalHistoryDbo.history_record_created_date)
            .all()
        )

        assert len(principals_history) == 2
        assert principals_history[1].history_change_operation == "U"  # Update operation
        assert principals_history[1].fq_name == "test.principal"
        assert principals_history[1].first_name == "Updated"
        assert principals_history[1].last_name == "Name"
        assert principals_history[1].email == "updated@example.com"

    # Delete the principal
    with database_empty.Session.begin() as session:
        # Delete the principal
        principal = (
            session.query(PrincipalDbo).filter_by(principal_id=principal_id).one()
        )
        session.delete(principal)
        session.commit()

    # Verify the history tables have the delete records
    with database_empty.Session.begin() as session:
        # Check principals_history
        principals_history = (
            session.query(PrincipalHistoryDbo)
            .filter(PrincipalHistoryDbo.principal_id == principal_id)
            .order_by(PrincipalHistoryDbo.history_record_created_date)
            .all()
        )

        assert len(principals_history) == 3
        assert principals_history[2].history_change_operation == "D"  # Delete operation
        assert principals_history[2].fq_name == "test.principal"
