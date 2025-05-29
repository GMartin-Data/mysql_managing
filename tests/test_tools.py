import pandas as pd
import pytest
from sqlalchemy import create_engine, Engine

from db_tools_pkg import run_sql, execute_command


@pytest.fixture
def sqlite_engine() -> Engine:
    """
    Pytest fixture to create an in-memory SQLite engine.
    A fresh database is created for each test function that uses this fixture.
    """
    # In-memory SQLite database for fast, isolated tests
    engine = create_engine("sqlite:///:memory:")
    return engine


# --- Tests for execute_command ---
def test_execute_command_create_table_success(sqlite_engine: Engine):
    """Tests that execute_command can successfully create a simple table."""
    create_table_sql = (
        "CREATE TABLE IF NOT EXISTS my_test_table (id INTEGER PRIMARY KEY, name TEXT);"
    )
    success = execute_command(create_table_sql, sqlite_engine)
    assert success is True

    # Verify table exists by trying to select
    # NOTE: This uses run_sql, so it's a bit of an integration test
    verify_df = run_sql(
        """
        SELECT name
          FROM sqlite_master
         WHERE type='table' AND name='my_test_table';
        """,
        sqlite_engine,
    )
    assert isinstance(verify_df, pd.DataFrame)
    assert len(verify_df) == 1
    assert verify_df.iloc[0]["name"] == "my_test_table"


def test_execute_command_failure(sqlite_engine: Engine):
    """Tests that execute_command returns False on invalid SQL."""
    invalid_sql = "CREATE TABEL my_test_table (id INTEGER PRIMARY KEY, name TEXT);"  # Intentional typo: TABEL
    success = execute_command(invalid_sql, sqlite_engine)
    assert success is False


# --- Tests for run_sql ---
def test_run_sql_insert_and_select(sqlite_engine: Engine):
    """
    Tests run_sql for:
    - INSERT (returns rowcount)
    - SELECT (returns DataFrame).
    """
    execute_command(
        "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, email TEXT);",
        sqlite_engine,
    )

    # Test INSERT
    insert_query = """
    INSERT INTO users (email)
    VALUES (:email_val);
    """
    rows_affected = run_sql(
        insert_query, sqlite_engine, params={"email_val": "test@example.com"}
    )
    # 1. Check for None
    assert rows_affected is not None, "INSERT operation failed and returned None"
    # 2. Check for integer as output
    assert isinstance(rows_affected, int), (
        f"Expected row count (int), got {type(rows_affected)}"
    )
    # 3. Check for correct value
    assert rows_affected == 1

    rows_affected_2 = run_sql(
        insert_query, sqlite_engine, params={"email_val": "another@example.com"}
    )
    # 1. Check for None
    assert rows_affected_2 is not None, "INSERT operation failed and returned None"
    # 2. Check for integer as output
    assert isinstance(rows_affected_2, int), (
        f"Expected row count (int), got {type(rows_affected_2)}"
    )
    # 3. Check for correct value
    assert rows_affected_2 == 1

    # Test SELECT
    select_query = """
    SELECT id, email
      FROM users
     WHERE email LIKE :pattern
     ORDER BY id;
    """
    df_users = run_sql(select_query, sqlite_engine, params={"pattern": "%@example.com"})

    assert isinstance(df_users, pd.DataFrame)
    assert len(df_users) == 2
    assert df_users.iloc[0]["email"] == "test@example.com"
    assert df_users.iloc[1]["email"] == "another@example.com"


def test_run_sql_select_no_data(sqlite_engine: Engine):
    """
    Tests run_sql SELECT that returns no data, resulting in an empty DataFrame."""
    execute_command(
        "CREATE TABLE IF NOT EXISTS empty_stuff (id INTEGER);", sqlite_engine
    )

    df_empty = run_sql(
        """
        SELECT *
          FROM empty_stuff
         WHERE id = :id_val;
        """,
        sqlite_engine,
        params={"id_val": 1},
    )

    assert isinstance(df_empty, pd.DataFrame)
    assert df_empty.empty is True


def test_run_sql_update_data(sqlite_engine: Engine):
    """Tests run_sql for UPDATE command and checks rowcount."""
    execute_command(
        "CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT, quantity INTEGER);",
        sqlite_engine,
    )
    run_sql(
        """
        INSERT INTO items (id, name, quantity)
        VALUES (:id, :name, :qty);
        """,
        sqlite_engine,
        params={"id": 1, "name": "gadget", "qty": 10},
    )

    update_query = """
    UPDATE items
       SET quantity = :qty
     WHERE name = :name;
    """
    rows_updated = run_sql(
        update_query, sqlite_engine, params={"qty": 15, "name": "gadget"}
    )
    # 1. Check for None
    assert rows_updated is not None, "UPDATE operation failed"
    # 2. Check for integet as output
    assert isinstance(rows_updated, int), (
        f"Expected row count (int), got {type(rows_updated)}"
    )
    # 3. Check for correct value
    assert rows_updated == 1

    # Verify the update
    df_item = run_sql(
        """
        SELECT quantity
          FROM items
         WHERE name = :name;
        """,
        sqlite_engine,
        params={"name": "gadget"},
    )
    assert isinstance(df_item, pd.DataFrame)
    assert df_item.iloc[0]["quantity"] == 15


def test_run_sql_delete_data(sqlite_engine: Engine):
    """Tests run_sql for DELETE command and checks rowcount."""
    execute_command(
        "CREATE TABLE IF NOT EXISTS stuff_to_delete (id INTEGER PRIMARY KEY, label TEXT);",
        sqlite_engine,
    )

    insert_single_sql = """
    INSERT INTO stuff_to_delete (id, label)
    VALUES (:id, :label);
    """
    rows_inserted_1 = run_sql(
        insert_single_sql, sqlite_engine, params={"id": 1, "label": "delete_me"}
    )
    # 1. Check for none
    assert rows_inserted_1 is not None, "INSERT operation failed"
    # 2. Check for integer as output
    assert isinstance(rows_inserted_1, int), (
        f"Expected row count (int), got {type(rows_inserted_1)}"
    )
    # Check for correct value
    assert rows_inserted_1 == 1

    rows_inserted_2 = run_sql(
        insert_single_sql, sqlite_engine, params={"id": 2, "label": "keep_me"}
    )
    # 1. Check for none
    assert rows_inserted_2 is not None, "INSERT operation failed"
    # 2. Check for integer as output
    assert isinstance(rows_inserted_2, int), (
        f"Expected row count (int), got {type(rows_inserted_2)}"
    )
    # Check for correct value
    assert rows_inserted_2 == 1

    delete_query = """
    DELETE FROM stuff_to_delete
     WHERE label = :label_val;
    """
    rows_deleted = run_sql(
        delete_query, sqlite_engine, params={"label_val": "delete_me"}
    )
    # 1. Check for None
    assert rows_deleted is not None, "DELETE operation failed"
    # 2. Check for integer as output
    assert isinstance(rows_deleted, int), (
        f"Expected row count (int), got {type(rows_deleted)}"
    )
    # 3. Check for correct value
    assert rows_deleted == 1

    df_stuff = run_sql(
        """
        SELECT COUNT(*) AS count
          FROM stuff_to_delete;
        """,
        sqlite_engine,
    )
    assert isinstance(df_stuff, pd.DataFrame)
    assert df_stuff.iloc[0]["count"] == 1
