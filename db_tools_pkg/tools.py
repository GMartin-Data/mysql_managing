# db_tools.py
#
# A collection of robust functions for interacting with a SQLAlchemy-compatible
# database, with a focus on security, error handling, and ease of use.
#
# Finalized on: May 28, 2025

from typing import Dict, Optional, Tuple, Union

import pandas as pd
from sqlalchemy import Engine, text
from sqlalchemy.exc import SQLAlchemyError


# Custom class for type hints
ParamsType = Optional[Union[Tuple, Dict]]


def execute_command(sql_command: str, db_engine: Engine) -> bool:
    """
    Executes a non-query, non parametrizable SQL command directly.

    !!! SECURITY WARNING !!!
    This function is for trusted, administrative commands where parameterization
    is not possible (e.g., CREATE USER, GRANT).
    It is VULNERABLE to SQL    injection if any part of the `sql_command`
    string is built from untrusted external input. Use with extreme care.
    """
    try:
        with db_engine.connect() as connection:
            # For DDL/DCL commands, we often need to wrap them in a transaction
            # and commit them to ensure they take effect immediately.
            with connection.begin() as transaction:
                connection.execute(text(sql_command))
                transaction.commit()
            return True
    except SQLAlchemyError as e:
        print(f"❌ An error occured with the admin command: {e}")
        return False


def run_sql(
    sql: str, engine: Engine, params: ParamsType = None
) -> Union[pd.DataFrame, int, None]:
    """
    Executes a SQL command securely using parameterization.
    This should be your default function for all data operations.

    - If the command is a SELECT query, it returns a Pandas DataFrame.
    - If the command is an INSERT, UPDATE or DELETE, it returns the number of
      affected rows (an integer).
    - If an error occur, it prints the error and returns None.
    """
    params = params or {}

    try:
        with engine.connect() as connection:
            with connection.begin() as transaction:
                try:
                    result = connection.execute(text(sql), params)

                    if result.returns_rows:
                        # It's a SELECT query. Return results as a DataFrame
                        return pd.DataFrame(result.mappings().all())
                    else:
                        # It's an INSERT/UPDATE/DELET.
                        # Commit and return the number of affected rows
                        transaction.commit()
                        return result.rowcount
                except:
                    # If an error occured within the transaction, roll it back
                    transaction.rollback()
                    raise  # Re-raise the exception to be called by outer block
    except SQLAlchemyError as e:
        print(f"❌ A database error occured: {e}")
        return None


def create_read_only_user(
    username: str, password: str, database: str, db_engine: Engine
) -> bool:
    """
    Safely creates a new user with read-only privileges on a specific database.
    This demonstrates the correct use of the execute_command tool.
    """
    print(f"🥚 Attempting to create read-only user: {username}...")

    # These DDL/DCL commands cannot be parameterized, so we use execute_command
    # after building the strings from the trusted inputs
    cmd_create = f"CREATE USER '{username}'@'%' IDENTIFIED BY '{password}';"
    cmd_usage = f"GRANT USAGE ON *.* TO '{username}'@'%';"
    cmd_select = f"GRANT SELECT ON '{database}'.* TO '{username}'@'%';"

    if not execute_command(cmd_create, db_engine):
        print(f"❌ Failed at CREATE USER step for {username}")
        return False

    if not execute_command(cmd_usage, db_engine):
        print(f"❌ Failed at GRANT USAGE step for {username}")
        return False

    if not execute_command(cmd_select, db_engine):
        print(f"❌ Failed at GRANT SELECT step for {username}")
        return False

    print(f"✅ Successfully created user '{username}'.")
    return True
