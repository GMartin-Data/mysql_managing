# SQLAlchemy Database Tools (`db_tools_pkg`)

A Python package providing a collection of robust and secure functions for interacting with SQLAlchemy-compatible databases. While primarily designed with Cloud SQL for MySQL in mind, its core functionalities are adaptable for other SQL backends (e.g., SQLite for testing).

This package emphasizes secure parameterization for data queries, clear error handling via Python's `logging` module, and helper functions for common database operations.

## Key Features

- **Secure Query Execution:** Uses parameterized queries for `SELECT`, `INSERT`, `UPDATE`, and `DELETE` operations via the `run_sql` function to prevent SQL injection vulnerabilities.
- **Dual-Purpose Query Function (`run_sql`):**
  - Returns Pandas DataFrames for `SELECT` queries.
  - Returns affected row counts for DML operations (`INSERT`, `UPDATE`, `DELETE`).
- **Administrative Command Execution (`execute_command`):**
  - Allows execution of DDL/DCL commands (e.g., `CREATE USER`, `GRANT`) where parameterization is not typically possible.
  - Includes prominent security warnings regarding its use.
- **User Creation Utility (`create_read_only_user`):**
  - A high-level function to easily create read-only users for a specified database.
- **Integrated Logging:** Uses Python's standard `logging` module for informative output. The library itself adds a `NullHandler` so consuming applications have full control over log configuration.
- **SQLAlchemy Integration:** Designed to work seamlessly with SQLAlchemy `Engine` objects.

## Installation & Dependencies

This package is currently intended for direct use within a project.

**Main Dependencies:**

- SQLAlchemy
- Pandas
- A database-specific driver (e.g., `PyMySQL` for MySQL)

These should be managed by your project's dependency manager (e.g., `uv` via `pyproject.toml`). Example:

```toml
# pyproject.toml
[project]
dependencies = [
    "sqlalchemy>=2.0", # Or your specific version
    "pandas>=2.0",     # Or your specific version
    "PyMySQL"          # For MySQL connectivity
]
```

## Basic Usage

### 1\. Setup: Creating a SQLAlchemy Engine

All functions in this package require a SQLAlchemy `Engine` object.

```python
from sqlalchemy import create_engine

# Example for MySQL:
DB_USER = "your_user"
DB_PASSWORD = "your_password"
DB_HOST = "your_db_host_or_ip"
DB_PORT = "3306" # Or your MySQL port
DB_NAME = "your_database_name"

connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)

# Example for in-memory SQLite (often used for testing):
# engine = create_engine("sqlite:///:memory:")
```

### 2\. Importing Functions

Assuming `db_tools_pkg` is in your Python path (e.g., you are in the project root and it's structured as `db_tools_pkg/tools.py` with an `__init__.py`):

```python
from db_tools_pkg import run_sql, execute_command, create_read_only_user
```

### 3\. Fetching Data (SELECT Queries)

Use `run_sql` for `SELECT` statements. It returns a Pandas DataFrame.

```python
sql_select = "SELECT id, name FROM users WHERE status = :status_val LIMIT :limit_val;"
params_dict = {"status_val": "active", "limit_val": 10}
users_df = run_sql(sql_select, engine, params=params_dict)

if users_df is not None:
    print("Active Users:")
    print(users_df.to_string())
```

### 4\. Modifying Data (INSERT, UPDATE, DELETE)

Use `run_sql` for these commands. It returns the number of affected rows.

```python
sql_insert = "INSERT INTO products (name, price) VALUES (:name, :price);"
params_insert = {"name": "New Gadget", "price": 49.99}
rows_inserted = run_sql(sql_insert, engine, params=params_insert)

if rows_inserted is not None:
    print(f"Successfully inserted {rows_inserted} product(s).")

sql_update = "UPDATE products SET price = :new_price WHERE name = :product_name;"
params_update = {"new_price": 45.99, "product_name": "New Gadget"}
rows_updated = run_sql(sql_update, engine, params=params_update)

if rows_updated is not None:
    print(f"Successfully updated {rows_updated} product(s).")
```

### 5\. Executing Administrative Commands (`execute_command`)

Use this for DDL/DCL commands where parameters are not typically used for the core structure. **Use with extreme caution if any part of the command string is dynamic.**

```python
# Example: Granting an additional privilege (use cautiously)
# Assumes 'some_user' and 'some_table' are trusted, controlled inputs.
# A real scenario might involve more complex validation.
some_user = "report_user"
some_table = "sales_summary"
admin_sql = f"GRANT UPDATE ON `{some_table}` TO '{some_user}'@'%';"
# Note: Building SQL with f-strings is dangerous if inputs are not trusted.
# This function is for cases where parameterization is not an option for the command structure.

success = execute_command(admin_sql, engine)
if success:
    print(f"Admin command executed successfully: {admin_sql}")
```

**Security Warning:** The `execute_command` function directly executes the SQL string provided. It is vulnerable to SQL injection if the command string is constructed with untrusted external input. Only use it for static administrative commands or commands built from thoroughly validated and trusted inputs.

### 6\. Creating a Read-Only User (`create_read_only_user`)

```python
# Assumes `new_user_password` is obtained securely (e.g., from env variable, secrets manager)
# and `target_db_name` is a trusted variable.
# new_user_password = "a_very_strong_password"
# target_db_name = "my_application_db"
# new_db_user = "app_readonly_user"

# success = create_read_only_user(
#     username=new_db_user,
#     password=new_user_password,
#     database=target_db_name,
#     db_engine=engine
# )

# if success:
#     print(f"Read-only user '{new_db_user}' created successfully.")
```

_(Commented out the direct execution of `create_read_only_user` as it requires a password and is a significant action, better for users to uncomment and adapt)._

## Logging Configuration

The `db_tools_pkg` uses Python's standard `logging` module. By default (as a library best practice), it adds a `NullHandler` to its logger (`db_tools_pkg.tools`). This means you won't see any log output from this package unless you configure logging in your application.

To see logs from `db_tools_pkg`, configure the Python logging system in your main application script. Here's a basic example using `dictConfig`:

```python
# In your main application script (e.g., app.py)
import logging
import logging.config

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s'
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG', # Or 'INFO' for less verbosity
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
            'stream': 'ext://sys.stdout'
        }
    },
    'loggers': {
        'db_tools_pkg': { # Configure logging for your package
            'handlers': ['console'],
            'level': 'DEBUG',    # Set the desired level for your package
            'propagate': False
        },
        'sqlalchemy.engine': { # Optional: See SQLAlchemy engine logs
            'handlers': ['console'],
            'level': 'INFO', # Use WARNING for less noise
            'propagate': False
        }
    },
    'root': { # Default for all other loggers
        'handlers': ['console'],
        'level': 'WARNING',
    }
}

logging.config.dictConfig(LOGGING_CONFIG)

# Now, when you use functions from db_tools_pkg, their logs will appear
# based on this configuration.
# from db_tools_pkg import run_sql
# ...
```

## Testing

The project includes a test suite in the `tests/` directory using `pytest`. Coverage reporting is also configured.

To run tests (ensure development dependencies like `pytest` and `pytest-cov` are installed):

```bash
python -m pytest --cov=db_tools_pkg
```

## Contributing (Placeholder)

Details on how to contribute to this project will be added here.

## License (Placeholder)

This project will be licensed under the [MIT License](https://www.google.com/search?q=LICENSE_FILE_TO_BE_ADDED).
