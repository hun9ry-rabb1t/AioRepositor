# AioRepositor: The All-in-One Database Manager 🚀

Welcome to **AioRepositor** – your friendly neighborhood database and repository manager. Whether you're juggling complex schemas or just trying to run a few simple queries, **AioRepositor** has your back. With this bad boy, you'll never have to write repetitive SQL statements again. It's a full-on repository factory that creates dataclass-based repo objects from your schema, and yeah, it plays nice with async too!

## Why AioRepositor? 🤔

- **One Repo to Rule Them All:** AioRepositor creates ready-to-use repositories for each table in your schema. That's less typing, more productivity!
- **Schema-First Approach:** Provide a schema, and AioRepositor will validate and transform it into an entire database and repo structure.
- **Async All the Way:** Built with `aiosqlite`, so it’s async-friendly out of the box.
- **Dynamic Dataclasses:** AioRepositor dynamically generates dataclasses for each table. Type-safe, Pythonic, and totally rad.
- **Cascade Deletes? It’s Got You!** AioRepositor handles foreign key constraints like a champ. If something is deleted, it ensures all related records are cleaned up.

## Installation 🛠️

### Prerequisites

You’ll need Python 3.10+ and `aiosqlite`. To install it:

```bash
pip install aiosqlite
```

After that, clone this repo or add the files to your project.

## How It Works 🧠

### AioRepositor

This is the core of the package. It's a singleton (only one instance allowed at a time, like that "One weird uncle at Thanksgiving"), and it handles everything from schema validation to connection management. It’s where you get your hands on those sweet, sweet repositories.

### Repositories

Each table in your schema gets its own repository. Want to `save_many` records? Done. Want to `delete` records and watch everything cascade away like a champ? No problem.

### Schema Parsing

Got your schema as a string? AioRepositor can convert that string into a dictionary and handle all the necessary table creation for you. It's the "chef" you never knew you needed.

## Example Usage 🚀

Let’s say you have a database schema for a typical e-commerce app with customers, products, orders, and reviews. Here’s how you can get started in no time:

```python
import os
import asyncio
from aiorepositor import AioRepositor

# Example schema (as a string)
schema_str = """
CREATE TABLE IF NOT EXISTS customers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    phone TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# Directory and database details
DB_FOLDER = 'my_db'
DB_NAME = 'ecommerce_db.sqlite'

# A simple setup function to initialize the db
async def setup_database(schema):
    repositories = await AioRepositor(schema, folder_name=DB_FOLDER, db_name=DB_NAME)
    return repositories

# Now lets run a simple test
async def main():
    # Get the repos after setting up the db
    repositories = await setup_database(schema_str)

    # Access the customer repo (auto-generated from your schema)
    customer_repo = repositories["customers"]

    # Create some customer data using our shiny new dataclass
    customer1 = customer_repo.RepoData(name="Alice", email="alice@wonderland.com", phone="123456789")
    customer2 = customer_repo.RepoData(name="Bob", email="bob@builder.com", phone="987654321")

    # Save them to the database
    await customer_repo.save_many([customer1, customer2])

    # Fetch all customers
    customers = await customer_repo.load_many()
    print(f"Loaded customers: {customers}")

    # Clean up
    AioRepositor._instance.clean_up(full=True)

# Run the async main function
asyncio.run(main())
```

## API Overview 🧑‍💻

### `AioRepositor`

The main class that handles your database and repository creation. Here’s a breakdown of the key methods:

- `__new__(schema, folder_name, db_name, indexes)`: Initializes the database and its schema.
- `create_connection()`: Sets up an SQLite database connection.
- `clean_up(full)`: Cleans up the database files and resets the instance. **Pro tip:** Use `full=True` if you want to remove the whole folder, not just the database file.
  
### Repositories (Auto-generated)

For each table in your schema, you'll get a repository with these awesome methods:

- `save_single(data)`: Inserts or updates a single record.
- `save_many(data_list)`: Inserts or updates multiple records.
- `load_single(**kwargs)`: Loads a single record based on provided filters.
- `load_many(**kwargs)`: Loads multiple records based on provided filters.
- `delete(**kwargs)`: Deletes records based on provided filters.
- `custom_query(query, params)`: Executes custom SQL and returns a dataclass for dynamic results.

## Design Patterns and Principles 📚

AioRepositor is built with some of the best design patterns and coding principles:

- **Singleton Pattern**: Only one instance of AioRepositor is created, ensuring consistent database management.
- **Repository Pattern**: Each table gets a repository, providing a clean abstraction for database operations.
- **CRUD Operations**: It supports Create, Read, Update, Delete operations through simple repository methods.
- **SOLID Principles**:
  - **Single Responsibility**: Each class does one thing (AioRepositor manages the database, repositories handle data for their specific tables).
  - **Open/Closed**: The system is designed to be easily extendable without modifying existing code.
  - **Dependency Inversion**: High-level modules depend on abstractions, not on lower-level details.
- **DRY (Don't Repeat Yourself)**: AioRepositor generates repository classes and SQL queries dynamically, reducing redundant code.
  
## What AioRepositor Does NOT Support (Yet!) ❌

- **Custom SQL Types**: AioRepositor currently supports only standard SQL types like `INTEGER`, `TEXT`, `REAL`, `BLOB`, `BOOLEAN`, etc. Support for custom SQL types will be added in future updates!
- **Sharding**: Sharding across multiple database instances isn't supported yet. This might be implemented in the near future.

## Why You'll Love It ❤️

- **Saves Time**: No more manual repo classes! AioRepositor auto-generates repositories and dataclasses.
- **Type-Safe**: Thanks to Python’s `dataclass`, each record is type-safe and structured.
- **Foreign Key Support**: It automatically enforces foreign keys in your SQLite database.
- **Indexing Support**: Provide indexes for frequently used columns for faster access.
- **Fun to Use**: Writing SQL is fun again. Maybe?

**Note:** AioRepositor currently supports only standard SQL types (like INTEGER, TEXT, etc.). Custom types will be supported in future updates!

## Contributions 💡

PRs, feedback, and discussions are always welcome. Just don’t forget to bring pizza 🍕.

Enjoy using **AioRepositor** and may your databases always stay sane and clean!



