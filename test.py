import os
import asyncio
from aiorepositor import AioRepositor


str_schema = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    is_active BOOLEAN
);

CREATE TABLE IF NOT EXISTS addresses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    address_line TEXT NOT NULL,
    city TEXT NOT NULL,
    postal_code TEXT NOT NULL,
    country TEXT NOT NULL,
    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS stores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    manager_id INTEGER,
    address_id INTEGER,
    phone TEXT,
    email TEXT,
    FOREIGN KEY(manager_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY(address_id) REFERENCES addresses(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    store_id INTEGER,
    name TEXT NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,
    available BOOLEAN DEFAULT TRUE,
    FOREIGN KEY(store_id) REFERENCES stores(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER,
    store_id INTEGER,
    address_id INTEGER,
    total_price DECIMAL(10, 2) NOT NULL,
    status TEXT CHECK(status IN ('pending', 'accepted', 'preparing', 'out for delivery', 'delivered', 'cancelled')),
    FOREIGN KEY(customer_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY(store_id) REFERENCES stores(id) ON DELETE CASCADE,
    FOREIGN KEY(address_id) REFERENCES addresses(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS order_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    price_per_item DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY(order_id) REFERENCES orders(id) ON DELETE CASCADE,
    FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS couriers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    vehicle_type TEXT CHECK(vehicle_type IN ('bike', 'car', 'scooter')),
    active BOOLEAN DEFAULT TRUE,
    assigned_orders INTEGER DEFAULT 0,
    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS order_deliveries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER,
    courier_id INTEGER,
    delivery_status TEXT CHECK(delivery_status IN ('assigned', 'picked up', 'in transit', 'delivered', 'failed')),
    assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    delivered_at TIMESTAMP,
    FOREIGN KEY(order_id) REFERENCES orders(id) ON DELETE CASCADE,
    FOREIGN KEY(courier_id) REFERENCES couriers(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS payments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER,
    amount DECIMAL(10, 2) NOT NULL,
    payment_method TEXT CHECK(payment_method IN ('credit card', 'debit card', 'cash', 'paypal', 'google pay', 'apple pay')),
    payment_status TEXT CHECK(payment_status IN ('pending', 'completed', 'failed')),
    payment_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(order_id) REFERENCES orders(id) ON DELETE CASCADE
);

"""


dict_schema = {
    "users": {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "name": "TEXT NOT NULL",
        "email": "TEXT UNIQUE",
        "is_active": "BOOLEAN"
    },
    "addresses": {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "user_id": "INTEGER",
        "address_line": "TEXT NOT NULL",
        "city": "TEXT NOT NULL",
        "postal_code": "TEXT NOT NULL",
        "country": "TEXT NOT NULL",
        "FOREIGN KEY(user_id)": "REFERENCES users(id) ON DELETE CASCADE"
    },
    "stores": {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "name": "TEXT NOT NULL",
        "manager_id": "INTEGER",
        "address_id": "INTEGER",
        "phone": "TEXT",
        "email": "TEXT",
        "FOREIGN KEY(manager_id)": "REFERENCES users(id) ON DELETE CASCADE",
        "FOREIGN KEY(address_id)": "REFERENCES addresses(id) ON DELETE CASCADE"
    },
    "products": {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "store_id": "INTEGER",
        "name": "TEXT NOT NULL",
        "description": "TEXT",
        "price": "DECIMAL(10, 2) NOT NULL",
        "available": "BOOLEAN DEFAULT TRUE",
        "FOREIGN KEY(store_id)": "REFERENCES stores(id) ON DELETE CASCADE"
    },
    "orders": {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "customer_id": "INTEGER",
        "store_id": "INTEGER",
        "address_id": "INTEGER",
        "total_price": "DECIMAL(10, 2) NOT NULL",
        "status": "TEXT CHECK(status IN ('pending', 'accepted', 'preparing', 'out for delivery', 'delivered', 'cancelled'))",
        "FOREIGN KEY(customer_id)": "REFERENCES users(id) ON DELETE CASCADE",
        "FOREIGN KEY(store_id)": "REFERENCES stores(id) ON DELETE CASCADE",
        "FOREIGN KEY(address_id)": "REFERENCES addresses(id) ON DELETE CASCADE"
    },
    "order_items": {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "order_id": "INTEGER",
        "product_id": "INTEGER",
        "quantity": "INTEGER NOT NULL CHECK (quantity > 0)",
        "price_per_item": "DECIMAL(10, 2) NOT NULL",
        "FOREIGN KEY(order_id)": "REFERENCES orders(id) ON DELETE CASCADE",
        "FOREIGN KEY(product_id)": "REFERENCES products(id) ON DELETE CASCADE"
    },
    "couriers": {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "user_id": "INTEGER",
        "vehicle_type": "TEXT CHECK(vehicle_type IN ('bike', 'car', 'scooter'))",
        "active": "BOOLEAN DEFAULT TRUE",
        "assigned_orders": "INTEGER DEFAULT 0",
        "FOREIGN KEY(user_id)": "REFERENCES users(id) ON DELETE CASCADE"
    },
    "order_deliveries": {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "order_id": "INTEGER",
        "courier_id": "INTEGER",
        "delivery_status": "TEXT CHECK(delivery_status IN ('assigned', 'picked up', 'in transit', 'delivered', 'failed'))",
        "assigned_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        "delivered_at": "TIMESTAMP",
        "FOREIGN KEY(order_id)": "REFERENCES orders(id) ON DELETE CASCADE",
        "FOREIGN KEY(courier_id)": "REFERENCES couriers(id) ON DELETE CASCADE"
    },
    "payments": {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "order_id": "INTEGER",
        "amount": "DECIMAL(10, 2) NOT NULL",
        "payment_method": "TEXT CHECK(payment_method IN ('credit card', 'debit card', 'cash', 'paypal', 'google pay', 'apple pay'))",
        "payment_status": "TEXT CHECK(payment_status IN ('pending', 'completed', 'failed'))",
        "payment_date": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        "FOREIGN KEY(order_id)": "REFERENCES orders(id) ON DELETE CASCADE"
    }
}

# DB dir and indexes constants
DB_FOLDER = 'test_db_folder'
DB_NAME = 'test_db.sqlite'
DB_PATH = os.path.join(DB_FOLDER, DB_NAME)
INDEXES = ['user_id', 'product_id', 'email']


async def set_db(_scheme):
    return await AioRepositor(_scheme, indexes=INDEXES, folder_name=DB_FOLDER, db_name=DB_NAME)


async def test_flow(_scheme):
    """Test database operations, including cascade operations and batching."""
    repositories = await set_db(_scheme)
    user_repo = repositories["users"]
    address_repo = repositories["addresses"]
    store_repo = repositories["stores"]
    product_repo = repositories["products"]
    order_repo = repositories["orders"]
    order_item_repo = repositories["order_items"]
    courier_repo = repositories["couriers"]
    order_delivery_repo = repositories["order_deliveries"]
    payment_repo = repositories["payments"]

    #batch insert for users table
    users_data = [
        user_repo.RepoData(name="John Doe", email="john.doe@example.com", is_active=True),
        user_repo.RepoData(name="Jane Doe", email="jane.doe@example.com", is_active=True)
    ]
    await user_repo.save_many(users_data)
    print(f"Inserted users with IDs: {[user.id for user in users_data]}")

    #batch insert for addresses table
    addresses_data = [
        address_repo.RepoData(user_id=users_data[0].id, address_line="123 Main St", city="Cityville", postal_code="12345", country="Countryland"),
        address_repo.RepoData(user_id=users_data[1].id, address_line="456 Elm St", city="Townsville", postal_code="54321", country="Countryland")
    ]
    await address_repo.save_many(addresses_data)
    print(f"Inserted addresses with IDs: {[address.id for address in addresses_data]}")

    #verify load_many for users
    loaded_users = await user_repo.load_many()
    print(f"Loaded users: {loaded_users}")

    #adding a store managed by the first user
    store_data = store_repo.RepoData(name="John's Store", manager_id=users_data[0].id, address_id=addresses_data[0].id, phone="123-456-7890", email="store@example.com")
    await store_repo.save_single(store_data)
    print(f"Inserted store with ID: {store_data.id}")

    #add product to the store
    product_data = product_repo.RepoData(store_id=store_data.id, name="Laptop", description="High-end laptop", price=1500.00, available=True)
    await product_repo.save_single(product_data)
    print(f"Inserted product with ID: {product_data.id}")

    #add an order for the first user
    order_data = order_repo.RepoData(customer_id=users_data[0].id, store_id=store_data.id, address_id=addresses_data[0].id, total_price=1500.00, status="pending")
    await order_repo.save_single(order_data)
    print(f"Inserted order with ID: {order_data.id}")

    #add an item to the order
    order_item_data = order_item_repo.RepoData(order_id=order_data.id, product_id=product_data.id, quantity=1, price_per_item=1500.00)
    await order_item_repo.save_single(order_item_data)
    print(f"Inserted order item with ID: {order_item_data.id}")

    #add courier
    courier_data = courier_repo.RepoData(user_id=users_data[1].id, vehicle_type="car", active=True, assigned_orders=0)
    await courier_repo.save_single(courier_data)
    print(f"Inserted courier with ID: {courier_data.id}")

    #assign delivery
    order_delivery_data = order_delivery_repo.RepoData(order_id=order_data.id, courier_id=courier_data.id, delivery_status="assigned", assigned_at=None, delivered_at=None)
    await order_delivery_repo.save_single(order_delivery_data)
    print(f"Inserted order delivery with ID: {order_delivery_data.id}")

    #add payment for the order
    payment_data = payment_repo.RepoData(order_id=order_data.id, amount=1500.00, payment_method="credit card", payment_status="completed", payment_date=None)
    await payment_repo.save_single(payment_data)
    print(f"Inserted payment with ID: {payment_data.id}")

    #custom query to retrieve data from the users table
    custom_query = "SELECT id, name, email FROM users WHERE is_active = :is_active"
    params = {"is_active": 1}
    custom_results = await user_repo.custom_query(custom_query, params)

    #results from the custom query
    print("Custom Query Results:")
    if custom_results:
        for result in custom_results:
            print(f"ID: {result.id}, Name: {result.name}, Email: {result.email}")
    else:
        print("No custom query results found.")

    #another custom query with filtering
    custom_query_2 = "SELECT id, name FROM users WHERE email = :email"
    params_2 = {"email": "jane.doe@example.com"}
    custom_results_2 = await user_repo.custom_query(custom_query_2, params_2)

    #display results from the second custom query
    print("Custom Query Results for specific email:")
    if custom_results_2:
        for result in custom_results_2:
            print(f"ID: {result.id}, Name: {result.name}")
    else:
        print("No results found for the specific email.")

    #cascade delete (delete user and check cascading)
    await user_repo.delete(id=users_data[0].id)
    print(f"Deleted user with ID: {users_data[0].id}")

    #verify that the related orders, addresses, etc., were also deleted
    deleted_orders = await order_repo.load_many(customer_id=users_data[0].id)
    print(f"Orders after user deletion: {deleted_orders}")

    #verify that the related store deleted
    deleted_store = await store_repo.load_single(id=store_data.id)
    print(f"Store after user deletion: {deleted_store}")

    #verify that the related payment deleted
    deleted_payment = await payment_repo.load_single(order_id=order_data.id)
    print(f"Payment after user deletion: {deleted_payment}")




if __name__ == "__main__":
    async def main():
        print("Setting up the database...")
        
        print("Running operation tests on dict schema...")
        await test_flow(dict_schema)
        
        print("Cleaning up the database...")
        AioRepositor._instance.clean_up(full=True)

        print("Running operation tests on str_schema...")
        await test_flow(str_schema)

        print("Cleaning up the database...")
        AioRepositor._instance.clean_up(full=True)

    asyncio.run(main())
