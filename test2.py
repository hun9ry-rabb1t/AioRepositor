import os
import asyncio
from aiorepositor import AioRepositor


str_schema = """
CREATE TABLE IF NOT EXISTS customers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    phone TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    stock INTEGER CHECK(stock >= 0),
    tags TEXT,  -- JSON format string for tags
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER,
    total_amount DECIMAL(10, 2) NOT NULL,
    status TEXT DEFAULT 'pending',
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(customer_id) REFERENCES customers(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS order_items (
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER CHECK(quantity > 0),
    price_per_item DECIMAL(10, 2) NOT NULL,
    PRIMARY KEY(order_id, product_id),
    FOREIGN KEY(order_id) REFERENCES orders(id) ON DELETE CASCADE,
    FOREIGN KEY(product_id) REFERENCES products(id)
);

CREATE TABLE IF NOT EXISTS product_reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER,
    customer_id INTEGER,
    rating INTEGER CHECK(rating >= 1 AND rating <= 5),
    review TEXT,
    review_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE,
    FOREIGN KEY(customer_id) REFERENCES customers(id) ON DELETE CASCADE
);
"""



dict_schema = {
    "customers": {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "name": "TEXT NOT NULL",
        "email": "TEXT UNIQUE NOT NULL",
        "phone": "TEXT",
        "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
    },
    "products": {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "name": "TEXT NOT NULL",
        "price": "DECIMAL(10, 2) NOT NULL",
        "stock": "INTEGER CHECK(stock >= 0)",
        "tags": "TEXT",  # JSON format string for tags
        "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
    },
    "orders": {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "customer_id": "INTEGER",
        "total_amount": "DECIMAL(10, 2) NOT NULL",
        "status": "TEXT DEFAULT 'pending'",
        "order_date": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        "FOREIGN KEY(customer_id)": "REFERENCES customers(id) ON DELETE CASCADE"
    },
    "order_items": {
        "order_id": "INTEGER",
        "product_id": "INTEGER",
        "quantity": "INTEGER CHECK(quantity > 0)",
        "price_per_item": "DECIMAL(10, 2) NOT NULL",
        "PRIMARY KEY": "(order_id, product_id)",
        "FOREIGN KEY(order_id)": "REFERENCES orders(id) ON DELETE CASCADE",
        "FOREIGN KEY(product_id)": "REFERENCES products(id)"
    },
    "product_reviews": {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "product_id": "INTEGER",
        "customer_id": "INTEGER",
        "rating": "INTEGER CHECK(rating >= 1 AND rating <= 5)",
        "review": "TEXT",
        "review_date": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        "FOREIGN KEY(product_id)": "REFERENCES products(id) ON DELETE CASCADE",
        "FOREIGN KEY(customer_id)": "REFERENCES customers(id) ON DELETE CASCADE"
    }
}


# DB dir and indexes constants
DB_FOLDER = 'test_db_folder'
DB_NAME = 'test_db.sqlite'
DB_PATH = os.path.join(DB_FOLDER, DB_NAME)


async def set_db(_schema):
    """Create and initialize the database."""
    return await AioRepositor(_schema, folder_name=DB_FOLDER, db_name=DB_NAME)


async def test_flow(_schema):
    """Test complex database operations, including cascade operations and complex queries."""
    repositories = await set_db(_schema)
    customer_repo = repositories["customers"]
    product_repo = repositories["products"]
    order_repo = repositories["orders"]
    order_item_repo = repositories["order_items"]
    review_repo = repositories["product_reviews"]

    #batch insert for customers
    customers_data = [
        customer_repo.RepoData(name="Alice Smith", email="alice@example.com", phone="555-1234"),
        customer_repo.RepoData(name="Bob Johnson", email="bob@example.com", phone="555-5678")
    ]
    await customer_repo.save_many(customers_data)
    print(f"Inserted customers with IDs: {[customer.id for customer in customers_data]}")

    #batch insert for products with JSON tags
    products_data = [
        product_repo.RepoData(name="Smartphone", price=799.99, stock=50, tags='["electronics", "mobile"]'),
        product_repo.RepoData(name="Laptop", price=1200.00, stock=30, tags='["electronics", "computers"]')
    ]
    await product_repo.save_many(products_data)
    print(f"Inserted products with IDs: {[product.id for product in products_data]}")

    #adding an order for the first customer
    order_data = order_repo.RepoData(customer_id=customers_data[0].id, total_amount=799.99, status="pending")
    await order_repo.save_single(order_data)
    print(f"Inserted order with ID: {order_data.id}")

    #add an item to the order
    order_item_data = order_item_repo.RepoData(order_id=order_data.id, product_id=products_data[0].id, quantity=1, price_per_item=799.99)
    await order_item_repo.save_single(order_item_data)
    print(f"Inserted order item for order {order_item_data.order_id} and product {order_item_data.product_id}")

    #add a product review by the first customer
    review_data = review_repo.RepoData(product_id=products_data[0].id, customer_id=customers_data[0].id, rating=5, review="Great product!")
    await review_repo.save_single(review_data)
    print(f"Inserted review with ID: {review_data.id}")

    #loading customer data
    loaded_customers = await customer_repo.load_many()
    print(f"Loaded customers: {loaded_customers}")

    #loading a single order by the first customer
    loaded_order = await order_repo.load_single(id=order_data.id)
    print(f"Loaded order: {loaded_order}")

    #custom query to get orders by status
    custom_query = "SELECT id, total_amount, status FROM orders WHERE status = :status"
    custom_results = await order_repo.custom_query(custom_query, {"status": "pending"})
    print(f"Custom Query - Pending Orders: {custom_results}")

    loaded_customer = await customer_repo.load_single(id=1)
    print(f"Customer before deletion: {loaded_customer}")

    #delete a customer and check cascading effects
    await customer_repo.delete(id=customers_data[0].id)
    print(f"Deleted customer with ID: {customers_data[0].id}")

    #verify that the related orders deleted
    deleted_orders = await order_repo.load_many(customer_id=customers_data[0].id)
    print(f"Orders after customer deletion: {deleted_orders}")

    #verify that the related order items deleted 
    deleted_order_items = await order_item_repo.load_many(order_id=order_data.id)
    print(f"Order items after customer deletion: {deleted_order_items}")

    #verify reviews for the deleted customer are still intact
    remaining_reviews = await review_repo.load_many(product_id=products_data[0].id)
    print(f"Remaining reviews for product after customer deletion: {remaining_reviews}")




if __name__ == "__main__":
    async def main():
    
        print("Running operation tests on dict schema...")
        await test_flow(dict_schema)
        
        print("Cleaning up the database...")
        AioRepositor._instance.clean_up(full=True)

        print("Running operation tests on str schema...")
        await test_flow(str_schema)
        
        print("Cleaning up the database...")
        AioRepositor._instance.clean_up(full=True)

    asyncio.run(main())
