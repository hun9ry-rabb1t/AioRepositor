import os
import asyncio
from aiorepositor import AioRepositor
from schema_parser import SchemaValidator
from repo_factory import RepositoryFactory
import shutil

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
    tags TEXT,
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
        "tags": "TEXT",
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
    """Test operations, including batch deletes, complex queries, and edge cases."""
    repositories = await set_db(_schema)
    customer_repo = repositories["customers"]
    product_repo = repositories["products"]
    order_repo = repositories["orders"]
    order_item_repo = repositories["order_items"]
    review_repo = repositories["product_reviews"]

    #bbatch insert for customers
    customers_data = [
        customer_repo.RepoData(name="Alice Smith", email="alice@example.com", phone="555-1234"),
        customer_repo.RepoData(name="Bob Johnson", email="bob@example.com", phone="555-5678"),
        customer_repo.RepoData(name="Charlie Brown", email="charlie@example.com", phone="555-9101")
    ]
    await customer_repo.save_many(customers_data)
    print(f"Inserted customers with IDs: {[customer.id for customer in customers_data]}")

    #batch insert for products with JSON tags
    products_data = [
        product_repo.RepoData(name="Smartphone", price=799.99, stock=50, tags='["electronics", "mobile"]'),
        product_repo.RepoData(name="Laptop", price=1200.00, stock=30, tags='["electronics", "computers"]'),
        product_repo.RepoData(name="Headphones", price=199.99, stock=100, tags='["electronics", "audio"]')
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

    #add product reviews
    review_data = [
        review_repo.RepoData(product_id=products_data[0].id, customer_id=customers_data[0].id, rating=5, review="Great product!"),
        review_repo.RepoData(product_id=products_data[1].id, customer_id=customers_data[1].id, rating=4, review="Good laptop"),
        review_repo.RepoData(product_id=products_data[2].id, customer_id=customers_data[2].id, rating=3, review="Average headphones")
    ]
    await review_repo.save_many(review_data)
    print(f"Inserted product reviews with IDs: {[review.id for review in review_data]}")

    
    #custom query: join customers, orders, and products to see which customer ordered what products
    custom_query = """
    SELECT customers.name as customer_name, products.name as product_name, orders.total_amount 
    FROM customers 
    JOIN orders ON customers.id = orders.customer_id 
    JOIN order_items ON orders.id = order_items.order_id 
    JOIN products ON order_items.product_id = products.id
    WHERE customers.id = :customer_id
    """
    result = await customer_repo.custom_query(custom_query, {"customer_id": customers_data[0].id})
    print(f"Customer Orders: {result}")

    #custom uery products with low stock
    custom_query_low_stock = "SELECT id, name, stock FROM products WHERE stock < :stock_limit"
    low_stock_products = await product_repo.custom_query(custom_query_low_stock, {"stock_limit": 50})
    print(f"Products with low stock: {low_stock_products}")


    #edge case test: inserting a customer with a duplicate email (should fail)
    try:
        duplicate_customer = customer_repo.RepoData(name="Duplicate", email="alice@example.com", phone="555-9999")
        await customer_repo.save_single(duplicate_customer)
    except Exception as e:
        print(f"Duplicate email insertion failed as expected: {e}")
    
    #edge case: insert an order for a non-existent customer (should fail)
    try:
        invalid_order = order_repo.RepoData(customer_id=9999, total_amount=500.00, status="pending")
        await order_repo.save_single(invalid_order)
    except Exception as e:
        print(f"Order for non-existent customer failed as expected: {e}")
    
    #batch delete products and ensure cascading delete for reviews and order_items
    print(f"Deleting products with IDs: {[product.id for product in products_data]}")
    for product in products_data:
        await product_repo.delete(id=product.id)
        print(f"Deleted product with ID: {product.id}")

    #verify cascading deletes in reviwes
    deleted_reviews = await review_repo.load_many()
    print(f"Product reviews after product deletion: {deleted_reviews}")
    
    #verify cascading deletes in order items
    deleted_order_items = await order_item_repo.load_many()
    print(f"Order items after product deletion: {deleted_order_items}")





if __name__ == "__main__":
    async def main():
       
        print("Running operation tests on dict schema...")
        await test_flow(dict_schema)
        
        print("Cleaning up the database...")
        AioRepositor._instance.clean_up(full=True)

        print("Running operation tests on str_schema...")
        await test_flow(dict_schema)
        
        print("Cleaning up the database...")
        AioRepositor._instance.clean_up(full=True)

    asyncio.run(main())
