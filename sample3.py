"""
====================================================================================
COMPLEX PYTHON E-COMMERCE BACKEND SIMULATION
====================================================================================

This single file simulates a multi-layered backend system for an e-commerce platform.
It is designed to be complex, lengthy (1200+ lines), and demonstrate high levels
of interdependency between different components (services, repositories, models).

Features:
- Logical sections simulating different modules (core, models, data, services, api).
- `asyncio` for asynchronous operations (simulating database/network I/O).
- Dataclasses for data models (POCO-like).
- A large collection of placeholder SQL query strings.
- Repository pattern for data access abstraction.
- Service layer for complex business logic.
- Mocked database and external services (Email, Payment) for runnable demo.
- Dependency Injection simulated in the main function.
- Custom exception classes.
- Standard logging configuration.
- Extensive type hinting for all functions and methods.

Note: In a real-world application, each class/logical section would be in its
own file (e.g., models.py, services.py).
====================================================================================
"""

# === IMPORTS ===
# Standard library imports
import asyncio
import logging
import sys
import time
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from dataclasses import dataclass, field
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Callable,
    Coroutine,
    Tuple,
    Set,
)
import random

# === GLOBAL LOGGER SETUP ===
# We'll configure this in a function, but get the logger instance here
log = logging.getLogger("ECommerceApp")


# === CORE UTILITIES (simulating core/utils.py) ===

def setup_logging():
    """Configures the global logger."""
    log.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - [%(levelname)-7s] - %(message)s"
    )
    handler.setFormatter(formatter)
    if not log.hasHandlers():
        log.addHandler(handler)
    log.info("Logging configured successfully")


class Config:
    """
    Simulates a configuration module (e.g., loaded from .env or config.ini).
    """

    DATABASE_URL: str = "postgresql+asyncpg://user:pass@mock-db-host:5432/ecommerce"
    SMTP_HOST: str = "smtp.mock-email.com"
    SMTP_PORT: int = 587
    SMTP_USER: str = "noreply@ecommerce.com"
    SMTP_PASS: str = "supersecretpassword123"
    PAYMENT_API_KEY: str = "pk_test_aBcDeFgHiJkLmNoPqRsTuVwXyZ"
    DEFAULT_CACHE_TTL: int = 300  # 5 minutes
    ADMIN_EMAIL: str = "admin@ecommerce.com"
    
    log.info("Configuration loaded")


# --- Custom Exception Classes ---

class ECommerceException(Exception):
    """Base exception for this application."""
    def __init__(self, message: str, *args):
        super().__init__(message, *args)
        self.message = message
        log.error(f"{self.__class__.__name__}: {message}")


class DataAccessException(ECommerceException):
    """Raised for errors in the repository or database layer."""
    def __init__(self, message: str, sql: Optional[str] = None, params: Optional[Dict] = None):
        self.sql = sql
        self.params = params
        super().__init__(f"Data access error: {message}")
        if sql:
            log.error(f"Failing SQL: {sql[:200]}...")
        if params:
            log.error(f"Failing Params: {params}")

class BusinessLogicException(ECommerceException):
    """Raised for validation or business rule failures (e.g., out of stock)."""
    def __init__(self, message: str, error_code: Optional[str] = None):
        self.error_code = error_code
        super().__init__(f"Business rule violation: {message}")
        if error_code:
            log.warning(f"Business error code: {error_code}")

class PaymentException(ECommerceException):
    """Raised for payment gateway failures."""
    def __init__(self, message: str, gateway_response: Optional[Any] = None):
        self.gateway_response = gateway_response
        super().__init__(f"Payment failed: {message}")

class UnauthorizedException(ECommerceException):
    """Raised for authentication or authorization failures."""
    def __init__(self, message: str = "Unauthorized access"):
        super().__init__(message)


# === SQL QUERIES (simulating core/sql_constants.py) ===

class SQLQueries:
    """
    A central repository for all SQL query strings.
    This simulates separating SQL from business logic.
    Using parameters (e.g., $1, $2 for asyncpg) is implied.
    """

    # --- User / Customer Queries ---
    GET_USER_BY_ID: str = """
        SELECT user_id, email, first_name, last_name, created_at, is_active
        FROM users
        WHERE user_id = $1;
    """
    
    GET_USER_BY_EMAIL: str = """
        SELECT user_id, email, first_name, last_name, created_at, is_active
        FROM users
        WHERE email = $1;
    """
    
    GET_USER_LOGIN_INFO: str = """
        SELECT user_id, email, hashed_password, salt
        FROM user_logins
        WHERE email = $1;
    """
    
    CREATE_USER: str = """
        INSERT INTO users (email, first_name, last_name, created_at, is_active)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING user_id;
    """
    
    CREATE_USER_LOGIN_INFO: str = """
        INSERT INTO user_logins (user_id, email, hashed_password, salt)
        VALUES ($1, $2, $3, $4);
    """
    
    GET_USER_ADDRESS: str = """
        SELECT address_id, user_id, street, city, state, zip_code, country, is_default
        FROM addresses
        WHERE user_id = $1 AND is_default = TRUE;
    """
    
    CREATE_ADDRESS: str = """
        INSERT INTO addresses (user_id, street, city, state, zip_code, country, is_default)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING address_id;
    """

    UPDATE_USER_NAME: str = """
        UPDATE users
        SET first_name = $1, last_name = $2
        WHERE user_id = $3;
    """

    # --- Product / Inventory Queries ---
    GET_PRODUCT_BY_ID: str = """
        SELECT product_id, sku, name, description, price, stock_quantity, is_active
        FROM products
        WHERE product_id = $1;
    """
    
    GET_PRODUCTS_BY_IDS: str = """
        SELECT product_id, sku, name, description, price, stock_quantity, is_active
        FROM products
        WHERE product_id = ANY($1::int[]);
    """

    GET_PRODUCTS_BY_CATEGORY: str = """
        SELECT p.product_id, p.sku, p.name, p.price, p.stock_quantity
        FROM products p
        JOIN product_categories pc ON p.product_id = pc.product_id
        WHERE pc.category_id = $1 AND p.is_active = TRUE
        LIMIT $2 OFFSET $3;
    """
    
    GET_FEATURED_PRODUCTS: str = """
        SELECT p.product_id, p.sku, p.name, p.price, p.stock_quantity
        FROM products p
        JOIN featured_products f ON p.product_id = f.product_id
        WHERE p.is_active = TRUE
        ORDER BY f.sort_order;
    """
    
    GET_STOCK_FOR_UPDATE: str = """
        SELECT stock_quantity
        FROM products
        WHERE product_id = $1
        FOR UPDATE;
    """
    
    UPDATE_PRODUCT_STOCK: str = """
        UPDATE products
        SET stock_quantity = $1
        WHERE product_id = $2;
    """

    BULK_UPDATE_PRODUCT_STOCKS: str = """
        UPDATE products AS p
        SET stock_quantity = p.stock_quantity - c.quantity
        FROM (VALUES %s) AS c(product_id, quantity)
        WHERE c.product_id = p.product_id;
    """ # Note: %s is for string formatting the VALUES list, not SQL injection

    # --- Order Queries ---
    CREATE_ORDER_HEADER: str = """
        INSERT INTO orders (user_id, order_date, status, total_amount, shipping_street,
                            shipping_city, shipping_state, shipping_zip, shipping_country,
                            billing_street, billing_city, billing_state, billing_zip, billing_country)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        RETURNING order_id, order_date, status;
    """
    
    BULK_INSERT_ORDER_ITEMS: str = """
        INSERT INTO order_items (order_id, product_id, quantity, unit_price)
        VALUES %s;
    """ # Note: %s is for string formatting the VALUES list
    
    GET_ORDER_HEADER_BY_ID: str = """
        SELECT order_id, user_id, order_date, status, total_amount, shipping_street, ...
        FROM orders
        WHERE order_id = $1;
    """
    
    GET_ORDER_ITEMS_BY_ORDER_ID: str = """
        SELECT item_id, order_id, product_id, quantity, unit_price
        FROM order_items
        WHERE order_id = $1;
    """
    
    GET_ORDERS_BY_USER_ID: str = """
        SELECT order_id, order_date, status, total_amount
        FROM orders
        WHERE user_id = $1
        ORDER BY order_date DESC
        LIMIT $2 OFFSET $3;
    """
    
    UPDATE_ORDER_STATUS: str = """
        UPDATE orders
        SET status = $1
        WHERE order_id = $2;
    """
    
    # --- Payment Queries ---
    CREATE_PAYMENT_TRANSACTION: str = """
        INSERT INTO payment_transactions (order_id, gateway_tx_id, amount, currency, status, payment_method)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING transaction_id;
    """

    UPDATE_PAYMENT_TRANSACTION_STATUS: str = """
        UPDATE payment_transactions
        SET status = $1, gateway_response = $2
        WHERE transaction_id = $3;
    """

    log.info(f"Loaded {len([v for v in dir(SQLQueries) if v.isupper()])} SQL queries")


# === DOMAIN MODELS (simulating models.py) ===

class OrderStatus(Enum):
    """Enum for the state of an order."""
    PENDING = "PENDING"
    AWAITING_PAYMENT = "AWAITING_PAYMENT"
    PROCESSING = "PROCESSING"
    SHIPPED = "SHIPPED"
    DELIVERED = "DELIVERED"
    CANCELLED = "CANCELLED"
    FRAUD_REVIEW = "FRAUD_REVIEW"


@dataclass
class Address:
    """Dataclass for a physical address."""
    address_id: Optional[int]
    user_id: Optional[int]
    street: str
    city: str
    state: str
    zip_code: str
    country: str
    is_default: bool = False

    def __str__(self) -> str:
        return f"{self.street}, {self.city}, {self.state} {self.zip_code}, {self.country}"
    
    def is_valid(self) -> bool:
        """Simple validation check."""
        return all([self.street, self.city, self.state, self.zip_code, self.country])


@dataclass
class User:
    """Dataclass for a user/customer."""
    user_id: int
    email: str
    first_name: str
    last_name: str
    created_at: datetime
    is_active: bool
    default_address: Optional[Address] = None
    
    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}"


@dataclass
class Product:
    """Dataclass for a product."""
    product_id: int
    sku: str
    name: str
    description: str
    price: Decimal
    stock_quantity: int
    is_active: bool

    def is_in_stock(self, requested_quantity: int = 1) -> bool:
        """Checks if the product is active and has enough stock."""
        return self.is_active and self.stock_quantity >= requested_quantity


@dataclass
class OrderItem:
    """Dataclass for an item within an order."""
    item_id: Optional[int]
    order_id: Optional[int]
    product_id: int
    quantity: int
    unit_price: Decimal  # Price at the time of purchase

    # This would be populated by a service
    product: Optional[Product] = None

    @property
    def line_total(self) -> Decimal:
        return self.unit_price * Decimal(self.quantity)


@dataclass
class Order:
    """Dataclass for a customer order."""
    order_id: Optional[int]
    user_id: int
    items: List[OrderItem]
    status: OrderStatus
    total_amount: Decimal
    order_date: datetime
    shipping_address: Address
    billing_address: Address
    
    # This would be populated by a service
    user: Optional[User] = None


@dataclass
class PaymentTransaction:
    """Dataclass for a payment transaction record."""
    transaction_id: Optional[int]
    order_id: int
    gateway_tx_id: str
    amount: Decimal
    currency: str
    status: str  # e.g., 'succeeded', 'failed', 'pending'
    payment_method: str
    gateway_response: Optional[Dict[str, Any]] = None


log.info("Domain models loaded")


# === DATABASE LAYER (simulating database.py) ===

class DatabaseConnection:
    """
    MOCK DatabaseConnection class.
    
    In a real app, this would wrap `asyncpg.Connection` or `sqlalchemy.ext.asyncio.AsyncConnection`.
    It simulates network latency and mock data returns.
    """
    _instance_count = 0

    def __init__(self, dsn: str):
        self.dsn = dsn
        self._is_connected: bool = False
        self._in_transaction: bool = False
        self._id = DatabaseConnection._instance_count
        DatabaseConnection._instance_count += 1
        log.debug(f"DBConnection[{self._id}] created (DSN: {dsn[:25]}...)")

    async def _simulate_latency(self, min_ms: int = 20, max_ms: int = 100):
        """Simulates network I/O latency."""
        await asyncio.sleep(random.randint(min_ms, max_ms) / 1000.0)

    async def connect(self):
        """Simulates connecting to the database."""
        if self._is_connected:
            return
        await self._simulate_latency()
        self._is_connected = True
        log.info(f"DBConnection[{self._id}] connected.")

    async def close(self):
        """Simulates closing the connection."""
        if self._in_transaction:
            await self.rollback() # Auto-rollback on close if transaction is open
        self._is_connected = False
        log.info(f"DBConnection[{self._id}] closed.")

    async def begin_transaction(self):
        """Simulates
        BEGIN;
        """
        if not self._is_connected:
            await self.connect()
        if self._in_transaction:
            raise DataAccessException("Transaction already in progress")
        
        await self._simulate_latency(10, 20)
        self._in_transaction = True
        log.debug(f"DBConnection[{self._id}] transaction BEGAN.")

    async def commit(self):
        """Simulates COMMIT;"""
        if not self._in_transaction:
            raise DataAccessException("No transaction to commit")
        
        await self._simulate_latency(30, 80)
        self._in_transaction = False
        log.debug(f"DBConnection[{self._id}] transaction COMMITTED.")

    async def rollback(self):
        """Simulates ROLLBACK;"""
        if not self._in_transaction:
            log.warning(f"DBConnection[{self._id}] rollback called with no active transaction.")
            return

        await self._simulate_latency(30, 80)
        self._in_transaction = False
        log.warning(f"DBConnection[{self._id}] transaction ROLLED BACK.")

    async def execute_query(self, sql: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Simulates executing a query that returns rows (e.g., SELECT).
        This is the most complex mock, returning data based on the SQL query.
        """
        if not self._is_connected:
            await self.connect()
            
        await self._simulate_latency()
        log.debug(f"DBConnection[{self._id}] execute_query: {sql.strip().splitlines()[0]}...")
        
        # --- MOCK DATA RETURN ---
        if sql == SQLQueries.GET_USER_BY_ID:
            user_id = params.get('$1', 1)
            return [{
                "user_id": user_id,
                "email": f"mock.user.{user_id}@example.com",
                "first_name": "Mock",
                "last_name": f"User{user_id}",
                "created_at": datetime.utcnow() - timedelta(days=30),
                "is_active": True
            }]
        elif sql == SQLQueries.GET_USER_ADDRESS:
            return [{
                "address_id": 1, "user_id": params.get('$1', 1),
                "street": "123 Mockingbird Lane", "city": "Testville",
                "state": "CA", "zip_code": "90210", "country": "USA", "is_default": True
            }]
        elif sql == SQLQueries.GET_PRODUCT_BY_ID:
            prod_id = params.get('$1', 101)
            return [{
                "product_id": prod_id, "sku": f"SKU-{prod_id}-MOCK",
                "name": f"Mock Product {prod_id}", "description": "A fantastic mock product.",
                "price": Decimal(str(random.uniform(10.0, 100.0))),
                "stock_quantity": 100, "is_active": True
            }]
        elif sql == SQLQueries.GET_PRODUCTS_BY_IDS:
            prod_ids = params.get('$1', [101, 102])
            results = []
            for pid in prod_ids:
                 results.append({
                    "product_id": pid, "sku": f"SKU-{pid}-MOCK",
                    "name": f"Mock Product {pid}", "description": "A fantastic mock product.",
                    "price": Decimal(f"{random.randint(10, 99)}.99"),
                    "stock_quantity": 50, "is_active": True
                })
            return results
        elif sql == SQLQueries.GET_STOCK_FOR_UPDATE:
             return [{"stock_quantity": 100}]
        elif sql == SQLQueries.GET_ORDER_HEADER_BY_ID:
            # ... implementation omitted
            return []
        elif sql == SQLQueries.GET_ORDER_ITEMS_BY_ORDER_ID:
            # ... implementation omitted
            return []
        
        log.warning(f"DBConnection[{self._id}] no mock data for query: {sql.strip().splitlines()[0]}...")
        return []

    async def execute_scalar(self, sql: str, params: Dict[str, Any]) -> Any:
        """
        Simulates executing a query that returns a single value
        (e.g., INSERT ... RETURNING id).
        """
        if not self._is_connected:
            await self.connect()
            
        await self._simulate_latency()
        log.debug(f"DBConnection[{self._id}] execute_scalar: {sql.strip().splitlines()[0]}...")
        
        if "INSERT INTO users" in sql:
            return random.randint(1000, 9999)  # Return new user_id
        if "INSERT INTO orders" in sql:
            return random.randint(10000, 99999) # Return new order_id
        if "INSERT INTO payment_transactions" in sql:
            return random.randint(100000, 999999) # Return new tx_id
            
        return 1 # Default for UPDATE/DELETE row count

    async def execute_many(self, sql_template: str, params_list: List[Dict[str, Any]]):
        """Simulates executemany (e.g., bulk inserts)."""
        if not self._is_connected:
            await self.connect()

        if not self._in_transaction:
            log.warning(f"DBConnection[{self._id}] execute_many called outside transaction.")
            # In a real app, this might be an error or auto-wrap in a transaction
            
        await self._simulate_latency(50, 150) # Slower for bulk operations
        log.debug(f"DBConnection[{self._id}] execute_many: {sql_template.strip().splitlines()[0]}... ({len(params_list)} items)")
        # No return value, just simulates the execution
        
        
# A "Factory" to provide connection instances, simulating a DI pool.
def get_db_connection_factory(config: Config) -> Callable[[], DatabaseConnection]:
    """Returns a factory function that creates new DB connections."""
    def factory() -> DatabaseConnection:
        return DatabaseConnection(config.DATABASE_URL)
    return factory


log.info("Database layer simulation loaded")

# === REPOSITORY LAYER (simulating repositories.py) ===

class BaseRepository:
    """Base class for all repositories."""
    def __init__(self, db_factory: Callable[[], DatabaseConnection]):
        self.db_factory = db_factory
        log.debug(f"{self.__class__.__name__} initialized")
        
    def _map_row_to_model(self, row: Dict[str, Any], model_class: Any) -> Any:
        """Utility to map a DB row (dict) to a dataclass."""
        # This is a simple mapper; a real one would handle column name mismatches
        return model_class(**row)


class UserRepository(BaseRepository):
    """Handles data access for User models."""

    async def get_by_id(self, user_id: int) -> Optional[User]:
        """Fetches a user by their ID."""
        sql = SQLQueries.GET_USER_BY_ID
        params = {"$1": user_id}
        db = self.db_factory()
        try:
            await db.connect()
            rows = await db.execute_query(sql, params)
            if not rows:
                return None
            
            # Also fetch address
            addr_sql = SQLQueries.GET_USER_ADDRESS
            addr_params = {"$1": user_id}
            addr_rows = await db.execute_query(addr_sql, addr_params)
            
            user_data = rows[0]
            user = User(**user_data)
            
            if addr_rows:
                user.default_address = Address(**addr_rows[0])
                
            return user
        except Exception as e:
            raise DataAccessException(f"Failed to get user {user_id}", sql, params) from e
        finally:
            await db.close()

    async def get_by_email(self, email: str) -> Optional[User]:
        """Fetches a user by their email."""
        sql = SQLQueries.GET_USER_BY_EMAIL
        params = {"$1": email}
        # ... (implementation similar to get_by_id) ...
        log.debug(f"Fetching user by email {email}")
        return await self.get_by_id(1) # Mocked return

    async def create(self, email: str, first_name: str, last_name: str, password_hash: str, salt: str) -> User:
        """
        Creates a new user and their login info in a transaction.
        """
        db = self.db_factory()
        try:
            await db.begin_transaction()
            
            # 1. Create User
            user_sql = SQLQueries.CREATE_USER
            user_params = {
                "$1": email, "$2": first_name, "$3": last_name,
                "$4": datetime.utcnow(), "$5": True
            }
            new_user_id = await db.execute_scalar(user_sql, user_params)
            
            if not new_user_id:
                raise DataAccessException("Failed to create user, no ID returned")

            # 2. Create Login Info
            login_sql = SQLQueries.CREATE_USER_LOGIN_INFO
            login_params = {
                "$1": new_user_id, "$2": email,
                "$3": password_hash, "$4": salt
            }
            await db.execute_scalar(login_sql, login_params)
            
            await db.commit()
            
            log.info(f"Created new user with ID {new_user_id}")
            
            # Return the newly created user object
            return User(
                user_id=new_user_id,
                email=email,
                first_name=first_name,
                last_name=last_name,
                created_at=datetime.utcnow(),
                is_active=True
            )
        except Exception as e:
            await db.rollback()
            raise DataAccessException(f"Failed to create user {email}", user_sql) from e
        finally:
            await db.close()
            

class ProductRepository(BaseRepository):
    """Handles data access for Product models."""

    async def get_by_id(self, product_id: int) -> Optional[Product]:
        """Fetches a single product by its ID."""
        sql = SQLQueries.GET_PRODUCT_BY_ID
        params = {"$1": product_id}
        db = self.db_factory()
        try:
            await db.connect()
            rows = await db.execute_query(sql, params)
            if not rows:
                return None
            return Product(**rows[0])
        except Exception as e:
            raise DataAccessException(f"Failed to get product {product_id}", sql, params) from e
        finally:
            await db.close()

    async def get_by_ids(self, product_ids: List[int]) -> List[Product]:
        """Fetches multiple products by their IDs."""
        if not product_ids:
            return []
        sql = SQLQueries.GET_PRODUCTS_BY_IDS
        params = {"$1": product_ids} # Assumes DB driver can handle list parameter
        db = self.db_factory()
        try:
            await db.connect()
            rows = await db.execute_query(sql, params)
            return [Product(**row) for row in rows]
        except Exception as e:
            raise DataAccessException(f"Failed to get products by IDs", sql, params) from e
        finally:
            await db.close()

    async def get_stock_for_update(self, product_id: int, db: DatabaseConnection) -> Optional[int]:
        """
        Gets the stock level for a product, locking the row.
        MUST be called within a transaction.
        """
        if not db or not db._in_transaction:
            raise DataAccessException("get_stock_for_update must be called within a transaction")
            
        sql = SQLQueries.GET_STOCK_FOR_UPDATE
        params = {"$1": product_id}
        rows = await db.execute_query(sql, params)
        if not rows:
            return None
        return int(rows[0]["stock_quantity"])

    async def update_stock(self, product_id: int, new_stock: int, db: DatabaseConnection) -> bool:
        """
        Updates a product's stock level.
        MUST be called within a transaction.
        """
        if not db or not db._in_transaction:
            raise DataAccessException("update_stock must be called within a transaction")
        
        sql = SQLQueries.UPDATE_PRODUCT_STOCK
        params = {"$1": new_stock, "$2": product_id}
        rows_affected = await db.execute_scalar(sql, params)
        return rows_affected > 0


class OrderRepository(BaseRepository):
    """Handles data access for Order models."""

    async def create_order_in_transaction(self, order: Order, db: DatabaseConnection) -> Order:
        """
        Creates an order header and all items within a transaction.
        Assumes the transaction is already started.
        """
        if not db or not db._in_transaction:
            raise DataAccessException("create_order_in_transaction must be called within a transaction")

        try:
            # 1. Create Order Header
            header_sql = SQLQueries.CREATE_ORDER_HEADER
            header_params = {
                "$1": order.user_id, "$2": order.order_date, "$3": order.status.value,
                "$4": order.total_amount,
                "$5": order.shipping_address.street, "$6": order.shipping_address.city,
                "$7": order.shipping_address.state, "$8": order.shipping_address.zip_code,
                "$9": order.shipping_address.country,
                "$10": order.billing_address.street, "$11": order.billing_address.city,
                "$12": order.billing_address.state, "$13": order.billing_address.zip_code,
                "$14": order.billing_address.country,
            }
            new_order_id = await db.execute_scalar(header_sql, header_params)
            if not new_order_id:
                raise DataAccessException("Failed to create order header, no ID returned")
                
            order.order_id = new_order_id
            log.debug(f"Created order header {new_order_id}")

            # 2. Bulk Insert Order Items
            if not order.items:
                log.warning(f"Order {new_order_id} created with no items.")
                return order
                
            item_sql_template = SQLQueries.BULK_INSERT_ORDER_ITEMS
            # This is complex. A real app would use a driver's `executemany`
            # or a complex `VALUES (...), (...), ...` string.
            # We will simulate with `execute_many`.
            
            item_params_list = []
            for item in order.items:
                item.order_id = new_order_id
                item_params_list.append({
                    "$1": item.order_id,
                    "$2": item.product_id,
                    "$3": item.quantity,
                    "$4": item.unit_price
                })
            
            # We'll just fake the SQL string for the mock
            await db.execute_many(item_sql_template % "...", item_params_list)
            log.debug(f"Bulk inserted {len(item_params_list)} items for order {new_order_id}")
            
            return order
        except Exception as e:
            # Let the calling service handle the rollback
            raise DataAccessException(f"Failed to create order items for user {order.user_id}", str(e)) from e

    async def update_status(self, order_id: int, new_status: OrderStatus) -> bool:
        """Updates an order's status."""
        sql = SQLQueries.UPDATE_ORDER_STATUS
        params = {"$1": new_status.value, "$2": order_id}
        db = self.db_factory()
        try:
            await db.connect()
            rows_affected = await db.execute_scalar(sql, params)
            return rows_affected > 0
        except Exception as e:
            raise DataAccessException(f"Failed to update status for order {order_id}", sql, params) from e
        finally:
            await db.close()

    async def get_by_id(self, order_id: int) -> Optional[Order]:
        """Gets a full order (header + items) by ID."""
        # ... implementation omitted for brevity ...
        # This would involve 2 queries: one for header, one for items
        log.debug(f"Mock-fetching full order for {order_id}")
        return None # Placeholder

class PaymentRepository(BaseRepository):
    """Handles data access for PaymentTransaction models."""
    
    async def create_transaction(self, tx: PaymentTransaction, db: Optional[DatabaseConnection] = None) -> PaymentTransaction:
        """Creates a new payment transaction record."""
        sql = SQLQueries.CREATE_PAYMENT_TRANSACTION
        params = {
            "$1": tx.order_id, "$2": tx.gateway_tx_id, "$3": tx.amount,
            "$4": tx.currency, "$5": tx.status, "$6": tx.payment_method
        }
        
        # Determine if we use the provided connection or create a new one
        conn_manager = db if db else self.db_factory()
        
        try:
            if not db: await conn_manager.connect()
            
            new_tx_id = await conn_manager.execute_scalar(sql, params)
            tx.transaction_id = new_tx_id
            return tx
        except Exception as e:
            raise DataAccessException("Failed to create payment transaction", sql, params) from e
        finally:
            if not db: await conn_manager.close()
            

log.info("Repository layer loaded")

# === EXTERNAL SERVICES (simulating services/external.py) ===

class EmailService:
    """
    MOCK Email Service.
    In a real app, this would use `aiosmtplib` or an HTTP API (SendGrid, Mailgun).
    """
    def __init__(self, config: Config):
        self.config = config
        log.info(f"EmailService configured for {config.SMTP_HOST} as {config.SMTP_USER}")

    async def send_email(self, to: str, subject: str, body_html: str) -> bool:
        """Simulates sending an email."""
        log.info(f"--- MOCK EMAIL SEND ---")
        log.info(f"To: {to}")
        log.info(f"From: {self.config.SMTP_USER}")
        log.info(f"Subject: {subject}")
        log.info(f"Body (snippet): {body_html[:75].replace('<p>', '').replace('</p>', ' ')}...")
        
        await asyncio.sleep(random.uniform(0.1, 0.3)) # Simulate network call
        
        if "fail@example.com" in to:
            log.error(f"Simulated SMTP failure for {to}")
            return False
            
        log.info(f"--- EMAIL SENT ---")
        return True


class PaymentGateway:
    """
    MOCK Payment Gateway Service.
    In a real app, this would use `aiohttp` to call Stripe, PayPal, etc.
    """
    def __init__(self, config: Config):
        self.api_key = config.PAYMENT_API_KEY
        log.info(f"PaymentGateway configured (API Key: {self.api_key[:8]}..._test)")

    async def process_payment(self, amount: Decimal, currency: str, token: str) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Simulates processing a payment token.
        Returns (success, transaction_id, gateway_response)
        """
        log.info(f"Processing payment for {amount} {currency} with token {token[:6]}...")
        await asyncio.sleep(random.uniform(0.5, 1.5)) # Simulate slow API call
        
        if token == "tok_fail_card_declined":
            log.warning("Payment failed: Card declined (simulated)")
            response = {"id": f"ch_fail_{random.randint(1000,9999)}", "status": "failed", "failure_code": "card_declined"}
            return (False, response["id"], response)

        if token == "tok_fail_fraud":
            log.warning("Payment failed: Fraud detected (simulated)")
            response = {"id": f"ch_fail_{random.randint(1000,9999)}", "status": "failed", "failure_code": "fraudulent"}
            return (False, response["id"], response)

        log.info("Payment successful (simulated)")
        response = {"id": f"ch_pass_{random.randint(1000,9999)}", "status": "succeeded", "amount": float(amount)}
        return (True, response["id"], response)


log.info("External services loaded")

# === BUSINESS LOGIC SERVICES (simulating services/logic.py) ===

class NotificationService:
    """Handles sending notifications to users."""
    
    def __init__(self, email_service: EmailService, config: Config):
        self.email_service = email_service
        self.config = config
        log.debug("NotificationService initialized")

    async def send_order_confirmation(self, user: User, order: Order):
        """Sends an order confirmation email."""
        subject = f"Your order #{order.order_id} is confirmed!"
        body = f"""
        <p>Hi {user.first_name},</p>
        <p>Thank you for your order! We're getting it ready.</p>
        <p><b>Order ID:</b> {order.order_id}</p>
        <p><b>Total:</b> {order.total_amount:.2f}</p>
        <p>We'll notify you when it ships. You can view your order status in your account.</p>
        <p>Thanks,<br/>The E-Commerce Team</p>
        """
        try:
            await self.email_service.send_email(user.email, subject, body)
        except Exception as e:
            # Log and fail silently. Notification failure should not
            # block the main application flow.
            log.exception(f"Failed to send order confirmation for order {order.order_id}", exc_info=e)

    async def notify_admin_of_fraud(self, order: Order, reason: str):
        """Sends an urgent email to the admin about a fraud review."""
        subject = f"[ACTION REQUIRED] Order #{order.order_id} Flagged for Fraud"
        body = f"""
        <p>Admin,</p>
        <p>Order <b>{order.order_id}</b> for user {order.user_id} ({order.user.email})
        was flagged for manual fraud review.</p>
        <p><b>Reason:</b> {reason}</p>
        <p><b>Total:</b> {order.total_amount:.2f}</p>
        <p>Please review this order in the admin panel immediately.</p>
        """
        try:
            await self.email_service.send_email(self.config.ADMIN_EMAIL, subject, body)
        except Exception as e:
            log.exception(f"CRITICAL: Failed to send fraud alert for order {order.order_id}", exc_info=e)


class InventoryService:
    """Handles logic for inventory and stock management."""
    
    def __init__(self, product_repo: ProductRepository):
        self.product_repo = product_repo
        log.debug("InventoryService initialized")
        
    async def reserve_stock_in_transaction(self, cart: Dict[int, int], db: DatabaseConnection):
        """
        Reserves stock for all items in a cart within a DB transaction.
        This is a critical, complex operation.
        
        cart = {product_id: quantity}
        """
        if not db or not db._in_transaction:
            raise BusinessLogicException("reserve_stock must be called within a transaction")
            
        log.info(f"Attempting to reserve stock for {len(cart)} product(s)...")
        
        products_to_update: List[Tuple[int, int]] = []
        
        for product_id, quantity in cart.items():
            if quantity <= 0:
                raise BusinessLogicException(f"Invalid quantity {quantity} for product {product_id}")
                
            # 1. Get current stock with a row lock
            current_stock = await self.product_repo.get_stock_for_update(product_id, db)
            
            if current_stock is None:
                raise BusinessLogicException(f"Product {product_id} not found", "PRODUCT_NOT_FOUND")

            # 2. Check if stock is sufficient
            if current_stock < quantity:
                log.warning(f"Insufficient stock for product {product_id}. Requested: {quantity}, Available: {current_stock}")
                raise BusinessLogicException(
                    f"Insufficient stock for product {product_id}. "
                    f"Requested: {quantity}, Available: {current_stock}",
                    "INSUFFICIENT_STOCK"
                )
            
            # 3. Prepare the update
            new_stock = current_stock - quantity
            products_to_update.append((product_id, new_stock))
            log.debug(f"Stock check OK for {product_id}. Reserving {quantity}. New stock will be {new_stock}")
        
        # 4. Perform all updates
        # In a real app, this would be a single bulk update query.
        # We will do it one-by-one for this mock.
        for product_id, new_stock in products_to_update:
            success = await self.product_repo.update_stock(product_id, new_stock, db)
            if not success:
                # This should not happen if the lock was successful
                raise DataAccessException(f"Failed to update stock for {product_id} even after lock")
                
        log.info(f"Successfully reserved stock for all {len(cart)} items.")


class PricingService:
    """Handles complex pricing, tax, and shipping calculations."""
    
    def __init__(self):
        log.debug("PricingService initialized")

    async def calculate_subtotal(self, items: List[Tuple[Product, int]]) -> Decimal:
        """Calculates subtotal from a list of (Product, quantity) tuples."""
        await asyncio.sleep(0.01) # Simulate some logic
        subtotal = sum(
            prod.price * Decimal(qty)
            for prod, qty in items
        )
        return subtotal.quantize(Decimal("0.01"))
        
    async def calculate_shipping(self, subtotal: Decimal, address: Address) -> Decimal:
        """Simulates a call to a shipping calculator (e.g., FedEx/UPS API)."""
        await asyncio.sleep(0.05) # Simulate external API call
        
        if subtotal > Decimal("100.00"):
            return Decimal("0.00") # Free shipping
            
        if address.country != "USA":
            return Decimal("19.99")
            
        if address.state in ("CA", "NY", "TX"):
            return Decimal("8.99")
            
        return Decimal("5.99")
        
    async def calculate_tax(self, subtotal: Decimal, address: Address) -> Decimal:
        """Simulates a call to a tax calculation service (e.g., Avalara)."""
        await asyncio.sleep(0.08) # Simulate external API call
        
        tax_rate = Decimal("0.00")
        if address.country == "USA":
            if address.state == "CA":
                tax_rate = Decimal("0.0725")
            elif address.state == "NY":
                tax_rate = Decimal("0.08875")
            elif address.state == "FL":
                tax_rate = Decimal("0.06")
        
        tax = (subtotal * tax_rate).quantize(Decimal("0.01"))
        return tax

    async def calculate_total(self, cart: Dict[int, int], products: List[Product], address: Address) -> Tuple[Decimal, Decimal, Decimal, Decimal]:
        """
        Calculates subtotal, shipping, tax, and grand total.
        Returns (subtotal, shipping, tax, total)
        """
        product_map = {p.product_id: p for p in products}
        items_with_products = [
            (product_map[pid], qty) for pid, qty in cart.items()
            if pid in product_map
        ]
        
        subtotal = await self.calculate_subtotal(items_with_products)
        shipping = await self.calculate_shipping(subtotal, address)
        tax = await self.calculate_tax(subtotal, address)
        
        total = (subtotal + shipping + tax).quantize(Decimal("0.01"))
        
        return (subtotal, shipping, tax, total)
        

class OrderProcessingService:
    """
    THE MOST COMPLEX, INTERDEPENDENT SERVICE.
    Orchestrates the entire order placement process.
    """
    def __init__(
        self,
        db_factory: Callable[[], DatabaseConnection],
        user_repo: UserRepository,
        product_repo: ProductRepository,
        order_repo: OrderRepository,
        payment_repo: PaymentRepository,
        inventory_service: InventoryService,
        pricing_service: PricingService,
        payment_gateway: PaymentGateway,
        notification_service: NotificationService
    ):
        self.db_factory = db_factory
        self.user_repo = user_repo
        self.product_repo = product_repo
        self.order_repo = order_repo
        self.payment_repo = payment_repo
        self.inventory_service = inventory_service
        self.pricing_service = pricing_service
        self.payment_gateway = payment_gateway
        self.notification_service = notification_service
        log.debug("OrderProcessingService initialized with all dependencies")

    async def place_order(
        self,
        user_id: int,
        cart: Dict[int, int],  # {product_id: quantity}
        shipping_address: Address,
        billing_address: Address,
        payment_token: str
    ) -> Order:
        """
        Main orchestration method for placing an order.
        
        Workflow:
        1. Validate inputs (cart, addresses).
        2. Get User and Product data.
        3. Calculate totals (subtotal, shipping, tax, total).
        4. Process payment via the gateway.
        5. If payment succeeds, START TRANSACTION.
        6.   a. Reserve stock (locks rows and updates).
        7.   b. Create Order and OrderItems in the database.
        8.   c. Create PaymentTransaction record.
        9. COMMIT TRANSACTION.
        10. If anything fails (payment or DB), rollback and raise.
        11. Send confirmation email (post-transaction).
        """
        log.info(f"Attempting to place order for user {user_id} with {len(cart)} item(s)")
        
        # 1. Validate inputs
        if not cart:
            raise BusinessLogicException("Cart cannot be empty", "EMPTY_CART")
        if not shipping_address.is_valid():
            raise BusinessLogicException("Invalid shipping address", "INVALID_SHIPPING_ADDRESS")
        if not billing_address.is_valid():
            raise BusinessLogicException("Invalid billing address", "INVALID_BILLING_ADDRESS")
            
        # 2. Get User and Product data
        user = await self.user_repo.get_by_id(user_id)
        if not user:
            raise BusinessLogicException(f"User {user_id} not found", "USER_NOT_FOUND")
        
        product_ids = list(cart.keys())
        products = await self.product_repo.get_by_ids(product_ids)
        if len(products) != len(product_ids):
            found_ids = {p.product_id for p in products}
            missing_ids = [pid for pid in product_ids if pid not in found_ids]
            raise BusinessLogicException(f"Products not found: {missing_ids}", "PRODUCT_NOT_FOUND")
            
        # 3. Calculate totals
        try:
            (subtotal, shipping, tax, total) = await self.pricing_service.calculate_total(
                cart, products, shipping_address
            )
            log.info(f"Calculated totals for user {user_id}: Sub={subtotal}, Ship={shipping}, Tax={tax}, TOTAL={total}")
        except Exception as e:
            log.exception("Price calculation failed", exc_info=e)
            raise BusinessLogicException("Failed to calculate order total", "PRICING_ERROR") from e
            
        # 4. Process payment
        try:
            success, gateway_tx_id, gateway_response = await self.payment_gateway.process_payment(
                total, "USD", payment_token
            )
            if not success:
                # Payment failed, do NOT proceed.
                raise PaymentException(
                    f"Payment declined: {gateway_response.get('failure_code', 'Unknown')}",
                    gateway_response
                )
        except Exception as e:
            log.exception(f"Payment gateway failed for user {user_id}", exc_info=e)
            if isinstance(e, PaymentException): raise
            raise PaymentException(f"Payment processing error: {e}") from e
            
        log.info(f"Payment successful for user {user_id}. Gateway TX ID: {gateway_tx_id}")

        # 5. --- BEGIN DATABASE TRANSACTION ---
        # This is the critical, atomic part of the operation.
        db = self.db_factory()
        new_order: Optional[Order] = None
        
        try:
            await db.begin_transaction()
            
            # 6. a. Reserve stock
            await self.inventory_service.reserve_stock_in_transaction(cart, db)
            
            # 7. b. Create Order and OrderItems
            product_map = {p.product_id: p for p in products}
            order_items = [
                OrderItem(
                    item_id=None, order_id=None, # Will be set by repo
                    product_id=pid,
                    quantity=qty,
                    unit_price=product_map[pid].price # In real life, check for sales, etc.
                ) for pid, qty in cart.items()
            ]
            
            order_to_create = Order(
                order_id=None, # Will be set by repo
                user_id=user_id,
                items=order_items,
                status=OrderStatus.PROCESSING,
                total_amount=total,
                order_date=datetime.utcnow(),
                shipping_address=shipping_address,
                billing_address=billing_address
            )
            
            new_order = await self.order_repo.create_order_in_transaction(order_to_create, db)
            
            # 8. c. Create PaymentTransaction record
            tx_record = PaymentTransaction(
                transaction_id=None,
                order_id=new_order.order_id,
                gateway_tx_id=gateway_tx_id,
                amount=total,
                currency="USD",
                status="succeeded",
                payment_method="card",
                gateway_response=gateway_response
            )
            await self.payment_repo.create_transaction(tx_record, db)
            
            # 9. COMMIT TRANSACTION
            await db.commit()
            
            log.info(f"Successfully created order {new_order.order_id} and committed to DB.")
            
        except (BusinessLogicException, DataAccessException, PaymentException) as e:
            # These are expected failures (e.g., out of stock during transaction)
            log.warning(f"Failed to commit order for user {user_id}: {e.message}")
            await db.rollback()
            # TODO: We should refund the payment here!
            # await self.payment_gateway.refund(gateway_tx_id)
            raise e # Re-raise the specific exception
        except Exception as e:
            # Unexpected failure
            log.exception(f"CRITICAL: Unexpected error during order commit for user {user_id}", exc_info=e)
            await db.rollback()
            # TODO: We should refund the payment here!
            # await self.payment_gateway.refund(gateway_tx_id)
            raise DataAccessException(f"Unexpected error, order rolled back: {e}") from e
        finally:
            await db.close()

        # 10. Send confirmation email (post-transaction)
        # This is non-atomic. If this fails, the order is still placed.
        try:
            new_order.user = user # Attach user for notification service
            await self.notification_service.send_order_confirmation(user, new_order)
        except Exception as e:
            log.exception(f"Order {new_order.order_id} placed, but confirmation email failed", exc_info=e)
            
        return new_order


log.info("Business logic services loaded")

# === API/PRESENTATION LAYER (simulating api/main.py) ===

class ApiApplication:
    """
    Simulates a web application (e.g., FastAPI or Flask)
    that routes requests to the correct services.
    """
    def __init__(
        self,
        order_service: OrderProcessingService,
        user_repo: UserRepository
    ):
        self.order_service = order_service
        self.user_repo = user_repo
        log.info("API Application created and wired")

    async def endpoint_place_order(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Simulates a POST /api/v1/orders endpoint.
        
        Expected request_data:
        {
            "user_id": 1,
            "cart": {101: 1, 102: 2},
            "shipping_address": {...},
            "billing_address": {...},
            "payment_token": "tok_visa"
        }
        """
        log.info(f"--- API Endpoint: /api/v1/orders ---")
        try:
            # 1. Deserialize/Validate (Pydantic would do this)
            user_id = int(request_data["user_id"])
            cart = {int(k): int(v) for k, v in request_data["cart"].items()}
            shipping_addr = Address(address_id=None, **request_data["shipping_address"])
            billing_addr = Address(address_id=None, **request_data["billing_address"])
            payment_token = str(request_data["payment_token"])

            # 2. Call the service
            new_order = await self.order_service.place_order(
                user_id, cart, shipping_addr, billing_addr, payment_token
            )
            
            # 3. Serialize response
            return {
                "status": "success",
                "order_id": new_order.order_id,
                "status": new_order.status.value,
                "total_amount": f"{new_order.total_amount:.2f}",
                "order_date": new_order.order_date.isoformat()
            }
            
        except (BusinessLogicException, PaymentException) as e:
            log.warning(f"Order placement failed (400 Bad Request): {e.message}")
            return {"status": "error", "message": e.message, "error_code": getattr(e, 'error_code', None)}
        except (DataAccessException, UnauthorizedException) as e:
            log.error(f"Order placement failed (500 Server Error): {e.message}")
            return {"status": "error", "message": "An internal server error occurred."}
        except Exception as e:
            log.exception(f"CRITICAL: Unhandled exception in place_order endpoint", exc_info=e)
            return {"status": "error", "message": "An unexpected server error occurred."}
            
    async def endpoint_get_user(self, user_id: int) -> Dict[str, Any]:
        """Simulates a GET /api/v1/users/{id} endpoint."""
        log.info(f"--- API Endpoint: /api/v1/users/{user_id} ---")
        user = await self.user_repo.get_by_id(user_id)
        if not user:
            return {"status": "error", "message": "User not found"}
        
        return {
            "status": "success",
            "data": {
                "user_id": user.user_id,
                "email": user.email,
                "full_name": user.full_name,
                "default_address": str(user.default_address) if user.default_address else None
            }
        }

log.info("API layer simulation loaded")

# === MAIN EXECUTION BLOCK (simulating run.py) ===

async def main():
    """
    Main asynchronous function to set up and run the simulation.
    This simulates the Dependency Injection container setup.
    """
    setup_logging()
    log.info("====================================")
    log.info("=  E-Commerce App Simulation START =")
    log.info("====================================")
    
    start_time = time.monotonic()

    # --- 1. Dependency Injection / App Setup ---
    log.info("--- Setting up dependencies (DI Container) ---")
    config = Config()
    
    # Factories
    db_factory = get_db_connection_factory(config)
    
    # External Services
    email_service = EmailService(config)
    payment_gateway = PaymentGateway(config)
    
    # Repositories
    user_repo = UserRepository(db_factory)
    product_repo = ProductRepository(db_factory)
    order_repo = OrderRepository(db_factory)
    payment_repo = PaymentRepository(db_factory)
    
    # Business Logic Services
    inventory_service = InventoryService(product_repo)
    pricing_service = PricingService()
    notification_service = NotificationService(email_service, config)
    
    # Main Orchestration Service
    order_service = OrderProcessingService(
        db_factory=db_factory,
        user_repo=user_repo,
        product_repo=product_repo,
        order_repo=order_repo,
        payment_repo=payment_repo,
        inventory_service=inventory_service,
        pricing_service=pricing_service,
        payment_gateway=payment_gateway,
        notification_service=notification_service
    )
    
    # API Application
    api_app = ApiApplication(
        order_service=order_service,
        user_repo=user_repo
    )
    
    log.info("--- All dependencies wired up. ---")
    
    # --- 2. Run Simulation Scenarios ---

    # Scenario 1: Get a user
    log.info("\n--- SCENARIO 1: Get User ---")
    user_response = await api_app.endpoint_get_user(user_id=1)
    log.info(f"Get User Response: {user_response}")

    # Scenario 2: Place a successful order
    log.info("\n--- SCENARIO 2: Place Successful Order ---")
    success_order_request = {
        "user_id": 1,
        "cart": {101: 1, 102: 2}, # product 101 (qty 1), product 102 (qty 2)
        "shipping_address": {
            "user_id": 1, "street": "123 Main St", "city": "Anytown",
            "state": "CA", "zip_code": "12345", "country": "USA"
        },
        "billing_address": {
            "user_id": 1, "street": "123 Main St", "city": "Anytown",
            "state": "CA", "zip_code": "12345", "country": "USA"
        },
        "payment_token": "tok_visa_success"
    }
    order_response = await api_app.endpoint_place_order(success_order_request)
    log.info(f"Place Order (Success) Response: {order_response}")
    
    # Scenario 3: Place a failed order (Card Declined)
    log.info("\n--- SCENARIO 3: Place Failed Order (Card Declined) ---")
    fail_order_request = success_order_request.copy()
    fail_order_request["payment_token"] = "tok_fail_card_declined"
    
    order_response_fail = await api_app.endpoint_place_order(fail_order_request)
    log.info(f"Place Order (Failed) Response: {order_response_fail}")

    # Scenario 4: Place a failed order (Out of Stock)
    # This is harder to mock without changing the mock DB
    # We'll simulate it by requesting a huge quantity
    log.info("\n--- SCENARIO 4: Place Failed Order (Out of Stock) ---")
    out_of_stock_request = success_order_request.copy()
    out_of_stock_request["cart"] = {101: 1000} # Mock DB only has 100
    out_of_stock_request["payment_token"] = "tok_visa_success_2"
    
    order_response_stock = await api_app.endpoint_place_order(out_of_stock_request)
    log.info(f"Place Order (Out of Stock) Response: {order_response_stock}")


    end_time = time.monotonic()
    log.info("====================================")
    log.info(f"=  Simulation FINISHED in {end_time - start_time:.4f}s  =")
    log.info("====================================")


if __name__ == "__main__":
    """
    Main entry point for the script.
    """
    try:
        asyncio.run(main())
    except Exception as e:
        log.exception("A fatal error occurred during the simulation.", exc_info=e)
        sys.exit(1)
