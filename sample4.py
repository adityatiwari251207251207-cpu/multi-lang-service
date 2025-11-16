#!/usr/bin/env python3

"""
ecommerce_backend.py

A comprehensive backend service module for a large-scale e-commerce platform.
This file includes interdependent services for managing users, products, orders,
and generating complex reports. All database interactions are represented
as string-based SQL queries for demonstration.

Author: AI (Gemini)
Version: 1.0.0
"""

# Standard Library Imports
import sqlite3
import logging
import datetime
import hashlib
import json
import uuid
import re
from decimal import Decimal, getcontext
from typing import List, Dict, Any, Optional, Tuple, Union
from collections import namedtuple

# Set precision for Decimal operations
getcontext().prec = 10

# --- Configuration & Constants ---

# Database Configuration
DB_NAME = 'ecommerce_main.db'

# Logging Configuration
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    filename='ecommerce_service.log')
logger = logging.getLogger(__name__)

# Application Constants
DEFAULT_CURRENCY = 'USD'
PASSWORD_SALT = 'a_very_secret_ecommerce_salt_string'
MIN_PASSWORD_LENGTH = 8
MAX_ORDER_ITEMS = 50
SHIPPING_FEE_STANDARD = Decimal('5.99')
SHIPPING_FEE_EXPRESS = Decimal('15.99')
FREE_SHIPPING_THRESHOLD = Decimal('100.00')

# Order Statuses (simulating an Enum)
STATUS_PENDING = 'PENDING'
STATUS_PAID = 'PAID'
STATUS_SHIPPED = 'SHIPPED'
STATUS_DELIVERED = 'DELIVERED'
STATUS_CANCELLED = 'CANCELLED'
STATUS_REFUNDED = 'REFUNDED'
VALID_STATUSES = {STATUS_PENDING, STATUS_PAID, STATUS_SHIPPED, STATUS_DELIVERED, STATUS_CANCELLED, STATUS_REFUNDED}

# User Roles
ROLE_CUSTOMER = 'CUSTOMER'
ROLE_ADMIN = 'ADMIN'
ROLE_SUPPORT = 'SUPPORT'
VALID_ROLES = {ROLE_CUSTOMER, ROLE_ADMIN, ROLE_SUPPORT}

# Custom Exception Classes
class DatabaseError(Exception):
    """Exception raised for database-related errors."""
    pass

class ValidationError(ValueError):
    """Exception raised for data validation failures."""
    pass

class AuthenticationError(SecurityException):
    """Exception raised for auth failures."""
    pass

class OrderProcessingError(Exception):
    """Exception raised during order processing."""
    pass

class InventoryError(Exception):
    """Exception raised for inventory-related issues."""
    pass


# --- Database Manager Class ---

class DatabaseManager:
    """
    Handles all low-level database connections and query executions.
    This class is instantiated and used by all other services.
    """

    def __init__(self, db_path: str):
        """
        Initializes the database manager.
        :param db_path: Filesystem path to the SQLite database.
        """
        self.db_path = db_path
        self.connection = None
        logger.info(f"DatabaseManager initialized for: {db_path}")

    def connect(self) -> sqlite3.Connection:
        """
        Establishes and returns a database connection.
        """
        try:
            if not self.connection or self.connection.total_changes == -1:
                self.connection = sqlite3.connect(self.db_path)
                self.connection.row_factory = sqlite3.Row
                self.connection.execute("PRAGMA foreign_keys = ON;")
                logger.info("New database connection established.")
            return self.connection
        except sqlite3.Error as e:
            logger.error(f"Failed to connect to database at {self.db_path}: {e}")
            raise DatabaseError(f"Database connection failure: {e}")

    def disconnect(self):
        """
        Closes the database connection if it exists.
        """
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Database connection closed.")

    def execute_query(self, query: str, params: tuple = ()) -> List[sqlite3.Row]:
        """
        Executes a SELECT query and fetches all results.
        :param query: The SQL query string.
        :param params: A tuple of parameters to bind to the query.
        :return: A list of sqlite3.Row objects.
        """
        conn = self.connect()
        try:
            with conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                results = cursor.fetchall()
                logger.debug(f"Executed SELECT query: {query[:100]}... with params: {params}")
                return results
        except sqlite3.Error as e:
            logger.error(f"Failed to execute query '{query[:100]}...': {e}")
            raise DatabaseError(f"Query execution failed: {e}")

    def execute_script(self, script: str) -> None:
        """
        Executes a SQL script (multiple statements).
        :param script: The SQL script string.
        """
        conn = self.connect()
        try:
            with conn:
                conn.executescript(script)
                logger.info(f"Executed SQL script: {script[:100]}...")
        except sqlite3.Error as e:
            logger.error(f"Failed to execute script: {e}")
            raise DatabaseError(f"Script execution failed: {e}")

    def execute_update(self, query: str, params: tuple = ()) -> int:
        """
        Executes an INSERT, UPDATE, or DELETE query.
        :param query: The SQL query string.
        :param params: A tuple of parameters to bind to the query.
        :return: The number of rows affected.
        """
        conn = self.connect()
        try:
            with conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                rowcount = cursor.rowcount
                logger.debug(f"Executed UPDATE query: {query[:100]}... with params: {params}. Rows affected: {rowcount}")
                return rowcount
        except sqlite3.Error as e:
            logger.error(f"Failed to execute update query '{query[:100]}...': {e}")
            raise DatabaseError(f"Update query execution failed: {e}")

    def execute_insert_get_id(self, query: str, params: tuple = ()) -> int:
        """
        Executes an INSERT query and returns the new row ID.
        :param query: The SQL INSERT query string.
        :param params: A tuple of parameters to bind to the query.
        :return: The last inserted row ID.
        """
        conn = self.connect()
        try:
            with conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                last_id = cursor.lastrowid
                logger.debug(f"Executed INSERT query: {query[:100]}... with params: {params}. New ID: {last_id}")
                return last_id
        except sqlite3.Error as e:
            logger.error(f"Failed to execute insert query '{query[:100]}...': {e}")
            raise DatabaseError(f"Insert query execution failed: {e}")

    def __del__(self):
        """
        Destructor to ensure connection is closed.
        """
        self.disconnect()


# --- Utility Functions ---

def hash_password(password: str) -> str:
    """
    Hashes a password with a static salt using SHA-256.
    :param password: The plaintext password.
    :return: The hashed password as a hex digest.
    """
    if not password or len(password) < MIN_PASSWORD_LENGTH:
        raise ValidationError(f"Password must be at least {MIN_PASSWORD_LENGTH} characters long.")
    
    salted_password = password + PASSWORD_SALT
    return hashlib.sha256(salted_password.encode('utf-8')).hexdigest()

def validate_email(email: str) -> bool:
    """
    Validates an email address using a simple regex.
    :param email: The email string to validate.
    :return: True if valid, False otherwise.
    """
    regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(regex, email) is not None

def generate_api_key() -> str:
    """
    Generates a unique API key.
    :return: A new UUID4-based API key.
    """
    return str(uuid.uuid4())

def decimal_to_db(value: Decimal) -> str:
    """
    Converts a Decimal to a string for database storage.
    :param value: The Decimal value.
    :return: A string representation.
    """
    return str(value)

def db_to_decimal(value: Union[str, float, int]) -> Decimal:
    """
    Converts a database value (str, float, int) to a Decimal.
    :param value: The value from the database.
    :return: A Decimal object.
    """
    return Decimal(value)


# --- User Service Class ---

class UserService:
    """
    Manages user registration, authentication, and profile data.
    """

    def __init__(self, db_manager: DatabaseManager):
        """
        Initializes the user service.
        :param db_manager: An instance of DatabaseManager.
        """
        self.db = db_manager
        logger.info("UserService initialized.")

    def register_user(self, email: str, password: str, first_name: str, last_name: str) -> int:
        """
        Registers a new user in the database.
        :param email: User's email (must be unique).
        :param password: User's plaintext password.
        :param first_name: User's first name.
        :param last_name: User's last name.
        :return: The new user's ID.
        """
        if not validate_email(email):
            raise ValidationError("Invalid email format.")
        
        # Check for existing user
        if self.find_user_by_email(email):
            raise ValidationError("Email already registered.")
            
        hashed_pass = hash_password(password)
        created_at = datetime.datetime.utcnow().isoformat()
        
        sql = """
        INSERT INTO users (email, password_hash, first_name, last_name, role, created_at, last_login)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        
        try:
            user_id = self.db.execute_insert_get_id(sql, (
                email, hashed_pass, first_name, last_name, ROLE_CUSTOMER, created_at, created_at
            ))
            logger.info(f"New user registered with ID: {user_id} and email: {email}")
            
            # Create a default shipping address entry
            self.create_default_address(user_id)
            return user_id
        except DatabaseError as e:
            logger.error(f"Failed to register user {email}: {e}")
            raise

    def create_default_address(self, user_id: int):
        """
        Creates a blank, default address entry for a new user.
        :param user_id: The user's ID.
        """
        sql = """
        INSERT INTO addresses (user_id, is_default_shipping, is_default_billing)
        VALUES (?, 1, 1)
        """
        try:
            self.db.execute_insert_get_id(sql, (user_id,))
            logger.info(f"Created default address entry for user_id: {user_id}")
        except DatabaseError as e:
            logger.warning(f"Could not create default address for user_id {user_id}: {e}")
            # Non-fatal error, continue registration

    def authenticate_user(self, email: str, password: str) -> Dict[str, Any]:
        """
        Authenticates a user by email and password.
        :param email: User's email.
        :param password: User's plaintext password.
        :return: A dictionary of user data if successful.
        """
        user = self.find_user_by_email(email)
        if not user:
            logger.warning(f"Auth failed: No user found for email {email}")
            raise AuthenticationError("Invalid email or password.")
            
        hashed_pass = hash_password(password)
        if user['password_hash'] != hashed_pass:
            logger.warning(f"Auth failed: Incorrect password for email {email}")
            raise AuthenticationError("Invalid email or password.")
            
        # Update last_login timestamp
        self.update_last_login(user['user_id'])
        
        logger.info(f"User authenticated successfully: {email}")
        return dict(user)

    def find_user_by_email(self, email: str) -> Optional[sqlite3.Row]:
        """
        Finds a user by their email address.
        :param email: The email to search for.
        :return: A sqlite3.Row object or None if not found.
        """
        sql = "SELECT * FROM users WHERE email = ? LIMIT 1"
        results = self.db.execute_query(sql, (email,))
        return results[0] if results else None

    def find_user_by_id(self, user_id: int) -> Optional[sqlite3.Row]:
        """
        Finds a user by their ID.
        :param user_id: The ID to search for.
        :return: A sqlite3.Row object or None if not found.
        """
        sql = "SELECT * FROM users WHERE user_id = ? LIMIT 1"
        results = self.db.execute_query(sql, (user_id,))
        return results[0] if results else None

    def update_last_login(self, user_id: int):
        """
        Updates the last_login timestamp for a user.
        :param user_id: The user's ID.
        """
        now = datetime.datetime.utcnow().isoformat()
        sql = "UPDATE users SET last_login = ? WHERE user_id = ?"
        try:
            self.db.execute_update(sql, (now, user_id))
        except DatabaseError as e:
            logger.warning(f"Failed to update last_login for user_id {user_id}: {e}")
            # Non-fatal, don't block login

    def get_user_profile(self, user_id: int) -> Dict[str, Any]:
        """
        Retrieves a user's profile and their associated addresses.
        :param user_id: The user's ID.
        :return: A dictionary containing user info and a list of addresses.
        """
        user_sql = """
        SELECT user_id, email, first_name, last_name, role, created_at 
        FROM users 
        WHERE user_id = ?
        """
        address_sql = """
        SELECT address_id, street_line1, street_line2, city, state, postal_code, country, is_default_shipping, is_default_billing
        FROM addresses
        WHERE user_id = ?
        ORDER BY is_default_shipping DESC, address_id
        """
        
        user_results = self.db.execute_query(user_sql, (user_id,))
        if not user_results:
            raise ValidationError(f"User not found with ID: {user_id}")
            
        address_results = self.db.execute_query(address_sql, (user_id,))
        
        profile = {
            "user_info": dict(user_results[0]),
            "addresses": [dict(row) for row in address_results]
        }
        return profile

    def update_user_address(self, user_id: int, address_id: int, address_data: Dict[str, Any]) -> int:
        """
        Updates a specific address for a user.
        :param user_id: The user's ID (for verification).
        :param address_id: The address ID to update.
        :param address_data: A dict with new address fields.
        :return: Number of rows-affected.
        """
        allowed_fields = ['street_line1', 'street_line2', 'city', 'state', 'postal_code', 'country']
        updates = []
        params = []
        
        for key, value in address_data.items():
            if key in allowed_fields:
                updates.append(f"{key} = ?")
                params.append(value)
        
        if not updates:
            raise ValidationError("No valid address fields provided for update.")
            
        params.extend([user_id, address_id])
        
        sql = f"""
        UPDATE addresses
        SET {', '.join(updates)}
        WHERE user_id = ? AND address_id = ?
        """
        
        return self.db.execute_update(sql, tuple(params))

    def change_user_role(self, target_user_id: int, new_role: str, admin_user_id: int) -> bool:
        """
        Allows an admin to change another user's role.
        :param target_user_id: The user to be modified.
        :param new_role: The new role to assign.
        :param admin_user_id: The ID of the user performing the action (must be ADMIN).
        :return: True on success.
        """
        admin = self.find_user_by_id(admin_user_id)
        if not admin or admin['role'] != ROLE_ADMIN:
            logger.error(f"Permission denied: User {admin_user_id} attempted to change role for {target_user_id}")
            raise AuthenticationError("You do not have permission to perform this action.")
            
        if new_role not in VALID_ROLES:
            raise ValidationError(f"Invalid role: {new_role}")
            
        if target_user_id == admin_user_id:
            raise ValidationError("Admins cannot change their own role.")
            
        sql = "UPDATE users SET role = ? WHERE user_id = ?"
        rows_affected = self.db.execute_update(sql, (new_role, target_user_id))
        
        if rows_affected == 0:
            raise ValidationError(f"Target user {target_user_id} not found.")
            
        logger.info(f"Admin {admin_user_id} changed role for user {target_user_id} to {new_role}")
        return True


# --- Product & Inventory Service Class ---

class ProductService:
    """
    Manages product catalog, categories, reviews, and inventory levels.
    """

    def __init__(self, db_manager: DatabaseManager):
        """
        Initializes the product service.
        :param db_manager: An instance of DatabaseManager.
        """
        self.db = db_manager
        logger.info("ProductService initialized.")

    def add_product_category(self, name: str, description: str, parent_category_id: Optional[int] = None) -> int:
        """
        Adds a new product category.
        :param name: Category name.
        :param description: Category description.
        :param parent_category_id: Optional parent category for sub-categories.
        :return: The new category ID.
        """
        sql = """
        INSERT INTO categories (name, description, parent_category_id)
        VALUES (?, ?, ?)
        """
        return self.db.execute_insert_get_id(sql, (name, description, parent_category_id))

    def add_product(self, name: str, description: str, price: Decimal, category_id: int, stock_quantity: int, sku: str) -> int:
        """
        Adds a new product to the catalog.
        :param name: Product name.
        :param description: Product description.
        :param price: Product price (as Decimal).
        :param category_id: The category this product belongs to.
        :param stock_quantity: Initial stock quantity.
        :param sku: Stock Keeping Unit (must be unique).
        :return: The new product ID.
        """
        if price <= Decimal('0.00'):
            raise ValidationError("Price must be positive.")
        if stock_quantity < 0:
            raise ValidationError("Stock quantity cannot be negative.")
            
        # Check for unique SKU
        if self.get_product_by_sku(sku):
            raise ValidationError(f"SKU '{sku}' already exists.")
            
        db_price = decimal_to_db(price)
        
        product_sql = """
        INSERT INTO products (name, description, price, category_id, sku, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        inventory_sql = """
        INSERT INTO inventory (product_id, quantity, last_updated)
        VALUES (?, ?, ?)
        """
        
        conn = self.db.connect()
        try:
            with conn:
                cursor = conn.cursor()
                now = datetime.datetime.utcnow().isoformat()
                
                # Insert product
                cursor.execute(product_sql, (name, description, db_price, category_id, sku, now))
                product_id = cursor.lastrowid
                
                if not product_id:
                    raise DatabaseError("Failed to get lastrowid for new product.")
                
                # Insert inventory
                cursor.execute(inventory_sql, (product_id, stock_quantity, now))
                
                logger.info(f"Added new product {name} (ID: {product_id}, SKU: {sku}) with stock {stock_quantity}")
                return product_id
                
        except sqlite3.Error as e:
            logger.error(f"Failed to add product {name}: {e}")
            raise DatabaseError(f"Product creation failed: {e}")

    def get_product_by_id(self, product_id: int) -> Optional[Dict[str, Any]]:
        """
        Retrieves a single product and its inventory level by ID.
        :param product_id: The product ID.
        :return: A dictionary of product data or None.
        """
        sql = """
        SELECT 
            p.product_id, p.name, p.description, p.price, p.sku, p.created_at,
            c.name as category_name, c.category_id,
            i.quantity as stock_quantity
        FROM products p
        JOIN categories c ON p.category_id = c.category_id
        JOIN inventory i ON p.product_id = i.product_id
        WHERE p.product_id = ?
        """
        result = self.db.execute_query(sql, (product_id,))
        if not result:
            return None
            
        product = dict(result[0])
        product['price'] = db_to_decimal(product['price'])
        return product

    def get_product_by_sku(self, sku: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves a single product and its inventory level by SKU.
        :param sku: The product SKU.
        :return: A dictionary of product data or None.
        """
        sql = """
        SELECT 
            p.product_id, p.name, p.description, p.price, p.sku, p.created_at,
            c.name as category_name, c.category_id,
            i.quantity as stock_quantity
        FROM products p
        JOIN categories c ON p.category_id = c.category_id
        JOIN inventory i ON p.product_id = i.product_id
        WHERE p.sku = ?
        """
        result = self.db.execute_query(sql, (sku,))
        if not result:
            return None
            
        product = dict(result[0])
        product['price'] = db_to_decimal(product['price'])
        return product

    def update_product_stock(self, product_id: int, quantity_change: int) -> int:
        """
        Updates the stock for a product. Use negative for reduction.
        This function *must* be called within a transaction if part of an order.
        :param product_id: The product ID to update.
        :param quantity_change: The amount to add/subtract (e.g., -2 to subtract 2).
        :return: The new stock level.
        """
        now = datetime.datetime.utcnow().isoformat()
        
        # This SQL ensures we don't go below zero
        sql = """
        UPDATE inventory
        SET 
            quantity = quantity + ?,
            last_updated = ?
        WHERE product_id = ? AND (quantity + ?) >= 0
        """
        
        params = (quantity_change, now, product_id, quantity_change)
        rows_affected = self.db.execute_update(sql, params)
        
        if rows_affected == 0:
            # Check current stock to see why it failed
            current_stock = self.get_stock_level(product_id)
            if current_stock is None:
                raise InventoryError(f"Product ID {product_id} not found in inventory.")
            if current_stock + quantity_change < 0:
                logger.error(f"InventoryError: Tried to reduce stock for {product_id} by {abs(quantity_change)}, but only {current_stock} available.")
                raise InventoryError(f"Insufficient stock for product ID {product_id}. Available: {current_stock}, Requested: {abs(quantity_change)}")
            raise DatabaseError("Failed to update stock, unknown reason.")

        new_stock = self.get_stock_level(product_id)
        logger.info(f"Updated stock for product_id {product_id} by {quantity_change}. New stock: {new_stock}")
        return new_stock

    def get_stock_level(self, product_id: int) -> Optional[int]:
        """
        Gets the current stock level for a single product.
        :param product_id: The product ID.
        :return: The stock quantity, or None if product not found.
        """
        sql = "SELECT quantity FROM inventory WHERE product_id = ?"
        result = self.db.execute_query(sql, (product_id,))
        return result[0]['quantity'] if result else None

    def search_products(self, search_term: str, category_id: Optional[int] = None, min_price: Optional[Decimal] = None, max_price: Optional[Decimal] = None, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Performs a complex search for products.
        :param search_term: Text to search in name and description.
        :param category_id: Optional category to filter by.
        :param min_price: Optional minimum price.
        :param max_price: Optional maximum price.
        :param limit: Max number of results.
        :return: A list of product dictionaries.
        """
        
        # This query is complex and interdependent on multiple tables
        sql_base = """
        SELECT 
            p.product_id, p.name, p.price, p.sku,
            c.name as category_name,
            i.quantity as stock_quantity,
            AVG(r.rating) as average_rating
        FROM products p
        JOIN categories c ON p.category_id = c.category_id
        JOIN inventory i ON p.product_id = i.product_id
        LEFT JOIN reviews r ON p.product_id = r.product_id
        WHERE (p.name LIKE ? OR p.description LIKE ?)
        """
        
        params = [f'%{search_term}%', f'%{search_term}%']
        
        if category_id is not None:
            sql_base += " AND p.category_id = ? "
            params.append(category_id)
            
        if min_price is not None:
            sql_base += " AND p.price >= ? "
            params.append(decimal_to_db(min_price))
            
        if max_price is not None:
            sql_base += " AND p.price <= ? "
            params.append(decimal_to_db(max_price))
            
        sql_end = """
        GROUP BY p.product_id, p.name, p.price, p.sku, c.name, i.quantity
        ORDER BY average_rating DESC, p.name
        LIMIT ?
        """
        params.append(limit)
        
        full_sql = sql_base + sql_end
        
        results = self.db.execute_query(full_sql, tuple(params))
        
        # Convert price back to Decimal
        products = []
        for row in results:
            product = dict(row)
            product['price'] = db_to_decimal(product['price'])
            products.append(product)
            
        return products

    def add_product_review(self, user_id: int, product_id: int, rating: int, review_text: str) -> int:
        """
        Allows a user to add a review for a product.
        (Note: In a real system, we'd verify the user purchased the product first).
        :param user_id: The user leaving the review.
        :param product_id: The product being reviewed.
        :param rating: Rating from 1 to 5.
        :param review_text: The text content of the review.
        :return: The new review ID.
        """
        if not (1 <= rating <= 5):
            raise ValidationError("Rating must be between 1 and 5.")
            
        # Check if user has already reviewed this product
        existing_sql = "SELECT review_id FROM reviews WHERE user_id = ? AND product_id = ?"
        if self.db.execute_query(existing_sql, (user_id, product_id)):
            raise ValidationError("You have already reviewed this product.")
            
        sql = """
        INSERT INTO reviews (product_id, user_id, rating, review_text, created_at)
        VALUES (?, ?, ?, ?, ?)
        """
        now = datetime.datetime.utcnow().isoformat()
        review_id = self.db.execute_insert_get_id(sql, (product_id, user_id, rating, review_text, now))
        logger.info(f"User {user_id} added review {review_id} for product {product_id} with rating {rating}")
        
        # This is interdependent: we immediately update the product's average rating
        self.update_product_average_rating(product_id)
        
        return review_id

    def update_product_average_rating(self, product_id: int):
        """
        A helper function to recalculate and update a product's average rating.
        This is an example of an interdependent function call.
        :param product_id: The product to update.
        """
        
        # This SQL calculates the average from the reviews table
        avg_sql = """
        SELECT AVG(rating) as avg_rating
        FROM reviews
        WHERE product_id = ?
        """
        
        result = self.db.execute_query(avg_sql, (product_id,))
        avg_rating = result[0]['avg_rating'] if result and result[0]['avg_rating'] is not None else 0.0
        
        # This SQL updates the products table
        update_sql = """
        UPDATE products
        SET average_rating = ?
        WHERE product_id = ?
        """
        
        try:
            self.db.execute_update(update_sql, (round(avg_rating, 2), product_id))
            logger.info(f"Updated average rating for product {product_id} to {avg_rating:.2f}")
        except DatabaseError as e:
            logger.error(f"Failed to update average rating for product {product_id}: {e}")
            # Non-fatal, don't crash the review submission


# --- Order Service Class ---

class OrderService:
    """
    Manages the creation, processing, and fulfillment of orders.
    This class is highly interdependent on UserService and ProductService.
    """

    CartItem = namedtuple('CartItem', ['product_id', 'quantity'])

    def __init__(self, db_manager: DatabaseManager, user_service: UserService, product_service: ProductService):
        """
        Initializes the order service.
        :param db_manager: An instance of DatabaseManager.
        :param user_service: An instance of UserService.
        :param product_service: An instance of ProductService.
        """
        self.db = db_manager
        self.users = user_service
        self.products = product_service
        logger.info("OrderService initialized.")

    def create_order(self, user_id: int, cart: List[CartItem], shipping_address_id: int, billing_address_id: int, shipping_method: str = 'STANDARD') -> int:
        """
        Creates a new order from a user's cart.
        This is a complex, transactional operation.
        :param user_id: The user placing the order.
        :param cart: A list of CartItem tuples (product_id, quantity).
        :param shipping_address_id: The user's address ID for shipping.
        :param billing_address_id: The user's address ID for billing.
        :param shipping_method: 'STANDARD' or 'EXPRESS'.
        :return: The new order ID.
        """
        if not cart:
            raise OrderProcessingError("Cannot create an order with an empty cart.")
        if len(cart) > MAX_ORDER_ITEMS:
            raise OrderProcessingError(f"Cart exceeds maximum item count of {MAX_ORDER_ITEMS}.")
            
        # --- 1. Validation Phase ---
        
        # Validate user and addresses
        try:
            user_profile = self.users.get_user_profile(user_id)
            user_addresses = {addr['address_id'] for addr in user_profile['addresses']}
            if shipping_address_id not in user_addresses or billing_address_id not in user_addresses:
                raise OrderProcessingError("Invalid shipping or billing address ID for this user.")
        except ValidationError as e:
            raise OrderProcessingError(f"Invalid user: {e}")
            
        # --- 2. Pricing and Stock Check Phase ---
        
        # This block is highly interdependent on ProductService
        subtotal = Decimal('0.00')
        validated_items = []
        
        for item in cart:
            product = self.products.get_product_by_id(item.product_id)
            if not product:
                raise OrderProcessingError(f"Product ID {item.product_id} not found.")
                
            if item.quantity <= 0:
                raise OrderProcessingError(f"Invalid quantity ({item.quantity}) for product {item.product_id}.")
                
            current_stock = product['stock_quantity']
            if current_stock < item.quantity:
                logger.warning(f"Order failed: Insufficient stock for {product['sku']} (ID: {item.product_id}). Needed: {item.quantity}, Have: {current_stock}")
                raise InventoryError(f"Insufficient stock for '{product['name']}'. Requested: {item.quantity}, Available: {current_stock}")
                
            item_price = product['price']
            line_total = item_price * Decimal(item.quantity)
            subtotal += line_total
            
            validated_items.append({
                'product_id': item.product_id,
                'quantity': item.quantity,
                'price_at_purchase': item_price  # Lock in the price
            })
            
        # --- 3. Calculate Final Total ---
        
        shipping_fee = self.calculate_shipping(subtotal, shipping_method)
        total_amount = subtotal + shipping_fee
        
        # --- 4. Database Transaction Phase ---
        
        conn = self.db.connect()
        try:
            with conn:
                cursor = conn.cursor()
                now = datetime.datetime.utcnow().isoformat()
                
                # Step 4a: Create the main order record
                order_sql = """
                INSERT INTO orders (user_id, status, total_amount, subtotal, shipping_fee, shipping_address_id, billing_address_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """
                cursor.execute(order_sql, (
                    user_id, STATUS_PENDING, decimal_to_db(total_amount), decimal_to_db(subtotal),
                    decimal_to_db(shipping_fee), shipping_address_id, billing_address_id, now
                ))
                order_id = cursor.lastrowid
                if not order_id:
                    raise DatabaseError("Failed to create order record and get ID.")
                    
                logger.info(f"Created order {order_id} for user {user_id}. Status: PENDING.")
                
                # Step 4b: Insert all order items
                items_sql = """
                INSERT INTO order_items (order_id, product_id, quantity, price_at_purchase)
                VALUES (?, ?, ?, ?)
                """
                item_data_tuples = [
                    (order_id, item['product_id'], item['quantity'], decimal_to_db(item['price_at_purchase']))
                    for item in validated_items
                ]
                cursor.executemany(items_sql, item_data_tuples)
                
                # Step 4c: Update inventory (interdependent call)
                # We do this inside the same transaction
                # We need to use the *ProductService* but on the *current* connection/cursor
                # This is tricky. A better design would have ProductService accept a cursor.
                # For this sim, we'll just re-implement the stock update logic here.
                
                logger.info(f"Updating inventory for {len(validated_items)} items in order {order_id}")
                inventory_update_sql = """
                UPDATE inventory
                SET 
                    quantity = quantity - ?,
                    last_updated = ?
                WHERE product_id = ? AND (quantity - ?) >= 0
                """
                for item in validated_items:
                    rows_affected = cursor.execute(inventory_update_sql, (
                        item['quantity'], now, item['product_id'], item['quantity']
                    )).rowcount
                    
                    if rows_affected == 0:
                        # This should have been caught in phase 2, but this is a final check
                        # If it fails here, the transaction rolls back.
                        product = self.products.get_product_by_id(item['product_id'])
                        raise InventoryError(f"Failed to reserve stock for '{product['name']}'. Stock may have changed. Please try again.")

                # Step 4d: Add an entry to order_status_history
                history_sql = """
                INSERT INTO order_status_history (order_id, status, changed_at)
                VALUES (?, ?, ?)
                """
                cursor.execute(history_sql, (order_id, STATUS_PENDING, now))
            
            # Transaction commits here
            logger.info(f"Successfully created and reserved stock for order {order_id}. Total: {total_amount}")
            return order_id
            
        except (sqlite3.Error, InventoryError, DatabaseError) as e:
            logger.error(f"Failed to create order for user {user_id} due to: {e}. Rolling back transaction.")
            # Transaction automatically rolls back on exception
            if isinstance(e, InventoryError):
                raise  # Re-raise the specific error
            raise OrderProcessingError(f"Order creation failed due to a database error: {e}")

    def calculate_shipping(self, subtotal: Decimal, method: str) -> Decimal:
        """
        Calculates shipping fee based on subtotal and method.
        :param subtotal: The order subtotal.
        :param method: 'STANDARD' or 'EXPRESS'.
        :return: The shipping fee.
        """
        if subtotal >= FREE_SHIPPING_THRESHOLD and method == 'STANDARD':
            return Decimal('0.00')
            
        if method == 'EXPRESS':
            return SHIPPING_FEE_EXPRESS
            
        return SHIPPING_FEE_STANDARD

    def update_order_status(self, order_id: int, new_status: str, admin_user_id: Optional[int] = None) -> bool:
        """
        Updates an order's status.
        :param order_id: The order to update.
        :param new_status: The new status (e.g., PAID, SHIPPED).
        :param admin_user_id: Optional. If provided, checks for admin/support role.
        :return: True on success.
        """
        if new_status not in VALID_STATUSES:
            raise ValidationError(f"Invalid order status: {new_status}")
            
        if admin_user_id:
            admin = self.users.find_user_by_id(admin_user_id)
            if not admin or admin['role'] not in (ROLE_ADMIN, ROLE_SUPPORT):
                raise AuthenticationError("You do not have permission to update order status.")
        
        # Get current status
        current_status_res = self.db.execute_query("SELECT status FROM orders WHERE order_id = ?", (order_id,))
        if not current_status_res:
            raise OrderProcessingError(f"Order ID {order_id} not found.")
        current_status = current_status_res[0]['status']

        if current_status == new_status:
            return True # No change needed

        # Add state transition logic
        if current_status == STATUS_CANCELLED or current_status == STATUS_REFUNDED:
            raise OrderProcessingError(f"Cannot change status of a {current_status} order.")
        
        # --- Transaction to update status and log history ---
        conn = self.db.connect()
        try:
            with conn:
                now = datetime.datetime.utcnow().isoformat()
                
                # Step 1: Update the order
                order_update_sql = "UPDATE orders SET status = ? WHERE order_id = ?"
                conn.execute(order_update_sql, (new_status, order_id))
                
                # Step 2: Log the change
                history_sql = """
                INSERT INTO order_status_history (order_id, status, changed_at, changed_by_user_id)
                VALUES (?, ?, ?, ?)
                """
                conn.execute(history_sql, (order_id, new_status, now, admin_user_id))
                
                # Step 3: Interdependent action: Handle refunds
                if new_status == STATUS_CANCELLED or new_status == STATUS_REFUNDED:
                    # This function is interdependent with ProductService
                    self.restock_cancelled_order_items(order_id, conn)
            
            logger.info(f"Order {order_id} status updated to {new_status}" + (f" by user {admin_user_id}" if admin_user_id else ""))
            return True

        except (sqlite3.Error, InventoryError, DatabaseError) as e:
            logger.error(f"Failed to update status for order {order_id}: {e}. Rolling back.")
            raise OrderProcessingError(f"Order status update failed: {e}")

    def restock_cancelled_order_items(self, order_id: int, db_conn: sqlite3.Connection):
        """
        Helper function to restock items from a cancelled or refunded order.
        This MUST be called from within an existing database transaction.
        :param order_id: The order ID being cancelled.
        :param db_conn: The active database connection/transaction.
        """
        logger.warning(f"Restocking items for cancelled/refunded order {order_id}")
        
        # Get all items from the order
        items_sql = "SELECT product_id, quantity FROM order_items WHERE order_id = ?"
        try:
            items = db_conn.execute(items_sql, (order_id,)).fetchall()
            
            if not items:
                logger.error(f"No items found for order {order_id} during restock. This is unusual.")
                return

            inventory_update_sql = """
            UPDATE inventory
            SET 
                quantity = quantity + ?,
                last_updated = ?
            WHERE product_id = ?
            """
            now = datetime.datetime.utcnow().isoformat()
            
            for item in items:
                # Use the ProductService to update stock
                # This is complex. We're inside the OrderService transaction,
                # so we can't use self.products.update_product_stock directly
                # as it would start a *new* transaction.
                # We must execute the query on the provided connection.
                
                db_conn.execute(inventory_update_sql, (item['quantity'], now, item['product_id']))
                logger.info(f"Restocked {item['quantity']} of product {item['product_id']} from order {order_id}")

        except sqlite3.Error as e:
            logger.error(f"CRITICAL: Failed to restock items for order {order_id} during cancellation: {e}")
            # We raise this to roll back the *entire* status change
            raise InventoryError(f"Failed to restock items for order {order_id}: {e}")

    def get_order_details(self, order_id: int) -> Dict[str, Any]:
        """
        Retrieves complete details for a single order.
        This is a very complex, multi-join SQL query.
        :param order_id: The ID of the order to fetch.
        :return: A dictionary with order details, items, and history.
        """
        
        # Query 1: Get main order info and joined addresses
        order_sql = """
        SELECT
            o.order_id, o.user_id, o.status, o.total_amount, o.subtotal, o.shipping_fee, o.created_at,
            u.email as user_email,
            sa.street_line1 as ship_street1, sa.street_line2 as ship_street2, sa.city as ship_city, 
            sa.state as ship_state, sa.postal_code as ship_zip, sa.country as ship_country,
            ba.street_line1 as bill_street1, ba.street_line2 as bill_street2, ba.city as bill_city,
            ba.state as bill_state, ba.postal_code as bill_zip, ba.country as bill_country
        FROM orders o
        JOIN users u ON o.user_id = u.user_id
        JOIN addresses sa ON o.shipping_address_id = sa.address_id
        JOIN addresses ba ON o.billing_address_id = ba.address_id
        WHERE o.order_id = ?
        """
        
        # Query 2: Get all items for the order
        items_sql = """
        SELECT
            oi.product_id, oi.quantity, oi.price_at_purchase,
            p.name as product_name, p.sku
        FROM order_items oi
        JOIN products p ON oi.product_id = p.product_id
        WHERE oi.order_id = ?
        """
        
        # Query 3: Get order status history
        history_sql = """
        SELECT
            h.status, h.changed_at, h.changed_by_user_id,
            u.email as changed_by_email
        FROM order_status_history h
        LEFT JOIN users u ON h.changed_by_user_id = u.user_id
        WHERE h.order_id = ?
        ORDER BY h.changed_at ASC
        """
        
        order_res = self.db.execute_query(order_sql, (order_id,))
        if not order_res:
            raise OrderProcessingError(f"Order ID {order_id} not found.")
            
        items_res = self.db.execute_query(items_sql, (order_id,))
        history_res = self.db.execute_query(history_sql, (order_id,))
        
        # Assemble the final nested dictionary
        order_data = dict(order_res[0])
        
        # Convert Decimals
        order_data['total_amount'] = db_to_decimal(order_data['total_amount'])
        order_data['subtotal'] = db_to_decimal(order_data['subtotal'])
        order_data['shipping_fee'] = db_to_decimal(order_data['shipping_fee'])
        
        # Format items
        order_items = []
        for row in items_res:
            item = dict(row)
            item['price_at_purchase'] = db_to_decimal(item['price_at_purchase'])
            order_items.append(item)
            
        order_data['items'] = order_items
        order_data['status_history'] = [dict(row) for row in history_res]
        
        return order_data


# --- Reporting Service Class ---

class ReportingService:
    """
    Generates complex, read-only reports for business analytics.
    These queries are often the most complex and interdependent.
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initializes the reporting service.
        :param db_manager: An instance of DatabaseManager.
        """
        self.db = db_manager
        logger.info("ReportingService initialized.")

    def get_sales_summary_by_date_range(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Generates a sales summary (total revenue, orders, items) for a date range.
        :param start_date: ISO 8601 date string (e.g., '2023-01-01')
        :param end_date: ISO 8601 date string (e.g., '2023-01-31')
        :return: A dictionary containing the summary.
        """
        
        # This query joins orders and order_items and filters by date
        # It excludes cancelled/refunded orders from revenue.
        sql = """
        SELECT
            COUNT(DISTINCT o.order_id) as total_orders,
            SUM(o.total_amount) as total_revenue,
            SUM(oi.quantity) as total_items_sold,
            AVG(o.total_amount) as average_order_value
        FROM orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        WHERE o.created_at >= ? 
          AND o.created_at <= ?
          AND o.status NOT IN (?, ?)
        """
        
        # Add time to end_date to make it inclusive
        end_date_inclusive = end_date + 'T23:59:59Z'
        start_date_iso = start_date + 'T00:00:00Z'
        
        params = (start_date_iso, end_date_inclusive, STATUS_CANCELLED, STATUS_REFUNDED)
        
        result = self.db.execute_query(sql, params)
        summary = dict(result[0])
        
        # Convert Decimals
        summary['total_revenue'] = db_to_decimal(summary['total_revenue'] or '0.00')
        summary['average_order_value'] = db_to_decimal(summary['average_order_value'] or '0.00')
        summary['total_orders'] = summary['total_orders'] or 0
        summary['total_items_sold'] = summary['total_items_sold'] or 0
        
        return summary

    def get_top_selling_products(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Gets the top-selling products by quantity sold.
        :param limit: Number of products to return.
        :return: A list of product summary dictionaries.
        """
        
        # This query joins products, order_items, and orders
        sql = """
        SELECT
            p.product_id,
            p.name,
            p.sku,
            SUM(oi.quantity) as total_quantity_sold,
            SUM(oi.quantity * oi.price_at_purchase) as total_revenue
        FROM products p
        JOIN order_items oi ON p.product_id = oi.product_id
        JOIN orders o ON oi.order_id = o.order_id
        WHERE o.status NOT IN (?, ?)
        GROUP BY p.product_id, p.name, p.sku
        ORDER BY total_quantity_sold DESC
        LIMIT ?
        """
        
        params = (STATUS_CANCELLED, STATUS_REFUNDED, limit)
        results = self.db.execute_query(sql, params)
        
        top_products = []
        for row in results:
            product = dict(row)
            product['total_revenue'] = db_to_decimal(product['total_revenue'])
            top_products.append(product)
            
        return top_products

    def get_customer_lifetime_value_report(self, limit: int = 25) -> List[Dict[str, Any]]:
        """
        Generates a report of top customers by total amount spent (LTV).
        :param limit: Number of customers to return.
        :return: A list of customer LTV summaries.
        """
        
        # This query joins users and orders
        sql = """
        SELECT
            u.user_id,
            u.email,
            u.first_name,
            u.last_name,
            COUNT(o.order_id) as total_orders,
            SUM(o.total_amount) as lifetime_value
        FROM users u
        JOIN orders o ON u.user_id = o.user_id
        WHERE o.status NOT IN (?, ?)
        GROUP BY u.user_id, u.email, u.first_name, u.last_name
        ORDER BY lifetime_value DESC
        LIMIT ?
        """
        
        params = (STATUS_CANCELLED, STATUS_REFUNDED, limit)
        results = self.db.execute_query(sql, params)
        
        top_customers = []
        for row in results:
            customer = dict(row)
            customer['lifetime_value'] = db_to_decimal(customer['lifetime_value'])
            top_customers.append(customer)
            
        return top_customers

    def get_inventory_stock_report(self, low_stock_threshold: int = 10) -> Dict[str, List[Dict[str, Any]]]:
        """
        Generates a report of all inventory, highlighting low-stock items.
        :param low_stock_threshold: The quantity to consider 'low stock'.
        :return: A dictionary with 'low_stock' and 'in_stock' lists.
        """
        
        sql = """
        SELECT
            p.product_id,
            p.name,
            p.sku,
            c.name as category_name,
            i.quantity,
            i.last_updated
        FROM products p
        JOIN inventory i ON p.product_id = i.product_id
        JOIN categories c ON p.category_id = c.category_id
        ORDER BY i.quantity ASC
        """
        
        results = self.db.execute_query(sql)
        
        report = {
            'low_stock': [],
            'in_stock': []
        }
        
        for row in results:
            item = dict(row)
            if item['quantity'] <= low_stock_threshold:
                report['low_stock'].append(item)
            else:
                report['in_stock'].append(item)
                
        return report


# --- Main Application Setup & Schema Definition ---

def setup_database_schema(db_manager: DatabaseManager):
    """
    Defines and executes the entire database schema (DDL).
    This function demonstrates the complex interdependencies of the tables
    with foreign keys.
    """
    
    schema_script = """
    PRAGMA foreign_keys = ON;

    -- Users Table: Stores customer and admin information
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'CUSTOMER' CHECK(role IN ('CUSTOMER', 'ADMIN', 'SUPPORT')),
        created_at TEXT NOT NULL,
        last_login TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_users_email ON users (email);

    -- Addresses Table: Stores multiple addresses per user
    CREATE TABLE IF NOT EXISTS addresses (
        address_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        street_line1 TEXT,
        street_line2 TEXT,
        city TEXT,
        state TEXT,
        postal_code TEXT,
        country TEXT,
        is_default_shipping INTEGER DEFAULT 0,
        is_default_billing INTEGER DEFAULT 0,
        FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_addresses_user_id ON addresses (user_id);

    -- Categories Table: For organizing products
    CREATE TABLE IF NOT EXISTS categories (
        category_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        description TEXT,
        parent_category_id INTEGER,
        FOREIGN KEY (parent_category_id) REFERENCES categories (category_id) ON DELETE SET NULL
    );
    CREATE INDEX IF NOT EXISTS idx_categories_name ON categories (name);

    -- Products Table: The main product catalog
    CREATE TABLE IF NOT EXISTS products (
        product_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        description TEXT,
        price TEXT NOT NULL, -- Stored as string to avoid precision loss
        category_id INTEGER NOT NULL,
        sku TEXT UNIQUE NOT NULL,
        average_rating REAL DEFAULT 0.0,
        created_at TEXT NOT NULL,
        FOREIGN KEY (category_id) REFERENCES categories (category_id) ON DELETE RESTRICT
    );
    CREATE INDEX IF NOT EXISTS idx_products_sku ON products (sku);
    CREATE INDEX IF NOT EXISTS idx_products_name ON products (name);
    CREATE INDEX IF NOT EXISTS idx_products_category_id ON products (category_id);

    -- Inventory Table: Tracks stock for each product
    CREATE TABLE IF NOT EXISTS inventory (
        product_id INTEGER PRIMARY KEY,
        quantity INTEGER NOT NULL DEFAULT 0 CHECK(quantity >= 0),
        last_updated TEXT NOT NULL,
        FOREIGN KEY (product_id) REFERENCES products (product_id) ON DELETE CASCADE
    );

    -- Orders Table: The main record for each order
    CREATE TABLE IF NOT EXISTS orders (
        order_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        status TEXT NOT NULL CHECK(status IN ('PENDING', 'PAID', 'SHIPPED', 'DELIVERED', 'CANCELLED', 'REFUNDED')),
        total_amount TEXT NOT NULL,
        subtotal TEXT NOT NULL,
        shipping_fee TEXT NOT NULL,
        shipping_address_id INTEGER NOT NULL,
        billing_address_id INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE RESTRICT,
        FOREIGN KEY (shipping_address_id) REFERENCES addresses (address_id) ON DELETE RESTRICT,
        FOREIGN KEY (billing_address_id) REFERENCES addresses (address_id) ON DELETE RESTRICT
    );
    CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders (user_id);
    CREATE INDEX IF NOT EXISTS idx_orders_status ON orders (status);
    CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders (created_at);

    -- Order Items Table: Links products to orders (line items)
    CREATE TABLE IF NOT EXISTS order_items (
        order_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id INTEGER NOT NULL,
        product_id INTEGER NOT NULL,
        quantity INTEGER NOT NULL CHECK(quantity > 0),
        price_at_purchase TEXT NOT NULL, -- Price at the time of order
        FOREIGN KEY (order_id) REFERENCES orders (order_id) ON DELETE CASCADE,
        FOREIGN KEY (product_id) REFERENCES products (product_id) ON DELETE RESTRICT
    );
    CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items (order_id);
    CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items (product_id);

    -- Reviews Table: User reviews for products
    CREATE TABLE IF NOT EXISTS reviews (
        review_id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        rating INTEGER NOT NULL CHECK(rating >= 1 AND rating <= 5),
        review_text TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY (product_id) REFERENCES products (product_id) ON DELETE CASCADE,
        FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE,
        UNIQUE (product_id, user_id) -- One review per user per product
    );
    CREATE INDEX IF NOT EXISTS idx_reviews_product_id ON reviews (product_id);

    -- Order Status History Table: Logs all status changes for an order
    CREATE TABLE IF NOT EXISTS order_status_history (
        history_id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id INTEGER NOT NULL,
        status TEXT NOT NULL,
        changed_at TEXT NOT NULL,
        changed_by_user_id INTEGER, -- NULL if changed by system
        FOREIGN KEY (order_id) REFERENCES orders (order_id) ON DELETE CASCADE,
        FOREIGN KEY (changed_by_user_id) REFERENCES users (user_id) ON DELETE SET NULL
    );
    CREATE INDEX IF NOT EXISTS idx_order_status_history_order_id ON order_status_history (order_id);
    """
    
    try:
        db_manager.execute_script(schema_script)
        logger.info("Database schema verified/created successfully.")
    except DatabaseError as e:
        logger.critical(f"FATAL: Could not initialize database schema: {e}")
        raise

# --- Main Execution Block (Example Usage) ---

def main():
    """
    Main function to initialize services and demonstrate usage.
    """
    logger.info("--- Starting E-Commerce Backend Service (Demo) ---")
    
    # Initialize all services with the same DB manager
    db_manager = DatabaseManager(DB_NAME)
    
    # Create schema if it doesn't exist
    setup_database_schema(db_manager)
    
    user_service = UserService(db_manager)
    product_service = ProductService(db_manager)
    order_service = OrderService(db_manager, user_service, product_service)
    reporting_service = ReportingService(db_manager)
    
    logger.info("All services initialized.")
    
    try:
        # --- Demo 1: User Registration ---
        logger.info("Demo 1: Registering users...")
        try:
            admin_id = user_service.register_user("admin@example.com", "AdminPass123", "Admin", "User")
            user_service.change_user_role(admin_id, ROLE_ADMIN, admin_id) # Fails (can't change own role)
        except ValidationError as e:
            logger.warning(f"Caught expected error: {e}")
            # We need an admin, let's just update the DB directly for the demo
            db_manager.execute_update("UPDATE users SET role = ? WHERE email = ?", (ROLE_ADMIN, "admin@example.com"))
            admin_user = user_service.find_user_by_email("admin@example.com")
            admin_id = admin_user['user_id']
            logger.info(f"Admin user created/promoted with ID: {admin_id}")

        user_id_1 = user_service.register_user("alice@example.com", "AlicePass123", "Alice", "Smith")
        user_id_2 = user_service.register_user("bob@example.com", "BobPass123", "Bob", "Johnson")
        
        # --- Demo 2: Admin creates categories and products ---
        logger.info("Demo 2: Creating categories and products...")
        cat_id_electronics = product_service.add_product_category("Electronics", "Gadgets and devices")
        cat_id_books = product_service.add_product_category("Books", "Paperback and hardcover books")
        
        prod_id_laptop = product_service.add_product("Pro Laptop 15\"", "A powerful laptop", Decimal("1299.99"), cat_id_electronics, 50, "SKU-LAP-001")
        prod_id_phone = product_service.add_product("Smart Phone X", "The latest smartphone", Decimal("799.00"), cat_id_electronics, 150, "SKU-PHN-002")
        prod_id_book = product_service.add_product("Database Design", "A book on SQL", Decimal("49.95"), cat_id_books, 200, "SKU-BOK-003")

        # --- Demo 3: Users update profile and add reviews ---
        logger.info("Demo 3: Updating profiles and adding reviews...")
        user_service.update_user_address(user_id_1, 1, {
            'street_line1': '123 Main St',
            'city': 'Anytown',
            'state': 'CA',
            'postal_code': '12345',
            'country': 'USA'
        })
        profile = user_service.get_user_profile(user_id_1)
        alice_addr_id = profile['addresses'][0]['address_id']
        
        product_service.add_product_review(user_id_1, prod_id_laptop, 5, "Amazing laptop! Super fast.")
        product_service.add_product_review(user_id_2, prod_id_laptop, 4, "Pretty good, but battery could be better.")
        
        laptop_details = product_service.get_product_by_id(prod_id_laptop)
        logger.info(f"Laptop average rating is now: {laptop_details['average_rating']}")

        # --- Demo 4: User 1 creates an order ---
        logger.info("Demo 4: Creating an order...")
        cart = [
            OrderService.CartItem(product_id=prod_id_laptop, quantity=1),
            OrderService.CartItem(product_id=prod_id_book, quantity=2)
        ]
        
        try:
            order_id_1 = order_service.create_order(user_id_1, cart, alice_addr_id, alice_addr_id, 'STANDARD')
            logger.info(f"Successfully created order {order_id_1}")
        except (OrderProcessingError, InventoryError) as e:
            logger.error(f"Failed to create order: {e}")

        # --- Demo 5: Admin processes the order ---
        logger.info("Demo 5: Processing the order...")
        if 'order_id_1' in locals():
            order_service.update_order_status(order_id_1, STATUS_PAID, admin_id)
            order_service.update_order_status(order_id_1, STATUS_SHIPPED, admin_id)
            order_service.update_order_status(order_id_1, STATUS_DELIVERED, admin_id)

        # --- Demo 6: Create a failing order (insufficient stock) ---
        logger.info("Demo 6: Creating a failing order (insufficient stock)...")
        cart_fail = [OrderService.CartItem(product_id=prod_id_laptop, quantity=1000)] # We only have < 50
        try:
            order_service.create_order(user_id_2, cart_fail, 2, 2, 'STANDARD') # Assuming user 2 has address ID 2
        except InventoryError as e:
            logger.warning(f"Caught expected inventory error: {e}")
        
        # --- Demo 7: Cancel an order and restock ---
        logger.info("Demo 7: Cancelling an order and restocking...")
        cart_cancel = [OrderService.CartItem(product_id=prod_id_phone, quantity=1)]
        try:
            order_id_2 = order_service.create_order(user_id_2, cart_cancel, 2, 2, 'STANDARD')
            logger.info(f"Created order {order_id_2} to be cancelled.")
            phone_stock_before = product_service.get_stock_level(prod_id_phone)
            logger.info(f"Stock of phone before cancel: {phone_stock_before}")
            
            order_service.update_order_status(order_id_2, STATUS_CANCELLED, admin_id)
            
            phone_stock_after = product_service.get_stock_level(prod_id_phone)
            logger.info(f"Stock of phone after cancel: {phone_stock_after}")
            assert phone_stock_after == phone_stock_before + 1
            logger.info("Restock successful.")
        except (OrderProcessingError, InventoryError) as e:
            logger.error(f"Failed during cancellation demo: {e}")


        # --- Demo 8: Run Reports ---
        logger.info("Demo 8: Running reports...")
        
        sales_summary = reporting_service.get_sales_summary_by_date_range('2020-01-01', datetime.datetime.utcnow().strftime('%Y-%m-%d'))
        logger.info(f"Sales Summary: {json.dumps(sales_summary, default=str, indent=2)}")
        
        top_products = reporting_service.get_top_selling_products()
        logger.info(f"Top Products: {json.dumps(top_products, default=str, indent=2)}")
        
        top_customers = reporting_service.get_customer_lifetime_value_report()
        logger.info(f"Top Customers: {json.dumps(top_customers, default=str, indent=2)}")
        
        inventory_report = reporting_service.get_inventory_stock_report()
        logger.info(f"Low Stock Items: {json.dumps(inventory_report['low_stock'], default=str, indent=2)}")

        # --- Demo 9: Get complex order details ---
        logger.info("Demo 9: Getting full order details...")
        if 'order_id_1' in locals():
            full_details = order_service.get_order_details(order_id_1)
            logger.info(f"Full details for order {order_id_1}: {json.dumps(full_details, default=str, indent=2)}")

    except Exception as e:
        logger.critical(f"An unhandled exception occurred during demo: {e}", exc_info=True)
    
    finally:
        db_manager.disconnect()
        logger.info("--- E-Commerce Backend Service (Demo) Finished ---")

if __name__ == "__main__":
    main()

# End of file. Approx 1150 lines.#!/usr/bin/env python3

"""
ecommerce_backend.py

A comprehensive backend service module for a large-scale e-commerce platform.
This file includes interdependent services for managing users, products, orders,
and generating complex reports. All database interactions are represented
as string-based SQL queries for demonstration.

Author: AI (Gemini)
Version: 1.0.0
"""

# Standard Library Imports
import sqlite3
import logging
import datetime
import hashlib
import json
import uuid
import re
from decimal import Decimal, getcontext
from typing import List, Dict, Any, Optional, Tuple, Union
from collections import namedtuple

# Set precision for Decimal operations
getcontext().prec = 10

# --- Configuration & Constants ---

# Database Configuration
DB_NAME = 'ecommerce_main.db'

# Logging Configuration
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    filename='ecommerce_service.log')
logger = logging.getLogger(__name__)

# Application Constants
DEFAULT_CURRENCY = 'USD'
PASSWORD_SALT = 'a_very_secret_ecommerce_salt_string'
MIN_PASSWORD_LENGTH = 8
MAX_ORDER_ITEMS = 50
SHIPPING_FEE_STANDARD = Decimal('5.99')
SHIPPING_FEE_EXPRESS = Decimal('15.99')
FREE_SHIPPING_THRESHOLD = Decimal('100.00')

# Order Statuses (simulating an Enum)
STATUS_PENDING = 'PENDING'
STATUS_PAID = 'PAID'
STATUS_SHIPPED = 'SHIPPED'
STATUS_DELIVERED = 'DELIVERED'
STATUS_CANCELLED = 'CANCELLED'
STATUS_REFUNDED = 'REFUNDED'
VALID_STATUSES = {STATUS_PENDING, STATUS_PAID, STATUS_SHIPPED, STATUS_DELIVERED, STATUS_CANCELLED, STATUS_REFUNDED}

# User Roles
ROLE_CUSTOMER = 'CUSTOMER'
ROLE_ADMIN = 'ADMIN'
ROLE_SUPPORT = 'SUPPORT'
VALID_ROLES = {ROLE_CUSTOMER, ROLE_ADMIN, ROLE_SUPPORT}

# Custom Exception Classes
class DatabaseError(Exception):
    """Exception raised for database-related errors."""
    pass

class ValidationError(ValueError):
    """Exception raised for data validation failures."""
    pass

class AuthenticationError(SecurityException):
    """Exception raised for auth failures."""
    pass

class OrderProcessingError(Exception):
    """Exception raised during order processing."""
    pass

class InventoryError(Exception):
    """Exception raised for inventory-related issues."""
    pass


# --- Database Manager Class ---

class DatabaseManager:
    """
    Handles all low-level database connections and query executions.
    This class is instantiated and used by all other services.
    """

    def __init__(self, db_path: str):
        """
        Initializes the database manager.
        :param db_path: Filesystem path to the SQLite database.
        """
        self.db_path = db_path
        self.connection = None
        logger.info(f"DatabaseManager initialized for: {db_path}")

    def connect(self) -> sqlite3.Connection:
        """
        Establishes and returns a database connection.
        """
        try:
            if not self.connection or self.connection.total_changes == -1:
                self.connection = sqlite3.connect(self.db_path)
                self.connection.row_factory = sqlite3.Row
                self.connection.execute("PRAGMA foreign_keys = ON;")
                logger.info("New database connection established.")
            return self.connection
        except sqlite3.Error as e:
            logger.error(f"Failed to connect to database at {self.db_path}: {e}")
            raise DatabaseError(f"Database connection failure: {e}")

    def disconnect(self):
        """
        Closes the database connection if it exists.
        """
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Database connection closed.")

    def execute_query(self, query: str, params: tuple = ()) -> List[sqlite3.Row]:
        """
        Executes a SELECT query and fetches all results.
        :param query: The SQL query string.
        :param params: A tuple of parameters to bind to the query.
        :return: A list of sqlite3.Row objects.
        """
        conn = self.connect()
        try:
            with conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                results = cursor.fetchall()
                logger.debug(f"Executed SELECT query: {query[:100]}... with params: {params}")
                return results
        except sqlite3.Error as e:
            logger.error(f"Failed to execute query '{query[:100]}...': {e}")
            raise DatabaseError(f"Query execution failed: {e}")

    def execute_script(self, script: str) -> None:
        """
        Executes a SQL script (multiple statements).
        :param script: The SQL script string.
        """
        conn = self.connect()
        try:
            with conn:
                conn.executescript(script)
                logger.info(f"Executed SQL script: {script[:100]}...")
        except sqlite3.Error as e:
            logger.error(f"Failed to execute script: {e}")
            raise DatabaseError(f"Script execution failed: {e}")

    def execute_update(self, query: str, params: tuple = ()) -> int:
        """
        Executes an INSERT, UPDATE, or DELETE query.
        :param query: The SQL query string.
        :param params: A tuple of parameters to bind to the query.
        :return: The number of rows affected.
        """
        conn = self.connect()
        try:
            with conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                rowcount = cursor.rowcount
                logger.debug(f"Executed UPDATE query: {query[:100]}... with params: {params}. Rows affected: {rowcount}")
                return rowcount
        except sqlite3.Error as e:
            logger.error(f"Failed to execute update query '{query[:100]}...': {e}")
            raise DatabaseError(f"Update query execution failed: {e}")

    def execute_insert_get_id(self, query: str, params: tuple = ()) -> int:
        """
        Executes an INSERT query and returns the new row ID.
        :param query: The SQL INSERT query string.
        :param params: A tuple of parameters to bind to the query.
        :return: The last inserted row ID.
        """
        conn = self.connect()
        try:
            with conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                last_id = cursor.lastrowid
                logger.debug(f"Executed INSERT query: {query[:100]}... with params: {params}. New ID: {last_id}")
                return last_id
        except sqlite3.Error as e:
            logger.error(f"Failed to execute insert query '{query[:100]}...': {e}")
            raise DatabaseError(f"Insert query execution failed: {e}")

    def __del__(self):
        """
        Destructor to ensure connection is closed.
        """
        self.disconnect()


# --- Utility Functions ---

def hash_password(password: str) -> str:
    """
    Hashes a password with a static salt using SHA-256.
    :param password: The plaintext password.
    :return: The hashed password as a hex digest.
    """
    if not password or len(password) < MIN_PASSWORD_LENGTH:
        raise ValidationError(f"Password must be at least {MIN_PASSWORD_LENGTH} characters long.")
    
    salted_password = password + PASSWORD_SALT
    return hashlib.sha256(salted_password.encode('utf-8')).hexdigest()

def validate_email(email: str) -> bool:
    """
    Validates an email address using a simple regex.
    :param email: The email string to validate.
    :return: True if valid, False otherwise.
    """
    regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(regex, email) is not None

def generate_api_key() -> str:
    """
    Generates a unique API key.
    :return: A new UUID4-based API key.
    """
    return str(uuid.uuid4())

def decimal_to_db(value: Decimal) -> str:
    """
    Converts a Decimal to a string for database storage.
    :param value: The Decimal value.
    :return: A string representation.
    """
    return str(value)

def db_to_decimal(value: Union[str, float, int]) -> Decimal:
    """
    Converts a database value (str, float, int) to a Decimal.
    :param value: The value from the database.
    :return: A Decimal object.
    """
    return Decimal(value)


# --- User Service Class ---

class UserService:
    """
    Manages user registration, authentication, and profile data.
    """

    def __init__(self, db_manager: DatabaseManager):
        """
        Initializes the user service.
        :param db_manager: An instance of DatabaseManager.
        """
        self.db = db_manager
        logger.info("UserService initialized.")

    def register_user(self, email: str, password: str, first_name: str, last_name: str) -> int:
        """
        Registers a new user in the database.
        :param email: User's email (must be unique).
        :param password: User's plaintext password.
        :param first_name: User's first name.
        :param last_name: User's last name.
        :return: The new user's ID.
        """
        if not validate_email(email):
            raise ValidationError("Invalid email format.")
        
        # Check for existing user
        if self.find_user_by_email(email):
            raise ValidationError("Email already registered.")
            
        hashed_pass = hash_password(password)
        created_at = datetime.datetime.utcnow().isoformat()
        
        sql = """
        INSERT INTO users (email, password_hash, first_name, last_name, role, created_at, last_login)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        
        try:
            user_id = self.db.execute_insert_get_id(sql, (
                email, hashed_pass, first_name, last_name, ROLE_CUSTOMER, created_at, created_at
            ))
            logger.info(f"New user registered with ID: {user_id} and email: {email}")
            
            # Create a default shipping address entry
            self.create_default_address(user_id)
            return user_id
        except DatabaseError as e:
            logger.error(f"Failed to register user {email}: {e}")
            raise

    def create_default_address(self, user_id: int):
        """
        Creates a blank, default address entry for a new user.
        :param user_id: The user's ID.
        """
        sql = """
        INSERT INTO addresses (user_id, is_default_shipping, is_default_billing)
        VALUES (?, 1, 1)
        """
        try:
            self.db.execute_insert_get_id(sql, (user_id,))
            logger.info(f"Created default address entry for user_id: {user_id}")
        except DatabaseError as e:
            logger.warning(f"Could not create default address for user_id {user_id}: {e}")
            # Non-fatal error, continue registration

    def authenticate_user(self, email: str, password: str) -> Dict[str, Any]:
        """
        Authenticates a user by email and password.
        :param email: User's email.
        :param password: User's plaintext password.
        :return: A dictionary of user data if successful.
        """
        user = self.find_user_by_email(email)
        if not user:
            logger.warning(f"Auth failed: No user found for email {email}")
            raise AuthenticationError("Invalid email or password.")
            
        hashed_pass = hash_password(password)
        if user['password_hash'] != hashed_pass:
            logger.warning(f"Auth failed: Incorrect password for email {email}")
            raise AuthenticationError("Invalid email or password.")
            
        # Update last_login timestamp
        self.update_last_login(user['user_id'])
        
        logger.info(f"User authenticated successfully: {email}")
        return dict(user)

    def find_user_by_email(self, email: str) -> Optional[sqlite3.Row]:
        """
        Finds a user by their email address.
        :param email: The email to search for.
        :return: A sqlite3.Row object or None if not found.
        """
        sql = "SELECT * FROM users WHERE email = ? LIMIT 1"
        results = self.db.execute_query(sql, (email,))
        return results[0] if results else None

    def find_user_by_id(self, user_id: int) -> Optional[sqlite3.Row]:
        """
        Finds a user by their ID.
        :param user_id: The ID to search for.
        :return: A sqlite3.Row object or None if not found.
        """
        sql = "SELECT * FROM users WHERE user_id = ? LIMIT 1"
        results = self.db.execute_query(sql, (user_id,))
        return results[0] if results else None

    def update_last_login(self, user_id: int):
        """
        Updates the last_login timestamp for a user.
        :param user_id: The user's ID.
        """
        now = datetime.datetime.utcnow().isoformat()
        sql = "UPDATE users SET last_login = ? WHERE user_id = ?"
        try:
            self.db.execute_update(sql, (now, user_id))
        except DatabaseError as e:
            logger.warning(f"Failed to update last_login for user_id {user_id}: {e}")
            # Non-fatal, don't block login

    def get_user_profile(self, user_id: int) -> Dict[str, Any]:
        """
        Retrieves a user's profile and their associated addresses.
        :param user_id: The user's ID.
        :return: A dictionary containing user info and a list of addresses.
        """
        user_sql = """
        SELECT user_id, email, first_name, last_name, role, created_at 
        FROM users 
        WHERE user_id = ?
        """
        address_sql = """
        SELECT address_id, street_line1, street_line2, city, state, postal_code, country, is_default_shipping, is_default_billing
        FROM addresses
        WHERE user_id = ?
        ORDER BY is_default_shipping DESC, address_id
        """
        
        user_results = self.db.execute_query(user_sql, (user_id,))
        if not user_results:
            raise ValidationError(f"User not found with ID: {user_id}")
            
        address_results = self.db.execute_query(address_sql, (user_id,))
        
        profile = {
            "user_info": dict(user_results[0]),
            "addresses": [dict(row) for row in address_results]
        }
        return profile

    def update_user_address(self, user_id: int, address_id: int, address_data: Dict[str, Any]) -> int:
        """
        Updates a specific address for a user.
        :param user_id: The user's ID (for verification).
        :param address_id: The address ID to update.
        :param address_data: A dict with new address fields.
        :return: Number of rows-affected.
        """
        allowed_fields = ['street_line1', 'street_line2', 'city', 'state', 'postal_code', 'country']
        updates = []
        params = []
        
        for key, value in address_data.items():
            if key in allowed_fields:
                updates.append(f"{key} = ?")
                params.append(value)
        
        if not updates:
            raise ValidationError("No valid address fields provided for update.")
            
        params.extend([user_id, address_id])
        
        sql = f"""
        UPDATE addresses
        SET {', '.join(updates)}
        WHERE user_id = ? AND address_id = ?
        """
        
        return self.db.execute_update(sql, tuple(params))

    def change_user_role(self, target_user_id: int, new_role: str, admin_user_id: int) -> bool:
        """
        Allows an admin to change another user's role.
        :param target_user_id: The user to be modified.
        :param new_role: The new role to assign.
        :param admin_user_id: The ID of the user performing the action (must be ADMIN).
        :return: True on success.
        """
        admin = self.find_user_by_id(admin_user_id)
        if not admin or admin['role'] != ROLE_ADMIN:
            logger.error(f"Permission denied: User {admin_user_id} attempted to change role for {target_user_id}")
            raise AuthenticationError("You do not have permission to perform this action.")
            
        if new_role not in VALID_ROLES:
            raise ValidationError(f"Invalid role: {new_role}")
            
        if target_user_id == admin_user_id:
            raise ValidationError("Admins cannot change their own role.")
            
        sql = "UPDATE users SET role = ? WHERE user_id = ?"
        rows_affected = self.db.execute_update(sql, (new_role, target_user_id))
        
        if rows_affected == 0:
            raise ValidationError(f"Target user {target_user_id} not found.")
            
        logger.info(f"Admin {admin_user_id} changed role for user {target_user_id} to {new_role}")
        return True


# --- Product & Inventory Service Class ---

class ProductService:
    """
    Manages product catalog, categories, reviews, and inventory levels.
    """

    def __init__(self, db_manager: DatabaseManager):
        """
        Initializes the product service.
        :param db_manager: An instance of DatabaseManager.
        """
        self.db = db_manager
        logger.info("ProductService initialized.")

    def add_product_category(self, name: str, description: str, parent_category_id: Optional[int] = None) -> int:
        """
        Adds a new product category.
        :param name: Category name.
        :param description: Category description.
        :param parent_category_id: Optional parent category for sub-categories.
        :return: The new category ID.
        """
        sql = """
        INSERT INTO categories (name, description, parent_category_id)
        VALUES (?, ?, ?)
        """
        return self.db.execute_insert_get_id(sql, (name, description, parent_category_id))

    def add_product(self, name: str, description: str, price: Decimal, category_id: int, stock_quantity: int, sku: str) -> int:
        """
        Adds a new product to the catalog.
        :param name: Product name.
        :param description: Product description.
        :param price: Product price (as Decimal).
        :param category_id: The category this product belongs to.
        :param stock_quantity: Initial stock quantity.
        :param sku: Stock Keeping Unit (must be unique).
        :return: The new product ID.
        """
        if price <= Decimal('0.00'):
            raise ValidationError("Price must be positive.")
        if stock_quantity < 0:
            raise ValidationError("Stock quantity cannot be negative.")
            
        # Check for unique SKU
        if self.get_product_by_sku(sku):
            raise ValidationError(f"SKU '{sku}' already exists.")
            
        db_price = decimal_to_db(price)
        
        product_sql = """
        INSERT INTO products (name, description, price, category_id, sku, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        inventory_sql = """
        INSERT INTO inventory (product_id, quantity, last_updated)
        VALUES (?, ?, ?)
        """
        
        conn = self.db.connect()
        try:
            with conn:
                cursor = conn.cursor()
                now = datetime.datetime.utcnow().isoformat()
                
                # Insert product
                cursor.execute(product_sql, (name, description, db_price, category_id, sku, now))
                product_id = cursor.lastrowid
                
                if not product_id:
                    raise DatabaseError("Failed to get lastrowid for new product.")
                
                # Insert inventory
                cursor.execute(inventory_sql, (product_id, stock_quantity, now))
                
                logger.info(f"Added new product {name} (ID: {product_id}, SKU: {sku}) with stock {stock_quantity}")
                return product_id
                
        except sqlite3.Error as e:
            logger.error(f"Failed to add product {name}: {e}")
            raise DatabaseError(f"Product creation failed: {e}")

    def get_product_by_id(self, product_id: int) -> Optional[Dict[str, Any]]:
        """
        Retrieves a single product and its inventory level by ID.
        :param product_id: The product ID.
        :return: A dictionary of product data or None.
        """
        sql = """
        SELECT 
            p.product_id, p.name, p.description, p.price, p.sku, p.created_at,
            c.name as category_name, c.category_id,
            i.quantity as stock_quantity
        FROM products p
        JOIN categories c ON p.category_id = c.category_id
        JOIN inventory i ON p.product_id = i.product_id
        WHERE p.product_id = ?
        """
        result = self.db.execute_query(sql, (product_id,))
        if not result:
            return None
            
        product = dict(result[0])
        product['price'] = db_to_decimal(product['price'])
        return product

    def get_product_by_sku(self, sku: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves a single product and its inventory level by SKU.
        :param sku: The product SKU.
        :return: A dictionary of product data or None.
        """
        sql = """
        SELECT 
            p.product_id, p.name, p.description, p.price, p.sku, p.created_at,
            c.name as category_name, c.category_id,
            i.quantity as stock_quantity
        FROM products p
        JOIN categories c ON p.category_id = c.category_id
        JOIN inventory i ON p.product_id = i.product_id
        WHERE p.sku = ?
        """
        result = self.db.execute_query(sql, (sku,))
        if not result:
            return None
            
        product = dict(result[0])
        product['price'] = db_to_decimal(product['price'])
        return product

    def update_product_stock(self, product_id: int, quantity_change: int) -> int:
        """
        Updates the stock for a product. Use negative for reduction.
        This function *must* be called within a transaction if part of an order.
        :param product_id: The product ID to update.
        :param quantity_change: The amount to add/subtract (e.g., -2 to subtract 2).
        :return: The new stock level.
        """
        now = datetime.datetime.utcnow().isoformat()
        
        # This SQL ensures we don't go below zero
        sql = """
        UPDATE inventory
        SET 
            quantity = quantity + ?,
            last_updated = ?
        WHERE product_id = ? AND (quantity + ?) >= 0
        """
        
        params = (quantity_change, now, product_id, quantity_change)
        rows_affected = self.db.execute_update(sql, params)
        
        if rows_affected == 0:
            # Check current stock to see why it failed
            current_stock = self.get_stock_level(product_id)
            if current_stock is None:
                raise InventoryError(f"Product ID {product_id} not found in inventory.")
            if current_stock + quantity_change < 0:
                logger.error(f"InventoryError: Tried to reduce stock for {product_id} by {abs(quantity_change)}, but only {current_stock} available.")
                raise InventoryError(f"Insufficient stock for product ID {product_id}. Available: {current_stock}, Requested: {abs(quantity_change)}")
            raise DatabaseError("Failed to update stock, unknown reason.")

        new_stock = self.get_stock_level(product_id)
        logger.info(f"Updated stock for product_id {product_id} by {quantity_change}. New stock: {new_stock}")
        return new_stock

    def get_stock_level(self, product_id: int) -> Optional[int]:
        """
        Gets the current stock level for a single product.
        :param product_id: The product ID.
        :return: The stock quantity, or None if product not found.
        """
        sql = "SELECT quantity FROM inventory WHERE product_id = ?"
        result = self.db.execute_query(sql, (product_id,))
        return result[0]['quantity'] if result else None

    def search_products(self, search_term: str, category_id: Optional[int] = None, min_price: Optional[Decimal] = None, max_price: Optional[Decimal] = None, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Performs a complex search for products.
        :param search_term: Text to search in name and description.
        :param category_id: Optional category to filter by.
        :param min_price: Optional minimum price.
        :param max_price: Optional maximum price.
        :param limit: Max number of results.
        :return: A list of product dictionaries.
        """
        
        # This query is complex and interdependent on multiple tables
        sql_base = """
        SELECT 
            p.product_id, p.name, p.price, p.sku,
            c.name as category_name,
            i.quantity as stock_quantity,
            AVG(r.rating) as average_rating
        FROM products p
        JOIN categories c ON p.category_id = c.category_id
        JOIN inventory i ON p.product_id = i.product_id
        LEFT JOIN reviews r ON p.product_id = r.product_id
        WHERE (p.name LIKE ? OR p.description LIKE ?)
        """
        
        params = [f'%{search_term}%', f'%{search_term}%']
        
        if category_id is not None:
            sql_base += " AND p.category_id = ? "
            params.append(category_id)
            
        if min_price is not None:
            sql_base += " AND p.price >= ? "
            params.append(decimal_to_db(min_price))
            
        if max_price is not None:
            sql_base += " AND p.price <= ? "
            params.append(decimal_to_db(max_price))
            
        sql_end = """
        GROUP BY p.product_id, p.name, p.price, p.sku, c.name, i.quantity
        ORDER BY average_rating DESC, p.name
        LIMIT ?
        """
        params.append(limit)
        
        full_sql = sql_base + sql_end
        
        results = self.db.execute_query(full_sql, tuple(params))
        
        # Convert price back to Decimal
        products = []
        for row in results:
            product = dict(row)
            product['price'] = db_to_decimal(product['price'])
            products.append(product)
            
        return products

    def add_product_review(self, user_id: int, product_id: int, rating: int, review_text: str) -> int:
        """
        Allows a user to add a review for a product.
        (Note: In a real system, we'd verify the user purchased the product first).
        :param user_id: The user leaving the review.
        :param product_id: The product being reviewed.
        :param rating: Rating from 1 to 5.
        :param review_text: The text content of the review.
        :return: The new review ID.
        """
        if not (1 <= rating <= 5):
            raise ValidationError("Rating must be between 1 and 5.")
            
        # Check if user has already reviewed this product
        existing_sql = "SELECT review_id FROM reviews WHERE user_id = ? AND product_id = ?"
        if self.db.execute_query(existing_sql, (user_id, product_id)):
            raise ValidationError("You have already reviewed this product.")
            
        sql = """
        INSERT INTO reviews (product_id, user_id, rating, review_text, created_at)
        VALUES (?, ?, ?, ?, ?)
        """
        now = datetime.datetime.utcnow().isoformat()
        review_id = self.db.execute_insert_get_id(sql, (product_id, user_id, rating, review_text, now))
        logger.info(f"User {user_id} added review {review_id} for product {product_id} with rating {rating}")
        
        # This is interdependent: we immediately update the product's average rating
        self.update_product_average_rating(product_id)
        
        return review_id

    def update_product_average_rating(self, product_id: int):
        """
        A helper function to recalculate and update a product's average rating.
        This is an example of an interdependent function call.
        :param product_id: The product to update.
        """
        
        # This SQL calculates the average from the reviews table
        avg_sql = """
        SELECT AVG(rating) as avg_rating
        FROM reviews
        WHERE product_id = ?
        """
        
        result = self.db.execute_query(avg_sql, (product_id,))
        avg_rating = result[0]['avg_rating'] if result and result[0]['avg_rating'] is not None else 0.0
        
        # This SQL updates the products table
        update_sql = """
        UPDATE products
        SET average_rating = ?
        WHERE product_id = ?
        """
        
        try:
            self.db.execute_update(update_sql, (round(avg_rating, 2), product_id))
            logger.info(f"Updated average rating for product {product_id} to {avg_rating:.2f}")
        except DatabaseError as e:
            logger.error(f"Failed to update average rating for product {product_id}: {e}")
            # Non-fatal, don't crash the review submission


# --- Order Service Class ---

class OrderService:
    """
    Manages the creation, processing, and fulfillment of orders.
    This class is highly interdependent on UserService and ProductService.
    """

    CartItem = namedtuple('CartItem', ['product_id', 'quantity'])

    def __init__(self, db_manager: DatabaseManager, user_service: UserService, product_service: ProductService):
        """
        Initializes the order service.
        :param db_manager: An instance of DatabaseManager.
        :param user_service: An instance of UserService.
        :param product_service: An instance of ProductService.
        """
        self.db = db_manager
        self.users = user_service
        self.products = product_service
        logger.info("OrderService initialized.")

    def create_order(self, user_id: int, cart: List[CartItem], shipping_address_id: int, billing_address_id: int, shipping_method: str = 'STANDARD') -> int:
        """
        Creates a new order from a user's cart.
        This is a complex, transactional operation.
        :param user_id: The user placing the order.
        :param cart: A list of CartItem tuples (product_id, quantity).
        :param shipping_address_id: The user's address ID for shipping.
        :param billing_address_id: The user's address ID for billing.
        :param shipping_method: 'STANDARD' or 'EXPRESS'.
        :return: The new order ID.
        """
        if not cart:
            raise OrderProcessingError("Cannot create an order with an empty cart.")
        if len(cart) > MAX_ORDER_ITEMS:
            raise OrderProcessingError(f"Cart exceeds maximum item count of {MAX_ORDER_ITEMS}.")
            
        # --- 1. Validation Phase ---
        
        # Validate user and addresses
        try:
            user_profile = self.users.get_user_profile(user_id)
            user_addresses = {addr['address_id'] for addr in user_profile['addresses']}
            if shipping_address_id not in user_addresses or billing_address_id not in user_addresses:
                raise OrderProcessingError("Invalid shipping or billing address ID for this user.")
        except ValidationError as e:
            raise OrderProcessingError(f"Invalid user: {e}")
            
        # --- 2. Pricing and Stock Check Phase ---
        
        # This block is highly interdependent on ProductService
        subtotal = Decimal('0.00')
        validated_items = []
        
        for item in cart:
            product = self.products.get_product_by_id(item.product_id)
            if not product:
                raise OrderProcessingError(f"Product ID {item.product_id} not found.")
                
            if item.quantity <= 0:
                raise OrderProcessingError(f"Invalid quantity ({item.quantity}) for product {item.product_id}.")
                
            current_stock = product['stock_quantity']
            if current_stock < item.quantity:
                logger.warning(f"Order failed: Insufficient stock for {product['sku']} (ID: {item.product_id}). Needed: {item.quantity}, Have: {current_stock}")
                raise InventoryError(f"Insufficient stock for '{product['name']}'. Requested: {item.quantity}, Available: {current_stock}")
                
            item_price = product['price']
            line_total = item_price * Decimal(item.quantity)
            subtotal += line_total
            
            validated_items.append({
                'product_id': item.product_id,
                'quantity': item.quantity,
                'price_at_purchase': item_price  # Lock in the price
            })
            
        # --- 3. Calculate Final Total ---
        
        shipping_fee = self.calculate_shipping(subtotal, shipping_method)
        total_amount = subtotal + shipping_fee
        
        # --- 4. Database Transaction Phase ---
        
        conn = self.db.connect()
        try:
            with conn:
                cursor = conn.cursor()
                now = datetime.datetime.utcnow().isoformat()
                
                # Step 4a: Create the main order record
                order_sql = """
                INSERT INTO orders (user_id, status, total_amount, subtotal, shipping_fee, shipping_address_id, billing_address_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """
                cursor.execute(order_sql, (
                    user_id, STATUS_PENDING, decimal_to_db(total_amount), decimal_to_db(subtotal),
                    decimal_to_db(shipping_fee), shipping_address_id, billing_address_id, now
                ))
                order_id = cursor.lastrowid
                if not order_id:
                    raise DatabaseError("Failed to create order record and get ID.")
                    
                logger.info(f"Created order {order_id} for user {user_id}. Status: PENDING.")
                
                # Step 4b: Insert all order items
                items_sql = """
                INSERT INTO order_items (order_id, product_id, quantity, price_at_purchase)
                VALUES (?, ?, ?, ?)
                """
                item_data_tuples = [
                    (order_id, item['product_id'], item['quantity'], decimal_to_db(item['price_at_purchase']))
                    for item in validated_items
                ]
                cursor.executemany(items_sql, item_data_tuples)
                
                # Step 4c: Update inventory (interdependent call)
                # We do this inside the same transaction
                # We need to use the *ProductService* but on the *current* connection/cursor
                # This is tricky. A better design would have ProductService accept a cursor.
                # For this sim, we'll just re-implement the stock update logic here.
                
                logger.info(f"Updating inventory for {len(validated_items)} items in order {order_id}")
                inventory_update_sql = """
                UPDATE inventory
                SET 
                    quantity = quantity - ?,
                    last_updated = ?
                WHERE product_id = ? AND (quantity - ?) >= 0
                """
                for item in validated_items:
                    rows_affected = cursor.execute(inventory_update_sql, (
                        item['quantity'], now, item['product_id'], item['quantity']
                    )).rowcount
                    
                    if rows_affected == 0:
                        # This should have been caught in phase 2, but this is a final check
                        # If it fails here, the transaction rolls back.
                        product = self.products.get_product_by_id(item['product_id'])
                        raise InventoryError(f"Failed to reserve stock for '{product['name']}'. Stock may have changed. Please try again.")

                # Step 4d: Add an entry to order_status_history
                history_sql = """
                INSERT INTO order_status_history (order_id, status, changed_at)
                VALUES (?, ?, ?)
                """
                cursor.execute(history_sql, (order_id, STATUS_PENDING, now))
            
            # Transaction commits here
            logger.info(f"Successfully created and reserved stock for order {order_id}. Total: {total_amount}")
            return order_id
            
        except (sqlite3.Error, InventoryError, DatabaseError) as e:
            logger.error(f"Failed to create order for user {user_id} due to: {e}. Rolling back transaction.")
            # Transaction automatically rolls back on exception
            if isinstance(e, InventoryError):
                raise  # Re-raise the specific error
            raise OrderProcessingError(f"Order creation failed due to a database error: {e}")

    def calculate_shipping(self, subtotal: Decimal, method: str) -> Decimal:
        """
        Calculates shipping fee based on subtotal and method.
        :param subtotal: The order subtotal.
        :param method: 'STANDARD' or 'EXPRESS'.
        :return: The shipping fee.
        """
        if subtotal >= FREE_SHIPPING_THRESHOLD and method == 'STANDARD':
            return Decimal('0.00')
            
        if method == 'EXPRESS':
            return SHIPPING_FEE_EXPRESS
            
        return SHIPPING_FEE_STANDARD

    def update_order_status(self, order_id: int, new_status: str, admin_user_id: Optional[int] = None) -> bool:
        """
        Updates an order's status.
        :param order_id: The order to update.
        :param new_status: The new status (e.g., PAID, SHIPPED).
        :param admin_user_id: Optional. If provided, checks for admin/support role.
        :return: True on success.
        """
        if new_status not in VALID_STATUSES:
            raise ValidationError(f"Invalid order status: {new_status}")
            
        if admin_user_id:
            admin = self.users.find_user_by_id(admin_user_id)
            if not admin or admin['role'] not in (ROLE_ADMIN, ROLE_SUPPORT):
                raise AuthenticationError("You do not have permission to update order status.")
        
        # Get current status
        current_status_res = self.db.execute_query("SELECT status FROM orders WHERE order_id = ?", (order_id,))
        if not current_status_res:
            raise OrderProcessingError(f"Order ID {order_id} not found.")
        current_status = current_status_res[0]['status']

        if current_status == new_status:
            return True # No change needed

        # Add state transition logic
        if current_status == STATUS_CANCELLED or current_status == STATUS_REFUNDED:
            raise OrderProcessingError(f"Cannot change status of a {current_status} order.")
        
        # --- Transaction to update status and log history ---
        conn = self.db.connect()
        try:
            with conn:
                now = datetime.datetime.utcnow().isoformat()
                
                # Step 1: Update the order
                order_update_sql = "UPDATE orders SET status = ? WHERE order_id = ?"
                conn.execute(order_update_sql, (new_status, order_id))
                
                # Step 2: Log the change
                history_sql = """
                INSERT INTO order_status_history (order_id, status, changed_at, changed_by_user_id)
                VALUES (?, ?, ?, ?)
                """
                conn.execute(history_sql, (order_id, new_status, now, admin_user_id))
                
                # Step 3: Interdependent action: Handle refunds
                if new_status == STATUS_CANCELLED or new_status == STATUS_REFUNDED:
                    # This function is interdependent with ProductService
                    self.restock_cancelled_order_items(order_id, conn)
            
            logger.info(f"Order {order_id} status updated to {new_status}" + (f" by user {admin_user_id}" if admin_user_id else ""))
            return True

        except (sqlite3.Error, InventoryError, DatabaseError) as e:
            logger.error(f"Failed to update status for order {order_id}: {e}. Rolling back.")
            raise OrderProcessingError(f"Order status update failed: {e}")

    def restock_cancelled_order_items(self, order_id: int, db_conn: sqlite3.Connection):
        """
        Helper function to restock items from a cancelled or refunded order.
        This MUST be called from within an existing database transaction.
        :param order_id: The order ID being cancelled.
        :param db_conn: The active database connection/transaction.
        """
        logger.warning(f"Restocking items for cancelled/refunded order {order_id}")
        
        # Get all items from the order
        items_sql = "SELECT product_id, quantity FROM order_items WHERE order_id = ?"
        try:
            items = db_conn.execute(items_sql, (order_id,)).fetchall()
            
            if not items:
                logger.error(f"No items found for order {order_id} during restock. This is unusual.")
                return

            inventory_update_sql = """
            UPDATE inventory
            SET 
                quantity = quantity + ?,
                last_updated = ?
            WHERE product_id = ?
            """
            now = datetime.datetime.utcnow().isoformat()
            
            for item in items:
                # Use the ProductService to update stock
                # This is complex. We're inside the OrderService transaction,
                # so we can't use self.products.update_product_stock directly
                # as it would start a *new* transaction.
                # We must execute the query on the provided connection.
                
                db_conn.execute(inventory_update_sql, (item['quantity'], now, item['product_id']))
                logger.info(f"Restocked {item['quantity']} of product {item['product_id']} from order {order_id}")

        except sqlite3.Error as e:
            logger.error(f"CRITICAL: Failed to restock items for order {order_id} during cancellation: {e}")
            # We raise this to roll back the *entire* status change
            raise InventoryError(f"Failed to restock items for order {order_id}: {e}")

    def get_order_details(self, order_id: int) -> Dict[str, Any]:
        """
        Retrieves complete details for a single order.
        This is a very complex, multi-join SQL query.
        :param order_id: The ID of the order to fetch.
        :return: A dictionary with order details, items, and history.
        """
        
        # Query 1: Get main order info and joined addresses
        order_sql = """
        SELECT
            o.order_id, o.user_id, o.status, o.total_amount, o.subtotal, o.shipping_fee, o.created_at,
            u.email as user_email,
            sa.street_line1 as ship_street1, sa.street_line2 as ship_street2, sa.city as ship_city, 
            sa.state as ship_state, sa.postal_code as ship_zip, sa.country as ship_country,
            ba.street_line1 as bill_street1, ba.street_line2 as bill_street2, ba.city as bill_city,
            ba.state as bill_state, ba.postal_code as bill_zip, ba.country as bill_country
        FROM orders o
        JOIN users u ON o.user_id = u.user_id
        JOIN addresses sa ON o.shipping_address_id = sa.address_id
        JOIN addresses ba ON o.billing_address_id = ba.address_id
        WHERE o.order_id = ?
        """
        
        # Query 2: Get all items for the order
        items_sql = """
        SELECT
            oi.product_id, oi.quantity, oi.price_at_purchase,
            p.name as product_name, p.sku
        FROM order_items oi
        JOIN products p ON oi.product_id = p.product_id
        WHERE oi.order_id = ?
        """
        
        # Query 3: Get order status history
        history_sql = """
        SELECT
            h.status, h.changed_at, h.changed_by_user_id,
            u.email as changed_by_email
        FROM order_status_history h
        LEFT JOIN users u ON h.changed_by_user_id = u.user_id
        WHERE h.order_id = ?
        ORDER BY h.changed_at ASC
        """
        
        order_res = self.db.execute_query(order_sql, (order_id,))
        if not order_res:
            raise OrderProcessingError(f"Order ID {order_id} not found.")
            
        items_res = self.db.execute_query(items_sql, (order_id,))
        history_res = self.db.execute_query(history_sql, (order_id,))
        
        # Assemble the final nested dictionary
        order_data = dict(order_res[0])
        
        # Convert Decimals
        order_data['total_amount'] = db_to_decimal(order_data['total_amount'])
        order_data['subtotal'] = db_to_decimal(order_data['subtotal'])
        order_data['shipping_fee'] = db_to_decimal(order_data['shipping_fee'])
        
        # Format items
        order_items = []
        for row in items_res:
            item = dict(row)
            item['price_at_purchase'] = db_to_decimal(item['price_at_purchase'])
            order_items.append(item)
            
        order_data['items'] = order_items
        order_data['status_history'] = [dict(row) for row in history_res]
        
        return order_data


# --- Reporting Service Class ---

class ReportingService:
    """
    Generates complex, read-only reports for business analytics.
    These queries are often the most complex and interdependent.
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initializes the reporting service.
        :param db_manager: An instance of DatabaseManager.
        """
        self.db = db_manager
        logger.info("ReportingService initialized.")

    def get_sales_summary_by_date_range(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Generates a sales summary (total revenue, orders, items) for a date range.
        :param start_date: ISO 8601 date string (e.g., '2023-01-01')
        :param end_date: ISO 8601 date string (e.g., '2023-01-31')
        :return: A dictionary containing the summary.
        """
        
        # This query joins orders and order_items and filters by date
        # It excludes cancelled/refunded orders from revenue.
        sql = """
        SELECT
            COUNT(DISTINCT o.order_id) as total_orders,
            SUM(o.total_amount) as total_revenue,
            SUM(oi.quantity) as total_items_sold,
            AVG(o.total_amount) as average_order_value
        FROM orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        WHERE o.created_at >= ? 
          AND o.created_at <= ?
          AND o.status NOT IN (?, ?)
        """
        
        # Add time to end_date to make it inclusive
        end_date_inclusive = end_date + 'T23:59:59Z'
        start_date_iso = start_date + 'T00:00:00Z'
        
        params = (start_date_iso, end_date_inclusive, STATUS_CANCELLED, STATUS_REFUNDED)
        
        result = self.db.execute_query(sql, params)
        summary = dict(result[0])
        
        # Convert Decimals
        summary['total_revenue'] = db_to_decimal(summary['total_revenue'] or '0.00')
        summary['average_order_value'] = db_to_decimal(summary['average_order_value'] or '0.00')
        summary['total_orders'] = summary['total_orders'] or 0
        summary['total_items_sold'] = summary['total_items_sold'] or 0
        
        return summary

    def get_top_selling_products(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Gets the top-selling products by quantity sold.
        :param limit: Number of products to return.
        :return: A list of product summary dictionaries.
        """
        
        # This query joins products, order_items, and orders
        sql = """
        SELECT
            p.product_id,
            p.name,
            p.sku,
            SUM(oi.quantity) as total_quantity_sold,
            SUM(oi.quantity * oi.price_at_purchase) as total_revenue
        FROM products p
        JOIN order_items oi ON p.product_id = oi.product_id
        JOIN orders o ON oi.order_id = o.order_id
        WHERE o.status NOT IN (?, ?)
        GROUP BY p.product_id, p.name, p.sku
        ORDER BY total_quantity_sold DESC
        LIMIT ?
        """
        
        params = (STATUS_CANCELLED, STATUS_REFUNDED, limit)
        results = self.db.execute_query(sql, params)
        
        top_products = []
        for row in results:
            product = dict(row)
            product['total_revenue'] = db_to_decimal(product['total_revenue'])
            top_products.append(product)
            
        return top_products

    def get_customer_lifetime_value_report(self, limit: int = 25) -> List[Dict[str, Any]]:
        """
        Generates a report of top customers by total amount spent (LTV).
        :param limit: Number of customers to return.
        :return: A list of customer LTV summaries.
        """
        
        # This query joins users and orders
        sql = """
        SELECT
            u.user_id,
            u.email,
            u.first_name,
            u.last_name,
            COUNT(o.order_id) as total_orders,
            SUM(o.total_amount) as lifetime_value
        FROM users u
        JOIN orders o ON u.user_id = o.user_id
        WHERE o.status NOT IN (?, ?)
        GROUP BY u.user_id, u.email, u.first_name, u.last_name
        ORDER BY lifetime_value DESC
        LIMIT ?
        """
        
        params = (STATUS_CANCELLED, STATUS_REFUNDED, limit)
        results = self.db.execute_query(sql, params)
        
        top_customers = []
        for row in results:
            customer = dict(row)
            customer['lifetime_value'] = db_to_decimal(customer['lifetime_value'])
            top_customers.append(customer)
            
        return top_customers

    def get_inventory_stock_report(self, low_stock_threshold: int = 10) -> Dict[str, List[Dict[str, Any]]]:
        """
        Generates a report of all inventory, highlighting low-stock items.
        :param low_stock_threshold: The quantity to consider 'low stock'.
        :return: A dictionary with 'low_stock' and 'in_stock' lists.
        """
        
        sql = """
        SELECT
            p.product_id,
            p.name,
            p.sku,
            c.name as category_name,
            i.quantity,
            i.last_updated
        FROM products p
        JOIN inventory i ON p.product_id = i.product_id
        JOIN categories c ON p.category_id = c.category_id
        ORDER BY i.quantity ASC
        """
        
        results = self.db.execute_query(sql)
        
        report = {
            'low_stock': [],
            'in_stock': []
        }
        
        for row in results:
            item = dict(row)
            if item['quantity'] <= low_stock_threshold:
                report['low_stock'].append(item)
            else:
                report['in_stock'].append(item)
                
        return report


# --- Main Application Setup & Schema Definition ---

def setup_database_schema(db_manager: DatabaseManager):
    """
    Defines and executes the entire database schema (DDL).
    This function demonstrates the complex interdependencies of the tables
    with foreign keys.
    """
    
    schema_script = """
    PRAGMA foreign_keys = ON;

    -- Users Table: Stores customer and admin information
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'CUSTOMER' CHECK(role IN ('CUSTOMER', 'ADMIN', 'SUPPORT')),
        created_at TEXT NOT NULL,
        last_login TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_users_email ON users (email);

    -- Addresses Table: Stores multiple addresses per user
    CREATE TABLE IF NOT EXISTS addresses (
        address_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        street_line1 TEXT,
        street_line2 TEXT,
        city TEXT,
        state TEXT,
        postal_code TEXT,
        country TEXT,
        is_default_shipping INTEGER DEFAULT 0,
        is_default_billing INTEGER DEFAULT 0,
        FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_addresses_user_id ON addresses (user_id);

    -- Categories Table: For organizing products
    CREATE TABLE IF NOT EXISTS categories (
        category_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        description TEXT,
        parent_category_id INTEGER,
        FOREIGN KEY (parent_category_id) REFERENCES categories (category_id) ON DELETE SET NULL
    );
    CREATE INDEX IF NOT EXISTS idx_categories_name ON categories (name);

    -- Products Table: The main product catalog
    CREATE TABLE IF NOT EXISTS products (
        product_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        description TEXT,
        price TEXT NOT NULL, -- Stored as string to avoid precision loss
        category_id INTEGER NOT NULL,
        sku TEXT UNIQUE NOT NULL,
        average_rating REAL DEFAULT 0.0,
        created_at TEXT NOT NULL,
        FOREIGN KEY (category_id) REFERENCES categories (category_id) ON DELETE RESTRICT
    );
    CREATE INDEX IF NOT EXISTS idx_products_sku ON products (sku);
    CREATE INDEX IF NOT EXISTS idx_products_name ON products (name);
    CREATE INDEX IF NOT EXISTS idx_products_category_id ON products (category_id);

    -- Inventory Table: Tracks stock for each product
    CREATE TABLE IF NOT EXISTS inventory (
        product_id INTEGER PRIMARY KEY,
        quantity INTEGER NOT NULL DEFAULT 0 CHECK(quantity >= 0),
        last_updated TEXT NOT NULL,
        FOREIGN KEY (product_id) REFERENCES products (product_id) ON DELETE CASCADE
    );

    -- Orders Table: The main record for each order
    CREATE TABLE IF NOT EXISTS orders (
        order_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        status TEXT NOT NULL CHECK(status IN ('PENDING', 'PAID', 'SHIPPED', 'DELIVERED', 'CANCELLED', 'REFUNDED')),
        total_amount TEXT NOT NULL,
        subtotal TEXT NOT NULL,
        shipping_fee TEXT NOT NULL,
        shipping_address_id INTEGER NOT NULL,
        billing_address_id INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE RESTRICT,
        FOREIGN KEY (shipping_address_id) REFERENCES addresses (address_id) ON DELETE RESTRICT,
        FOREIGN KEY (billing_address_id) REFERENCES addresses (address_id) ON DELETE RESTRICT
    );
    CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders (user_id);
    CREATE INDEX IF NOT EXISTS idx_orders_status ON orders (status);
    CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders (created_at);

    -- Order Items Table: Links products to orders (line items)
    CREATE TABLE IF NOT EXISTS order_items (
        order_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id INTEGER NOT NULL,
        product_id INTEGER NOT NULL,
        quantity INTEGER NOT NULL CHECK(quantity > 0),
        price_at_purchase TEXT NOT NULL, -- Price at the time of order
        FOREIGN KEY (order_id) REFERENCES orders (order_id) ON DELETE CASCADE,
        FOREIGN KEY (product_id) REFERENCES products (product_id) ON DELETE RESTRICT
    );
    CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items (order_id);
    CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items (product_id);

    -- Reviews Table: User reviews for products
    CREATE TABLE IF NOT EXISTS reviews (
        review_id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        rating INTEGER NOT NULL CHECK(rating >= 1 AND rating <= 5),
        review_text TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY (product_id) REFERENCES products (product_id) ON DELETE CASCADE,
        FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE,
        UNIQUE (product_id, user_id) -- One review per user per product
    );
    CREATE INDEX IF NOT EXISTS idx_reviews_product_id ON reviews (product_id);

    -- Order Status History Table: Logs all status changes for an order
    CREATE TABLE IF NOT EXISTS order_status_history (
        history_id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id INTEGER NOT NULL,
        status TEXT NOT NULL,
        changed_at TEXT NOT NULL,
        changed_by_user_id INTEGER, -- NULL if changed by system
        FOREIGN KEY (order_id) REFERENCES orders (order_id) ON DELETE CASCADE,
        FOREIGN KEY (changed_by_user_id) REFERENCES users (user_id) ON DELETE SET NULL
    );
    CREATE INDEX IF NOT EXISTS idx_order_status_history_order_id ON order_status_history (order_id);
    """
    
    try:
        db_manager.execute_script(schema_script)
        logger.info("Database schema verified/created successfully.")
    except DatabaseError as e:
        logger.critical(f"FATAL: Could not initialize database schema: {e}")
        raise

# --- Main Execution Block (Example Usage) ---

def main():
    """
    Main function to initialize services and demonstrate usage.
    """
    logger.info("--- Starting E-Commerce Backend Service (Demo) ---")
    
    # Initialize all services with the same DB manager
    db_manager = DatabaseManager(DB_NAME)
    
    # Create schema if it doesn't exist
    setup_database_schema(db_manager)
    
    user_service = UserService(db_manager)
    product_service = ProductService(db_manager)
    order_service = OrderService(db_manager, user_service, product_service)
    reporting_service = ReportingService(db_manager)
    
    logger.info("All services initialized.")
    
    try:
        # --- Demo 1: User Registration ---
        logger.info("Demo 1: Registering users...")
        try:
            admin_id = user_service.register_user("admin@example.com", "AdminPass123", "Admin", "User")
            user_service.change_user_role(admin_id, ROLE_ADMIN, admin_id) # Fails (can't change own role)
        except ValidationError as e:
            logger.warning(f"Caught expected error: {e}")
            # We need an admin, let's just update the DB directly for the demo
            db_manager.execute_update("UPDATE users SET role = ? WHERE email = ?", (ROLE_ADMIN, "admin@example.com"))
            admin_user = user_service.find_user_by_email("admin@example.com")
            admin_id = admin_user['user_id']
            logger.info(f"Admin user created/promoted with ID: {admin_id}")

        user_id_1 = user_service.register_user("alice@example.com", "AlicePass123", "Alice", "Smith")
        user_id_2 = user_service.register_user("bob@example.com", "BobPass123", "Bob", "Johnson")
        
        # --- Demo 2: Admin creates categories and products ---
        logger.info("Demo 2: Creating categories and products...")
        cat_id_electronics = product_service.add_product_category("Electronics", "Gadgets and devices")
        cat_id_books = product_service.add_product_category("Books", "Paperback and hardcover books")
        
        prod_id_laptop = product_service.add_product("Pro Laptop 15\"", "A powerful laptop", Decimal("1299.99"), cat_id_electronics, 50, "SKU-LAP-001")
        prod_id_phone = product_service.add_product("Smart Phone X", "The latest smartphone", Decimal("799.00"), cat_id_electronics, 150, "SKU-PHN-002")
        prod_id_book = product_service.add_product("Database Design", "A book on SQL", Decimal("49.95"), cat_id_books, 200, "SKU-BOK-003")

        # --- Demo 3: Users update profile and add reviews ---
        logger.info("Demo 3: Updating profiles and adding reviews...")
        user_service.update_user_address(user_id_1, 1, {
            'street_line1': '123 Main St',
            'city': 'Anytown',
            'state': 'CA',
            'postal_code': '12345',
            'country': 'USA'
        })
        profile = user_service.get_user_profile(user_id_1)
        alice_addr_id = profile['addresses'][0]['address_id']
        
        product_service.add_product_review(user_id_1, prod_id_laptop, 5, "Amazing laptop! Super fast.")
        product_service.add_product_review(user_id_2, prod_id_laptop, 4, "Pretty good, but battery could be better.")
        
        laptop_details = product_service.get_product_by_id(prod_id_laptop)
        logger.info(f"Laptop average rating is now: {laptop_details['average_rating']}")

        # --- Demo 4: User 1 creates an order ---
        logger.info("Demo 4: Creating an order...")
        cart = [
            OrderService.CartItem(product_id=prod_id_laptop, quantity=1),
            OrderService.CartItem(product_id=prod_id_book, quantity=2)
        ]
        
        try:
            order_id_1 = order_service.create_order(user_id_1, cart, alice_addr_id, alice_addr_id, 'STANDARD')
            logger.info(f"Successfully created order {order_id_1}")
        except (OrderProcessingError, InventoryError) as e:
            logger.error(f"Failed to create order: {e}")

        # --- Demo 5: Admin processes the order ---
        logger.info("Demo 5: Processing the order...")
        if 'order_id_1' in locals():
            order_service.update_order_status(order_id_1, STATUS_PAID, admin_id)
            order_service.update_order_status(order_id_1, STATUS_SHIPPED, admin_id)
            order_service.update_order_status(order_id_1, STATUS_DELIVERED, admin_id)

        # --- Demo 6: Create a failing order (insufficient stock) ---
        logger.info("Demo 6: Creating a failing order (insufficient stock)...")
        cart_fail = [OrderService.CartItem(product_id=prod_id_laptop, quantity=1000)] # We only have < 50
        try:
            order_service.create_order(user_id_2, cart_fail, 2, 2, 'STANDARD') # Assuming user 2 has address ID 2
        except InventoryError as e:
            logger.warning(f"Caught expected inventory error: {e}")
        
        # --- Demo 7: Cancel an order and restock ---
        logger.info("Demo 7: Cancelling an order and restocking...")
        cart_cancel = [OrderService.CartItem(product_id=prod_id_phone, quantity=1)]
        try:
            order_id_2 = order_service.create_order(user_id_2, cart_cancel, 2, 2, 'STANDARD')
            logger.info(f"Created order {order_id_2} to be cancelled.")
            phone_stock_before = product_service.get_stock_level(prod_id_phone)
            logger.info(f"Stock of phone before cancel: {phone_stock_before}")
            
            order_service.update_order_status(order_id_2, STATUS_CANCELLED, admin_id)
            
            phone_stock_after = product_service.get_stock_level(prod_id_phone)
            logger.info(f"Stock of phone after cancel: {phone_stock_after}")
            assert phone_stock_after == phone_stock_before + 1
            logger.info("Restock successful.")
        except (OrderProcessingError, InventoryError) as e:
            logger.error(f"Failed during cancellation demo: {e}")


        # --- Demo 8: Run Reports ---
        logger.info("Demo 8: Running reports...")
        
        sales_summary = reporting_service.get_sales_summary_by_date_range('2020-01-01', datetime.datetime.utcnow().strftime('%Y-%m-%d'))
        logger.info(f"Sales Summary: {json.dumps(sales_summary, default=str, indent=2)}")
        
        top_products = reporting_service.get_top_selling_products()
        logger.info(f"Top Products: {json.dumps(top_products, default=str, indent=2)}")
        
        top_customers = reporting_service.get_customer_lifetime_value_report()
        logger.info(f"Top Customers: {json.dumps(top_customers, default=str, indent=2)}")
        
        inventory_report = reporting_service.get_inventory_stock_report()
        logger.info(f"Low Stock Items: {json.dumps(inventory_report['low_stock'], default=str, indent=2)}")

        # --- Demo 9: Get complex order details ---
        logger.info("Demo 9: Getting full order details...")
        if 'order_id_1' in locals():
            full_details = order_service.get_order_details(order_id_1)
            logger.info(f"Full details for order {order_id_1}: {json.dumps(full_details, default=str, indent=2)}")

    except Exception as e:
        logger.critical(f"An unhandled exception occurred during demo: {e}", exc_info=True)
    
    finally:
        db_manager.disconnect()
        logger.info("--- E-Commerce Backend Service (Demo) Finished ---")

if __name__ == "__main__":
    main()

# End of file. Approx 1150 lines.