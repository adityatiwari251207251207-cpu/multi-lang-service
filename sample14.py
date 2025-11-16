# -*- coding: utf-8 -*-
"""
ecommerce_service.py

A comprehensive, monolithic backend service for a fictional e-commerce platform.
This file includes modules for order processing, inventory management, user notifications,
and financial reporting.

Demonstrates:
- Interdependent functions (e.g., process_order calls check_inventory, charge_customer, etc.)
- Embedded SQL queries (as strings) for database interaction.
- Complex (mock) business logic.
- Utility classes and helper functions.
"""

import json
import datetime
import smtplib
import logging
from decimal import Decimal
from uuid import uuid4

# --- Constants & Configuration ---

DB_HOST = "prod.db.ecommerce.internal"
DB_USER = "service_account_orders"
DB_PASS = "secure_password_placeholder" # In a real app, use secrets management
DB_NAME = "ecommerce_main"

SMTP_SERVER = "smtp.mailservice.com"
SMTP_PORT = 587
SMTP_USER = "noreply@ecommerce.com"
SMTP_PASS = "secure_smtp_password"

LOGGING_LEVEL = logging.INFO
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# --- Logger Setup ---
logging.basicConfig(level=LOGGING_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# --- Mock Database Connection ---
# In a real application, this would use a library like psycopg2 or sqlalchemy
class MockDatabaseConnection:
    """
    A mock database connection class to simulate database interactions.
    This helps illustrate the SQL queries without a live database.
    """
    def __init__(self, host, dbname, user, password):
        self.host = host
        self.dbname = dbname
        self.user = user
        self.password = password
        self._connected = False
        logger.info(f"Initializing mock DB connection for {user}@{host}/{dbname}")

    def connect(self):
        """Simulates connecting to the database."""
        logger.info(f"Attempting to connect to mock DB {self.dbname}...")
        # Simulate connection delay
        # time.sleep(0.1) 
        self._connected = True
        logger.info("Mock DB connection successful.")

    def is_connected(self):
        """Checks if the connection is active."""
        return self._connected

    def close(self):
        """Simulates closing the connection."""
        self._connected = False
        logger.info(f"Mock DB connection to {self.dbname} closed.")

    def execute_query(self, sql, params=None):
        """
        Simulates executing a query that fetches data (e.g., SELECT).
        Returns mock data based on the query.
        """
        if not self.is_connected():
            logger.error("Cannot execute query: Not connected to database.")
            return None
        
        logger.debug(f"Executing SQL Query: {sql}")
        logger.debug(f"With Parameters: {params}")
        
        # --- SQL Query Mock Responses ---
        # This is where the "interdependent SQL" logic is simulated.
        
        if "SELECT product_id, name, price, stock_level FROM products WHERE product_id" in sql:
            # Mock for get_product_details
            product_id = params[0]
            return [{
                'product_id': product_id,
                'name': f'Mock Product {product_id}',
                'price': Decimal('199.99'),
                'stock_level': 100
            }]
            
        if "SELECT user_id, email, first_name, last_name, default_payment_id FROM users WHERE user_id" in sql:
            # Mock for get_user_details
            user_id = params[0]
            return [{
                'user_id': user_id,
                'email': 'customer@example.com',
                'first_name': 'John',
                'last_name': 'Doe',
                'default_payment_id': 'pay_mock_12345'
            }]
            
        if "SELECT payment_id, provider, last_four, expiry_date FROM payment_methods WHERE payment_id" in sql:
            # Mock for get_payment_details
            return [{'payment_id': params[0], 'provider': 'Visa', 'last_four': '4242', 'expiry_date': '12/26'}]
            
        if "SELECT promo_code, discount_type, discount_value, min_spend FROM promotions WHERE promo_code" in sql:
            # Mock for get_promotion_details
            return [{
                'promo_code': params[0],
                'discount_type': 'PERCENTAGE', # or 'FIXED'
                'discount_value': Decimal('10.00'), # 10%
                'min_spend': Decimal('50.00')
            }]
            
        if "SELECT * FROM orders WHERE order_date" in sql:
            # Mock for financial report
            return [
                {'order_id': 'order_mock_1', 'total_amount': Decimal('150.75'), 'status': 'COMPLETED'},
                {'order_id': 'order_mock_2', 'total_amount': Decimal('88.20'), 'status': 'COMPLETED'},
            ]

        logger.warning(f"No mock response defined for query: {sql}")
        return []

    def execute_commit(self, sql, params=None):
        """
        Simulates executing a query that changes data (e.g., INSERT, UPDATE, DELETE).
        Returns the number of rows affected.
        """
        if not self.is_connected():
            logger.error("Cannot execute commit: Not connected to database.")
            return 0
            
        logger.debug(f"Executing SQL Commit: {sql}")
        logger.debug(f"With Parameters: {params}")

        if "INSERT INTO orders" in sql:
            # Mock for create_order_record
            logger.info("Mock INSERT into 'orders' successful.")
            return 1 # 1 row affected
            
        if "INSERT INTO order_items" in sql:
            # Mock for create_order_record
            logger.info("Mock INSERT into 'order_items' successful.")
            return len(params) # N rows affected
            
        if "UPDATE products SET stock_level" in sql:
            # Mock for update_inventory_level
            logger.info("Mock UPDATE on 'products' successful.")
            return 1 # 1 row affected
            
        if "INSERT INTO order_status_history" in sql:
            # Mock for update_order_status
            logger.info("Mock INSERT into 'order_status_history' successful.")
            return 1
            
        if "UPDATE orders SET status" in sql:
            # Mock for update_order_status
            logger.info("Mock UPDATE on 'orders' successful.")
            return 1
            
        logger.warning(f"No mock response defined for commit query: {sql}")
        return 0

    def begin_transaction(self):
        """Simulates beginning a database transaction."""
        logger.info("Mock DB Transaction BEGAN.")
        pass

    def commit_transaction(self):
        """Simulates committing a database transaction."""
        logger.info("Mock DB Transaction COMMITTED.")
        pass

    def rollback_transaction(self):
        """Simulates rolling back a database transaction."""
        logger.warning("Mock DB Transaction ROLLED BACK.")
        pass

# --- Global DB Connection (Singleton Pattern) ---
# In a real app, use a connection pool
_db_connection = None

def get_db_connection():
    """
    Manages and returns a singleton mock database connection.
    """
    global _db_connection
    if _db_connection is None or not _db_connection.is_connected():
        logger.info("No active DB connection. Creating new one.")
        _db_connection = MockDatabaseConnection(DB_HOST, DB_NAME, DB_USER, DB_PASS)
        _db_connection.connect()
    return _db_connection

# --- Module: Payment Gateway ---

class MockPaymentGateway:
    """
    Simulates a third-party payment gateway like Stripe or Braintree.
    """
    def __init__(self, api_key):
        self.api_key = api_key
        logger.info("Mock Payment Gateway initialized.")

    def charge(self, amount, payment_token, currency="USD"):
        """
        Simulates charging a payment method.
        
        Args:
            amount (Decimal): The amount to charge.
            payment_token (str): A token representing the payment method.
            currency (str): The currency code.
            
        Returns:
            dict: A response dictionary with 'success' (bool) and 'transaction_id' or 'error'.
        """
        logger.info(f"Attempting to charge {amount} {currency} to token {payment_token}")
        
        if not isinstance(amount, Decimal) or amount <= Decimal('0.00'):
            logger.error(f"Invalid charge amount: {amount}")
            return {'success': False, 'error': 'Invalid charge amount.'}
            
        if payment_token == "token_fail_insufficient_funds":
            logger.warning(f"Payment failed for token {payment_token}: Insufficient funds.")
            return {'success': False, 'error': 'Insufficient funds.'}
            
        if payment_token == "token_fail_card_declined":
            logger.warning(f"Payment failed for token {payment_token}: Card declined.")
            return {'success': False, 'error': 'Card declined by issuer.'}

        # Simulate a successful charge
        transaction_id = f"txn_{uuid4()}"
        logger.info(f"Payment successful. Transaction ID: {transaction_id}")
        return {
            'success': True,
            'transaction_id': transaction_id,
            'amount_charged': amount,
            'currency': currency
        }

    def refund(self, transaction_id, amount):
        """
        Simulates refunding a previous transaction.
        """
        logger.info(f"Attempting to refund {amount} for transaction {transaction_id}")
        if not transaction_id.startswith("txn_"):
            logger.error("Refund failed: Invalid transaction ID format.")
            return {'success': False, 'error': 'Invalid transaction ID.'}
            
        # Simulate a successful refund
        refund_id = f"ref_{uuid4()}"
        logger.info(f"Refund successful. Refund ID: {refund_id}")
        return {
            'success': True,
            'refund_id': refund_id,
            'original_transaction_id': transaction_id,
            'amount_refunded': amount
        }

# --- Module: Notification Service ---

class NotificationService:
    """
    Handles sending notifications to users (e.g., email, SMS).
    """
    
    def __init__(self):
        logger.info("Notification Service initialized.")
        # In a real app, you might initialize connections to Twilio, SendGrid, etc.
        self.email_client = None

    def _connect_smtp(self):
        """
        Simulates connecting to an SMTP server for sending emails.
        """
        try:
            logger.info(f"Connecting to SMTP server {SMTP_SERVER}:{SMTP_PORT}...")
            # In a real app:
            # self.email_client = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
            # self.email_client.starttls()
            # self.email_client.login(SMTP_USER, SMTP_PASS)
            logger.info("SMTP connection successful (simulated).")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to SMTP server: {e}")
            return False

    def send_email(self, to_email, subject, body_html, body_text):
        """
        Simulates sending an email.
        """
        logger.info(f"Preparing to send email to: {to_email}")
        logger.info(f"Subject: {subject}")
        
        # Simulate the connection
        if self.email_client is None:
            if not self._connect_smtp():
                logger.error("Cannot send email, SMTP connection failed.")
                return False
                
        # In a real app:
        # msg = MIMEMultipart('alternative')
        # msg['Subject'] = subject
        # msg['From'] = SMTP_USER
        # msg['To'] = to_email
        # ... attach parts ...
        # self.email_client.sendmail(SMTP_USER, [to_email], msg.as_string())
        
        logger.info(f"Email successfully sent to {to_email} (simulated).")
        return True

    def send_order_confirmation_email(self, user_details, order_details):
        """
        A specific, interdependent function to format and send an order confirmation.
        """
        logger.info(f"Generating order confirmation for order {order_details['order_id']}")
        
        user_email = user_details.get('email')
        first_name = user_details.get('first_name', 'Valued Customer')
        
        if not user_email:
            logger.error("Cannot send order confirmation: User email is missing.")
            return False
            
        subject = f"Your e-commerce Order #{order_details['order_id']} is Confirmed!"
        
        # Build a simple text body
        text_body = f"Hello {first_name},\n\n"
        text_body += f"Thank you for your order! Your order ID is {order_details['order_id']}.\n\n"
        text_body += "Items:\n"
        for item in order_details['items']:
            text_body += f"- {item['name']} (x{item['quantity']}): ${item['line_total']}\n"
        text_body += f"\nSubtotal: ${order_details['subtotal']}"
        text_body += f"\nDiscount: -${order_details['discount_amount']}"
        text_body += f"\nShipping: ${order_details['shipping_cost']}"
        text_body += f"\nTotal: ${order_details['total_amount']}\n\n"
        text_body += "We'll notify you when your order ships.\n\n"
        text_body += "Thanks,\nThe e-commerce Team"
        
        # Build a simple HTML body
        html_body = f"<html><body>"
        html_body += f"<h1>Hello, {first_name}!</h1>"
        html_body += f"<p>Thank you for your order! Your order ID is <strong>{order_details['order_id']}</strong>.</p>"
        html_body += "<h2>Order Summary</h2>"
        html_body += "<table border='1' cellpadding='5' cellspacing='0'>"
        html_body += "<tr><th>Item</th><th>Quantity</th><th>Price</th></tr>"
        for item in order_details['items']:
            html_body += f"<tr><td>{item['name']}</td><td>{item['quantity']}</td><td>${item['line_total']}</td></tr>"
        html_body += "</table>"
        html_body += f"<p><strong>Subtotal:</strong> ${order_details['subtotal']}</p>"
        html_body += f"<p><strong>Discount:</strong> -${order_details['discount_amount']}</p>"
        html_body += f"<p><strong>Shipping:</strong> ${order_details['shipping_cost']}</p>"
        html_body += f"<h2><strong>Total: ${order_details['total_amount']}</strong></h2>"
        html_body += "<p>We'll notify you when your order ships.</p>"
        html_body += "<p>Thanks,<br>The e-commerce Team</p>"
        html_body += "</body></html>"
        
        return self.send_email(user_email, subject, html_body, text_body)

    def send_shipping_notification_email(self, user_email, order_id, tracking_number, carrier):
        """
        Simulates sending a shipping notification.
        """
        logger.info(f"Sending shipping notification for order {order_id}")
        subject = f"Your e-commerce Order #{order_id} has Shipped!"
        
        text_body = f"Great news! Your order {order_id} has shipped via {carrier}.\n"
        text_body += f"Your tracking number is: {tracking_number}\n"
        text_body += "You can track your package on the carrier's website.\n\n"
        text_body += "Thanks,\nThe e-commerce Team"
        
        html_body = f"<html><body>"
        html_body += f"<h1>Great news!</h1>"
        html_body += f"<p>Your order <strong>{order_id}</strong> has shipped via {carrier}.</p>"
        html_body += f"<p>Your tracking number is: <strong>{tracking_number}</strong></p>"
        html_body += "<p>Thanks,<br>The e-commerce Team</p>"
        html_body += "</body></html>"
        
        return self.send_email(user_email, subject, html_body, text_body)

# --- Module: Inventory Management ---

class InventoryManager:
    """
    Handles logic for checking and updating product inventory.
    """
    
    def __init__(self, db_conn):
        self.db = db_conn
        logger.info("Inventory Manager initialized.")

    def check_stock(self, product_id, quantity_requested):
        """
        Checks if sufficient stock exists for a product.
        
        Returns:
            dict: {'available': bool, 'stock_level': int, 'product_name': str}
        """
        logger.info(f"Checking stock for product {product_id}, quantity {quantity_requested}")
        
        # --- Interdependent SQL Query ---
        # This query is defined here but executed by the DB connection class.
        sql = """
        SELECT product_id, name, price, stock_level 
        FROM products 
        WHERE product_id = %s;
        """
        
        result = self.db.execute_query(sql, (product_id,))
        
        if not result:
            logger.warning(f"Stock check failed: Product {product_id} not found.")
            return {'available': False, 'error': 'Product not found.'}
            
        product_data = result[0]
        stock_level = product_data.get('stock_level', 0)
        
        if stock_level >= quantity_requested:
            logger.info(f"Stock available for {product_id}. Requested: {quantity_requested}, Available: {stock_level}")
            return {
                'available': True, 
                'stock_level': stock_level,
                'product_name': product_data.get('name'),
                'unit_price': product_data.get('price')
            }
        else:
            logger.warning(f"Insufficient stock for {product_id}. Requested: {quantity_requested}, Available: {stock_level}")
            return {
                'available': False, 
                'stock_level': stock_level, 
                'product_name': product_data.get('name'),
                'error': 'Insufficient stock.'
            }

    def reserve_stock(self, items_list, order_id):
        """
        Updates the stock levels in the database for a new order.
        This is a critical, interdependent step.
        
        Args:
            items_list (list): List of {'product_id': str, 'quantity': int}
            order_id (str): The ID of the order reserving the stock.
            
        Returns:
            bool: True if all stock was successfully updated, False otherwise.
        """
        logger.info(f"Reserving stock for order {order_id}")
        
        # This logic should be transactional. 
        # We check all items first, then update all items.
        
        # --- 1. Verification Phase ---
        stock_checks = []
        for item in items_list:
            check = self.check_stock(item['product_id'], item['quantity'])
            if not check['available']:
                logger.error(f"Failed to reserve stock. Item {item['product_id']} is out of stock.")
                # In a real app, you would roll back any reservations made so far.
                return False
            stock_checks.append(check)
        
        logger.info(f"All items for order {order_id} are available. Proceeding with reservation.")
        
        # --- 2. Reservation (Update) Phase ---
        # --- Interdependent SQL Query ---
        sql = """
        UPDATE products
        SET stock_level = stock_level - %s
        WHERE product_id = %s AND stock_level >= %s;
        """
        
        for item in items_list:
            quantity = item['quantity']
            product_id = item['product_id']
            
            # The "AND stock_level >= %s" provides a final check (optimistic locking)
            rows_affected = self.db.execute_commit(sql, (quantity, product_id, quantity))
            
            if rows_affected == 0:
                logger.critical(f"Stock reservation FAILED for {product_id}. Race condition? Stock level may have changed.")
                # This is a critical failure. We must roll back.
                # We would call self.db.rollback_transaction() here.
                # And we would need to un-reserve any items already updated.
                self.rollback_stock_reservation(items_list, order_id, item['product_id'])
                return False
                
        logger.info(f"Stock successfully reserved for all items in order {order_id}.")
        return True

    def rollback_stock_reservation(self, items_list, order_id, failed_at_product_id):
        """
        Helper function to roll back stock reservations if one item fails.
        """
        logger.warning(f"Rolling back stock reservation for order {order_id} due to failure at {failed_at_product_id}")
        
        # --- Interdependent SQL Query ---
        sql = """
        UPDATE products
        SET stock_level = stock_level + %s
        WHERE product_id = %s;
        """
        
        for item in items_list:
            if item['product_id'] == failed_at_product_id:
                # Don't roll back the one that failed (it never got decremented)
                break 
            
            logger.info(f"Restoring stock for {item['product_id']} by {item['quantity']}")
            self.db.execute_commit(sql, (item['quantity'], item['product_id']))
            
        logger.error(f"Stock reservation for order {order_id} has been rolled back.")

# --- Module: Order Processing ---

class OrderProcessor:
    """
    The main service class that orchestrates the entire order process.
    It depends on InventoryManager, NotificationService, and MockPaymentGateway.
    """
    
    def __init__(self, db_conn, inventory_manager, payment_gateway, notifier):
        self.db = db_conn
        self.inventory_manager = inventory_manager
        self.payment_gateway = payment_gateway
        self.notifier = notifier
        logger.info("Order Processor initialized. Ready to process orders.")

    def get_user_details(self, user_id):
        """Fetches user details from the database."""
        logger.info(f"Fetching details for user {user_id}")
        # --- Interdependent SQL Query ---
        sql = "SELECT user_id, email, first_name, last_name, default_payment_id FROM users WHERE user_id = %s;"
        result = self.db.execute_query(sql, (user_id,))
        if not result:
            logger.error(f"User {user_id} not found.")
            return None
        return result[0]

    def get_payment_details(self, payment_id):
        """Fetches payment method details."""
        logger.info(f"Fetching details for payment_id {payment_id}")
        # --- Interdependent SQL Query ---
        sql = "SELECT payment_id, provider, last_four, expiry_date FROM payment_methods WHERE payment_id = %s;"
        result = self.db.execute_query(sql, (payment_id,))
        if not result:
            logger.error(f"Payment method {payment_id} not found.")
            return None
        # This is a mock "token" for the gateway
        result[0]['token'] = f"token_mock_{result[0]['last_four']}"
        return result[0]
        
    def get_promotion_details(self, promo_code):
        """Fetches promotion details from the database."""
        if not promo_code:
            return None
        logger.info(f"Fetching details for promo code {promo_code}")
        # --- Interdependent SQL Query ---
        sql = "SELECT promo_code, discount_type, discount_value, min_spend FROM promotions WHERE promo_code = %s AND expiry_date > NOW();"
        result = self.db.execute_query(sql, (promo_code,))
        if not result:
            logger.warning(f"Promo code {promo_code} not valid or expired.")
            return None
        return result[0]

    def calculate_order_totals(self, items_with_details, promotion):
        """
        Calculates subtotal, discount, shipping, and total.
        
        Args:
            items_with_details (list): List of dicts, each enhanced by check_stock.
                                     e.g., {'product_id': ..., 'quantity': ..., 'unit_price': ...}
            promotion (dict): The promotion details, or None.
            
        Returns:
            dict: { 'subtotal': Decimal, 'discount_amount': Decimal, 'shipping_cost': Decimal, 'total_amount': Decimal }
        """
        logger.info("Calculating order totals...")
        
        subtotal = Decimal('0.00')
        for item in items_with_details:
            item['line_total'] = item['unit_price'] * item['quantity']
            subtotal += item['line_total']
            
        logger.info(f"Calculated subtotal: {subtotal}")
        
        discount_amount = Decimal('0.00')
        if promotion:
            min_spend = promotion.get('min_spend', Decimal('0.00'))
            if subtotal >= min_spend:
                promo_type = promotion['discount_type']
                promo_value = promotion['discount_value']
                
                if promo_type == 'PERCENTAGE':
                    discount_amount = (subtotal * (promo_value / Decimal('100.00'))).quantize(Decimal('0.01'))
                    logger.info(f"Applied {promo_value}% discount: -${discount_amount}")
                elif promo_type == 'FIXED':
                    discount_amount = promo_value
                    logger.info(f"Applied fixed discount: -${discount_amount}")
                
                # Ensure discount isn't more than subtotal
                discount_amount = min(subtotal, discount_amount)
            else:
                logger.warning(f"Promotion {promotion['promo_code']} not applied: minimum spend of {min_spend} not met.")
                
        # Mock shipping calculation
        shipping_cost = Decimal('9.99')
        if subtotal - discount_amount > Decimal('75.00'):
            shipping_cost = Decimal('0.00') # Free shipping over $75
            logger.info("Free shipping applied.")
        
        total_amount = (subtotal - discount_amount + shipping_cost).quantize(Decimal('0.01'))
        
        logger.info(f"Final totals: Subtotal=${subtotal}, Discount=${discount_amount}, Shipping=${shipping_cost}, Total=${total_amount}")
        
        return {
            'subtotal': subtotal,
            'discount_amount': discount_amount,
            'shipping_cost': shipping_cost,
            'total_amount': total_amount
        }
        
    def create_order_record(self, order_id, user_id, totals, items, shipping_address, payment_transaction_id):
        """
        Writes the final, confirmed order to the database.
        This is a highly interdependent function.
        
        Returns:
            bool: True on success, False on failure.
        """
        logger.info(f"Creating database record for order {order_id}")
        
        # --- Interdependent SQL Query ---
        order_sql = """
        INSERT INTO orders (
            order_id, user_id, status, total_amount, subtotal, 
            discount_amount, shipping_cost, shipping_address_json, 
            payment_transaction_id, order_date
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW());
        """
        
        order_params = (
            order_id,
            user_id,
            'PROCESSING', # Initial status
            totals['total_amount'],
            totals['subtotal'],
            totals['discount_amount'],
            totals['shipping_cost'],
            json.dumps(shipping_address),
            payment_transaction_id
        )
        
        if self.db.execute_commit(order_sql, order_params) == 0:
            logger.critical(f"Failed to INSERT master order record for {order_id}. This is a critical error.")
            return False
            
        # --- Interdependent SQL Query (Batch Insert) ---
        items_sql = """
        INSERT INTO order_items (order_id, product_id, quantity, unit_price, line_total)
        VALUES (%s, %s, %s, %s, %s);
        """
        
        # This part is often slow and should be a batch insert
        # We simulate it as a loop for clarity
        for item in items:
            item_params = (
                order_id,
                item['product_id'],
                item['quantity'],
                item['unit_price'],
                item['line_total']
            )
            if self.db.execute_commit(items_sql, item_params) == 0:
                 logger.critical(f"Failed to INSERT order_item {item['product_id']} for {order_id}.")
                 # In a real app, the transaction would roll back this entire function.
                 return False
                 
        logger.info(f"Successfully created all database records for order {order_id}")
        return True

    def process_new_order(self, user_id, items, shipping_address, promo_code=None, payment_id=None):
        """
        This is the main "god function" that coordinates everything.
        
        Args:
            user_id (str): The user placing the order.
            items (list): List of {'product_id': str, 'quantity': int}
            shipping_address (dict): Full shipping address.
            promo_code (str, optional): A promotion code.
            payment_id (str, optional): A specific payment ID. If None, use user's default.
            
        Returns:
            dict: {'success': bool, 'order_id': str, 'message': str}
        """
        logger.info(f"--- NEW ORDER RECEIVED --- User: {user_id}, Items: {len(items)}")
        
        # Start a database transaction
        self.db.begin_transaction()
        
        try:
            # --- 1. Get User and Payment Details ---
            user_details = self.get_user_details(user_id)
            if not user_details:
                raise ValueError(f"Invalid user_id: {user_id}")
                
            if payment_id is None:
                payment_id = user_details.get('default_payment_id')
                if not payment_id:
                    raise ValueError(f"User {user_id} has no default payment method.")
            
            payment_details = self.get_payment_details(payment_id)
            if not payment_details:
                raise ValueError(f"Invalid payment_id: {payment_id}")
            
            payment_token = payment_details['token'] # The token for the gateway
            
            # --- 2. Check Stock and Get Item Details ---
            # This is a complex, interdependent step
            logger.info("Validating stock and item details...")
            items_with_details = []
            for item in items:
                stock_check = self.inventory_manager.check_stock(item['product_id'], item['quantity'])
                if not stock_check['available']:
                    raise ValueError(f"Item {stock_check['product_name']} ({item['product_id']}) is out of stock.")
                
                # Enhance the item dict with price and name
                item_details = item.copy()
                item_details['unit_price'] = stock_check['unit_price']
                item_details['name'] = stock_check['product_name']
                items_with_details.append(item_details)

            logger.info("All items are in stock.")
            
            # --- 3. Calculate Totals & Apply Promotions ---
            promotion = self.get_promotion_details(promo_code)
            totals = self.calculate_order_totals(items_with_details, promotion)
            
            # --- 4. Charge Customer ---
            logger.info(f"Attempting to charge customer {totals['total_amount']}...")
            charge_response = self.payment_gateway.charge(
                amount=totals['total_amount'],
                payment_token=payment_token
            )
            
            if not charge_response['success']:
                raise Exception(f"Payment failed: {charge_response['error']}")
                
            transaction_id = charge_response['transaction_id']
            logger.info(f"Payment successful. Transaction: {transaction_id}")
            
            # --- 5. Reserve Stock (Critical Section) ---
            # This is the point of no return for the database transaction.
            # If this fails, we must refund the payment.
            new_order_id = f"ord_{uuid4()}"
            if not self.inventory_manager.reserve_stock(items_with_details, new_order_id):
                # This is a critical failure. Stock reservation failed after payment.
                raise Exception("Stock reservation failed post-payment. Critical error.")
            
            # --- 6. Create Order Record in Database ---
            if not self.create_order_record(
                order_id=new_order_id,
                user_id=user_id,
                totals=totals,
                items=items_with_details,
                shipping_address=shipping_address,
                payment_transaction_id=transaction_id
            ):
                raise Exception("Failed to write order record to database. Critical error.")
            
            # --- 7. Commit Transaction ---
            self.db.commit_transaction()
            
            # --- 8. Send Notifications (Post-Transaction) ---
            # This is done *after* the commit so we don't email for a failed order.
            order_details_for_email = totals.copy()
            order_details_for_email['order_id'] = new_order_id
            order_details_for_email['items'] = items_with_details
            
            self.notifier.send_order_confirmation_email(user_details, order_details_for_email)
            
            logger.info(f"--- ORDER {new_order_id} PROCESSED SUCCESSFULLY ---")
            return {'success': True, 'order_id': new_order_id, 'message': 'Order processed successfully.'}
            
        except Exception as e:
            # --- Handle All Errors ---
            logger.error(f"Order processing failed: {e}")
            
            # Roll back all database changes (stock, order records, etc.)
            self.db.rollback_transaction()
            logger.warning("Database transaction has been rolled back.")
            
            # If payment was charged but something *after* failed (e.g., stock reservation),
            # we MUST issue a refund.
            if 'transaction_id' in locals() and transaction_id:
                logger.critical(f"Issuing emergency refund for {transaction_id} due to post-payment failure.")
                refund_response = self.payment_gateway.refund(transaction_id, totals['total_amount'])
                if not refund_response['success']:
                    logger.critical(f"!!! EMERGENCY REFUND FAILED: {refund_response['error']} !!!")
                    # This requires manual intervention.
                    # Send an alert to the dev team.
                    self.notifier.send_email(
                        "devops@ecommerce.com", 
                        "CRITICAL: REFUND FAILED", 
                        f"Refund failed for {transaction_id}. Please investigate immediately.",
                        f"Refund failed for {transaction_id}. Please investigate immediately."
                    )
            
            return {'success': False, 'order_id': None, 'message': str(e)}

# --- Module: Reporting ---

class ReportingService:
    """
    Generates financial and operational reports.
    Depends on the database.
    """
    def __init__(self, db_conn):
        self.db = db_conn
        logger.info("Reporting Service initialized.")
        
    def generate_daily_sales_report(self, report_date):
        """
        Generates a summary of sales for a specific date.
        """
        logger.info(f"Generating sales report for {report_date}")
        
        # --- Interdependent SQL Query ---
        sql = """
        SELECT 
            order_id, 
            total_amount, 
            discount_amount, 
            shipping_cost,
            (total_amount - shipping_cost + discount_amount) AS revenue,
            status
        FROM 
            orders
        WHERE 
            status = 'COMPLETED' AND DATE(order_date) = %s;
        """
        
        results = self.db.execute_query(sql, (report_date,))
        
        if not results:
            logger.warning(f"No completed orders found for {report_date}.")
            return {'total_sales': 0, 'total_revenue': 0, 'order_count': 0, 'orders': []}
            
        total_sales = Decimal('0.00')
        total_revenue = Decimal('0.00')
        order_count = len(results)
        
        for order in results:
            total_sales += order['total_amount']
            total_revenue += order['revenue'] # Mock revenue calculation
            
        logger.info(f"Report for {report_date}: Orders={order_count}, Total Sales=${total_sales}, Total Revenue=${total_revenue}")
        
        return {
            'report_date': report_date,
            'total_sales': total_sales,
            'total_revenue': total_revenue,
            'order_count': order_count,
            'orders': results
        }
        
    def generate_low_stock_report(self, threshold=10):
        """
        Generates a report of all products with stock below a threshold.
        """
        logger.info(f"Generating low stock report (threshold: {threshold})")
        
        # --- Interdependent SQL Query ---
        sql = """
        SELECT 
            p.product_id,
            p.name,
            p.stock_level,
            s.supplier_name,
            s.supplier_contact_email
        FROM 
            products p
        LEFT JOIN 
            suppliers s ON p.supplier_id = s.supplier_id
        WHERE 
            p.stock_level < %s AND p.is_active = TRUE
        ORDER BY
            p.stock_level ASC;
        """
        
        low_stock_items = self.db.execute_query(sql, (threshold,))
        
        if not low_stock_items:
            logger.info("No items are low on stock. Good job!")
            return {'count': 0, 'items': []}
            
        logger.warning(f"Found {len(low_stock_items)} items low on stock.")
        
        # We could auto-email this report
        # self.notifier.send_email(...)
        
        return {
            'threshold': threshold,
            'count': len(low_stock_items),
            'items': low_stock_items
        }

# --- Main Application Execution ---

def main_simulation():
    """
    A main function to simulate the service's operation.
    This demonstrates how the interdependent classes are wired together.
    """
    logger.info("--- E-COMMERCE SERVICE SIMULATION START ---")
    
    # --- 1. Initialization (Dependency Injection) ---
    db_conn = get_db_connection()
    inventory_mgr = InventoryManager(db_conn)
    payment_gw = MockPaymentGateway(api_key="pk_live_mock_key")
    notifier_svc = NotificationService()
    
    order_processor = OrderProcessor(
        db_conn=db_conn,
        inventory_manager=inventory_mgr,
        payment_gateway=payment_gw,
        notifier=notifier_svc
    )
    
    reporting_svc = ReportingService(db_conn)
    
    logger.info("All services initialized.")
    
    # --- 2. Simulate a Successful Order ---
    logger.info("\n--- SIMULATION 1: Successful Order ---")
    
    # This is the "payload" that would come from a web API
    order_payload_1 = {
        'user_id': 'user_12345',
        'items': [
            {'product_id': 'prod_abc', 'quantity': 2},
            {'product_id': 'prod_xyz', 'quantity': 1}
        ],
        'shipping_address': {
            'name': 'John Doe',
            'street': '123 Main St',
            'city': 'Anytown',
            'state': 'CA',
            'zip': '12345'
        },
        'promo_code': 'WINTER10' # This is the 10% off code
    }
    
    result_1 = order_processor.process_new_order(**order_payload_1)
    print(f"Simulation 1 Result: {result_1}")
    
    # --- 3. Simulate a Failed Order (Insufficient Stock) ---
    logger.info("\n--- SIMULATION 2: Failed Order (Insufficient Stock) ---")
    
    order_payload_2 = {
        'user_id': 'user_67890',
        'items': [
            {'product_id': 'prod_abc', 'quantity': 9999} # This will fail
        ],
        'shipping_address': { 'name': 'Jane Smith', 'street': '456 Oak Ave', 'city': 'Otherville', 'state': 'NY', 'zip': '67890' },
        'promo_code': None
    }
    
    result_2 = order_processor.process_new_order(**order_payload_2)
    print(f"Simulation 2 Result: {result_2}")
    
    # --- 4. Simulate a Failed Order (Payment Declined) ---
    logger.info("\n--- SIMULATION 3: Failed Order (Payment Declined) ---")
    
    # We need to get a user and payment method that *will* fail
    # We'll cheat and just set the payment_id directly to a known failing token
    
    order_payload_3 = {
        'user_id': 'user_12345',
        'items': [{'product_id': 'prod_abc', 'quantity': 1}],
        'shipping_address': { 'name': 'John Doe', 'street': '123 Main St', 'city': 'Anytown', 'state': 'CA', 'zip': '12345' },
        'payment_id': 'payment_token_fail_card_declined' # This will fail
    }
    
    result_3 = order_processor.process_new_order(**order_payload_3)
    print(f"Simulation 3 Result: {result_3}")
    
    # --- 5. Simulate Generating Reports ---
    logger.info("\n--- SIMULATION 4: Generate Reports ---")
    
    today = datetime.date.today().isoformat()
    sales_report = reporting_svc.generate_daily_sales_report(today)
    print(f"Sales Report for {today}:")
    # Using json.dumps for pretty printing the decimal-containing dict
    print(json.dumps(sales_report, indent=2, default=str)) 
    
    low_stock_report = reporting_svc.generate_low_stock_report(threshold=50)
    print("\nLow Stock Report (Threshold < 50):")
    print(json.dumps(low_stock_report, indent=2, default=str))
    
    # --- 6. Shutdown ---
    db_conn.close()
    logger.info("--- E-COMMERCE SERVICE SIMULATION END ---")

# --- Standard Python Entry Point ---
if __name__ == "__main__":
    # This block runs when the script is executed directly
    main_simulation()