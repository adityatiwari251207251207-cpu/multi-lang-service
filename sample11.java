import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ===================================================================================
 * A Monolithic E-Commerce Management System
 * ===================================================================================
 * This single, massive Java file contains all logic for an e-commerce backend.
 * It combines Database, Inventory, User, Order, and Reporting services into
 * one class to demonstrate complex, large-scale code structure.
 *
 * This file demonstrates:
 * 1.  **Method Interdependency:** Complex methods (like placeOrder) calling numerous
 * other helper methods (like getStockLevel, updateStock, createOrderRecord).
 * 2.  **SQL Interdependency:** A complex schema where tables depend on each
 * other, and transactional logic (in placeOrder) that performs
 * interdependent SQL operations (UPDATE, INSERT, INSERT) that must
 * all succeed or fail together.
 * 3.  **Large Scale:** Expanded functionality to reach a significant line count,
 * including detailed error handling, logging, and data modeling.
 *
 * This system is designed to run against an SQLite database file.
 *
 * @author Gemini AI
 * @version 1.0
 * ===================================================================================
 */
public class MonolithicECommerce {

    // --- 1. CONFIGURATION AND LOGGING ---

    /**
     * The connection string for the SQLite database.
     * This defines the single source of truth for the database file.
     */
    private static final String DB_URL = "jdbc:sqlite:monolithic_ecommerce.db";

    /**
     * A shared logger for the entire application.
     * Used for detailed logging of operations, warnings, and errors.
     */
    private static final Logger LOGGER = Logger.getLogger(MonolithicECommerce.class.getName());

    /**
     * Static block to configure the logger.
     * This ensures logging is set up before any operations are performed.
     */
    static {
        // In a real app, you'd configure a logging properties file.
        // Here, we just set the level.
        LOGGER.setLevel(Level.INFO);
        LOGGER.info("MonolithicECommerce class loaded. Logging initialized.");
    }

    // --- 2. DATABASE SERVICE LAYER (Embedded) ---

    /**
     * Establishes and returns a connection to the SQLite database.
     * This method is the foundation for ALL database operations.
     * Every data-access method depends on this.
     *
     * @return A new, open Connection object.
     * @throws SQLException if a database access error occurs or the driver is not found.
     */
    public Connection connect() throws SQLException {
        Connection conn = null;
        try {
            // Ensure the JDBC driver is available
            Class.forName("org.sqlite.JDBC");
            conn = DriverManager.getConnection(DB_URL);
            LOGGER.log(Level.FINER, "Database connection established.");
        } catch (ClassNotFoundException e) {
            LOGGER.log(Level.SEVERE, "SQLite JDBC driver not found.", e);
            throw new SQLException("DB driver not found", e);
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Failed to establish database connection.", e);
            throw e; // Re-throw the exception to be handled by the caller
        }
        return conn;
    }

    /**
     * Initializes the entire database schema.
     * This method is critically important and must be run once.
     * The application is entirely dependent on this schema.
     *
     * **SQL INTERDEPENDENCY:**
     * - 'inventory' depends on 'products'
     * - 'orders' depends on 'users'
     * - 'order_items' depends on 'orders' and 'products'
     *
     * The execution order of these CREATE statements is mandatory.
     */
    public void initializeSchema() {
        LOGGER.info("Initializing database schema...");

        // 1. Users Table: The root entity for customers.
        String createUserTable = """
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            username TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """;
        
        // 2. Products Table: The catalog of items for sale.
        String createProductTable = """
        CREATE TABLE IF NOT EXISTS products (
            product_id TEXT PRIMARY KEY,
            sku TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            description TEXT,
            price REAL NOT NULL CHECK(price > 0),
            category TEXT
        );
        """;
        
        // 3. Inventory Table: Tracks stock levels.
        //    DEPENDS ON: 'products' table (FOREIGN KEY product_id)
        String createInventoryTable = """
        CREATE TABLE IF NOT EXISTS inventory (
            product_id TEXT PRIMARY KEY,
            quantity INTEGER NOT NULL CHECK(quantity >= 0),
            reserved_quantity INTEGER NOT NULL DEFAULT 0 CHECK(reserved_quantity >= 0),
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (product_id) REFERENCES products (product_id) ON DELETE CASCADE
        );
        """;
        
        // 4. Orders Table: Header record for a customer purchase.
        //    DEPENDS ON: 'users' table (FOREIGN KEY user_id)
        String createOrderTable = """
        CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDING' 
                CHECK(status IN ('PENDING', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED')),
            total_amount REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE RESTRICT
        );
        """;
        
        // 5. Order Items Table: Line items for an order.
        //    DEPENDS ON: 'orders' (order_id) AND 'products' (product_id)
        String createOrderItemsTable = """
        CREATE TABLE IF NOT EXISTS order_items (
            order_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT NOT NULL,
            product_id TEXT, -- Can be NULL if product is deleted
            quantity INTEGER NOT NULL CHECK(quantity > 0),
            price_at_purchase REAL NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders (order_id) ON DELETE CASCADE,
            FOREIGN KEY (product_id) REFERENCES products (product_id) ON DELETE SET NULL
        );
        """;
        
        // 6. Audit Log Table: For tracking critical changes.
        String createAuditLogTable = """
        CREATE TABLE IF NOT EXISTS audit_log (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            entity_type TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            action TEXT NOT NULL,
            details TEXT
        );
        """;

        // We use a single connection and statement to execute all schema queries.
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // Execute schema creation in dependent order
            LOGGER.finer("Creating 'users' table...");
            stmt.execute(createUserTable);
            
            LOGGER.finer("Creating 'products' table...");
            stmt.execute(createProductTable);
            
            LOGGER.finer("Creating 'inventory' table...");
            stmt.execute(createInventoryTable);
            
            LOGGER.finer("Creating 'orders' table...");
            stmt.execute(createOrderTable);
            
            LOGGER.finer("Creating 'order_items' table...");
            stmt.execute(createOrderItemsTable);
            
            LOGGER.finer("Creating 'audit_log' table...");
            stmt.execute(createAuditLogTable);
            
            LOGGER.info("Database schema initialized successfully.");
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Failed to initialize database schema. This is a fatal error.", e);
            // This would be a fatal error in a real application
            throw new RuntimeException("Database schema initialization failed", e);
        }
    }

    /**
     * A private helper method to log actions to the audit table.
     * This demonstrates an internal dependency.
     *
     * @param entityType e.g., "PRODUCT", "ORDER", "INVENTORY"
     * @param entityId The ID of the entity being changed
     * @param action e.g., "CREATE", "UPDATE_STOCK", "PLACE_ORDER"
     * @param details A JSON-like string of details
     */
    private void auditLog(String entityType, String entityId, String action, String details) {
        String sql = "INSERT INTO audit_log (entity_type, entity_id, action, details) VALUES (?, ?, ?, ?)";
        try (Connection conn = connect(); PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, entityType);
            pstmt.setString(2, entityId);
            pstmt.setString(3, action);
            pstmt.setString(4, details);
            pstmt.executeUpdate();
            LOGGER.finer(String.format("Audit log created for %s %s: %s", entityType, entityId, action));
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Failed to write to audit log!", e);
            // This is critical. In a real system, this might trigger an alert.
        }
    }
    

    // --- 3. USER SERVICE LAYER (Embedded) ---

    /**
     * Creates a new user in the system.
     *
     * @param userId The unique ID for the user.
     * @param email The user's email, must be unique.
     * @param username The user's display name.
     * @return true if the user was created, false if an error occurred (e.g., duplicate email).
     */
    public boolean createUser(String userId, String email, String username) {
        LOGGER.info("Attempting to create user: " + username);
        String sql = "INSERT INTO users (user_id, email, username) VALUES (?, ?, ?)";
        
        try (Connection conn = connect(); PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, userId);
            pstmt.setString(2, email);
            pstmt.setString(3, username);
            int rowsAffected = pstmt.executeUpdate();
            
            if (rowsAffected > 0) {
                LOGGER.info("Successfully created user: " + username);
                // METHOD INTERDEPENDENCY: Call audit log
                auditLog("USER", userId, "CREATE", "Email: " + email);
                return true;
            } else {
                // This case should not be hit if executeUpdate() is working, but good to have.
                LOGGER.warning("User creation for " + username + " affected 0 rows.");
                return false;
            }
        } catch (SQLException e) {
            if (e.getMessage().contains("UNIQUE constraint failed: users.email")) {
                LOGGER.warning("Failed to create user: Email already exists - " + email);
            } else {
                LOGGER.log(Level.SEVERE, "Error creating user " + username, e);
            }
            return false;
        }
    }

    /**
     * Retrieves a user's details by their user_id.
     *
     * @param userId The ID of the user to find.
     * @return A Map containing the user's data, or null if not found.
     */
    public Map<String, Object> getUserDetails(String userId) {
        LOGGER.finer("Fetching details for user: " + userId);
        String sql = "SELECT * FROM users WHERE user_id = ?";
        
        try (Connection conn = connect(); PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, userId);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    // Create a map to hold the user data
                    Map<String, Object> user = new HashMap<>();
                    user.put("user_id", rs.getString("user_id"));
                    user.put("email", rs.getString("email"));
                    user.put("username", rs.getString("username"));
                    user.put("created_at", rs.getTimestamp("created_at"));
                    LOGGER.finer("Found user: " + userId);
                    return user;
                } else {
                    LOGGER.info("No user found with ID: " + userId);
                    return null;
                }
            }
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error fetching user details for " + userId, e);
            return null;
        }
    }
    
    
    // --- 4. INVENTORY SERVICE LAYER (Embedded) ---

    /**
     * Adds a new product and sets its initial inventory.
     * This method must write to two tables, 'products' and 'inventory'.
     *
     * **SQL INTERDEPENDENCY (TRANSACTIONAL):**
     * This operation *should* be transactional. A failure after the first
     * INSERT would leave orphaned data (a product with no inventory record).
     * This implementation uses a manual transaction.
     *
     * @param productId The unique ID for the product.
     * @param sku The stock-keeping unit (SKU).
     * @param name The name of the product.
     * @param price The product's price.
     * @param initialQuantity The starting quantity.
     * @return true if successful, false otherwise.
     */
    public boolean addNewProduct(String productId, String sku, String name, double price, int initialQuantity) {
        LOGGER.info("Attempting to add new product: " + name);
        
        // SQL statements for the transaction
        String productSQL = "INSERT INTO products (product_id, sku, name, price) VALUES (?, ?, ?, ?)";
        String inventorySQL = "INSERT INTO inventory (product_id, quantity) VALUES (?, ?)";
        
        Connection conn = null;
        try {
            // METHOD INTERDEPENDENCY: Get connection
            conn = connect();
            // Start Transaction
            conn.setAutoCommit(false); 
            
            // 1. Insert into 'products' table
            try (PreparedStatement pstmtProduct = conn.prepareStatement(productSQL)) {
                pstmtProduct.setString(1, productId);
                pstmtProduct.setString(2, sku);
                pstmtProduct.setString(3, name);
                pstmtProduct.setDouble(4, price);
                pstmtProduct.executeUpdate();
            }
            
            // 2. Insert into 'inventory' table
            try (PreparedStatement pstmtInventory = conn.prepareStatement(inventorySQL)) {
                pstmtInventory.setString(1, productId);
                pstmtInventory.setInt(2, initialQuantity);
                pstmtInventory.executeUpdate();
            }
            
            // If both succeed, commit the transaction
            conn.commit(); 
            
            LOGGER.info(String.format("Successfully added product %s (%s) with quantity %d", name, productId, initialQuantity));
            // METHOD INTERDEPENDENCY: Call audit log
            auditLog("PRODUCT", productId, "CREATE", "Name: " + name + ", Qty: " + initialQuantity);
            return true;
            
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Failed to add new product: " + name, e);
            // Rollback in case of error
            if (conn != null) {
                try {
                    conn.rollback();
                    LOGGER.warning("Transaction rolled back for product: " + name);
                } catch (SQLException ex) {
                    LOGGER.log(Level.SEVERE, "Critical error: Failed to roll back transaction.", ex);
                }
            }
            return false;
        } finally {
            // Always close connection and restore auto-commit
            if (conn != null) {
                try {
                    conn.setAutoCommit(true);
                    conn.close();
                } catch (SQLException e) {
                    LOGGER.log(Level.WARNING, "Failed to close connection.", e);
                }
            }
        }
    }

    /**
     * Checks the current *available* stock level for a product.
     * Available = physical quantity - reserved quantity
     *
     * @param productId The product to check.
     * @return The available quantity. -1 if the product doesn't exist.
     */
    public int getAvailableStockLevel(String productId) {
        LOGGER.finer("Checking available stock for product: " + productId);
        String sql = "SELECT (quantity - reserved_quantity) AS available FROM inventory WHERE product_id = ?";
        
        try (Connection conn = connect(); PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, productId);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    int availableStock = rs.getInt("available");
                    LOGGER.finer(String.format("Stock for %s: %d available", productId, availableStock));
                    return availableStock;
                } else {
                    LOGGER.warning("No inventory record found for product: " + productId);
                    return -1; // Indicates product not found in inventory
                }
            }
        } catch (SQLException e) {
            LOGGER.log(Level.WARNING, "Error checking stock for " + productId, e);
            return -1;
        }
    }

    /**
     * Attempts to atomically update the physical stock for a product.
     * This uses a conditional UPDATE to prevent overselling.
     * This is the core of inventory-safe logic.
     *
     * @param productId The product to update.
     * @param quantityChange The amount to add/remove (e.g., -5 to remove 5).
     * @return true if the stock was successfully updated, false otherwise (e.g., insufficient stock).
     */
    public boolean updatePhysicalStock(String productId, int quantityChange) {
        LOGGER.info(String.format("Updating physical stock for %s by %d", productId, quantityChange));
        
        // **COMPLEX & INTERDEPENDENT SQL:**
        // This atomic query is the heart of safe inventory management.
        // It updates the quantity ONLY IF the resulting quantity is not negative.
        // (quantity + ?) >= 0
        // If current quantity is 3 and change is -5, (3 + -5) is -2.
        // The UPDATE will affect 0 rows, and we know the update failed.
        String sql = """
        UPDATE inventory
        SET 
            quantity = quantity + ?,
            last_updated = CURRENT_TIMESTAMP
        WHERE 
            product_id = ? AND (quantity + ?) >= 0;
        """;
        
        try (Connection conn = connect(); PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, quantityChange);
            pstmt.setString(2, productId);
            pstmt.setInt(3, quantityChange);
            
            int rowsAffected = pstmt.executeUpdate();
            
            if (rowsAffected > 0) {
                LOGGER.info("Stock for " + productId + " updated successfully.");
                // METHOD INTERDEPENDENCY: Call audit log
                auditLog("INVENTORY", productId, "UPDATE_STOCK", "Change: " + quantityChange);
                return true;
            } else {
                // This means the WHERE condition failed (insufficient stock)
                LOGGER.warning("Stock update for " + productId + " failed (insufficient stock or product not found).");
                return false;
            }
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error updating stock for " + productId, e);
            return false;
        }
    }
    
    /**
     * Reserves or un-reserves stock for a product.
     * This is used during the order process.
     *
     * @param productId The product to reserve.
     * @param quantityToReserve The amount to reserve (positive) or un-reserve (negative).
     * @return true if successful, false if not (e.g., not enough available stock to reserve).
     */
    public boolean reserveStock(String productId, int quantityToReserve) {
        LOGGER.info(String.format("Attempting to reserve %d for product %s", quantityToReserve, productId));

        // **COMPLEX & INTERDEPENDENT SQL:**
        // This query updates the 'reserved_quantity' only if
        // the available stock (quantity - reserved_quantity) is
        // greater than or equal to the amount we are *trying* to reserve.
        String sql = """
        UPDATE inventory
        SET
            reserved_quantity = reserved_quantity + ?
        WHERE
            product_id = ? AND (quantity - reserved_quantity) >= ?;
        """;
        
        // For un-reserving (negative quantityToReserve), we just do it.
        String unreserveSql = """
        UPDATE inventory
        SET
            reserved_quantity = reserved_quantity + ?
        WHERE
            product_id = ?;
        """;
        
        // Choose the correct SQL based on the operation
        String finalSql = (quantityToReserve >= 0) ? sql : unreserveSql;

        try (Connection conn = connect(); PreparedStatement pstmt = conn.prepareStatement(finalSql)) {
            pstmt.setInt(1, quantityToReserve);
            pstmt.setString(2, productId);
            if (quantityToReserve >= 0) {
                pstmt.setInt(3, quantityToReserve); // Set the third param only for the reserve query
            }
            
            int rowsAffected = pstmt.executeUpdate();
            
            if (rowsAffected > 0) {
                LOGGER.info(String.format("Successfully reserved/unreserved %d for %s", quantityToReserve, productId));
                auditLog("INVENTORY", productId, "RESERVE_STOCK", "Change: " + quantityToReserve);
                return true;
            } else {
                LOGGER.warning(String.format("Failed to reserve %d for %s: Insufficient available stock.", quantityToReserve, productId));
                return false;
            }
            
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error reserving stock for " + productId, e);
            return false;
        }
    }

    /**
     * Retrieves product details, including current stock.
     *
     * **SQL INTERDEPENDENCY (JOIN):**
     * This query is dependent on a JOIN between 'products' and 'inventory' tables.
     *
     * @param productId The ID of the product to fetch.
     * @return A Map of product data, or null if not found.
     */
    public Map<String, Object> getProductDetails(String productId) {
        LOGGER.finer("Fetching details for product: " + productId);
        String sql = """
        SELECT
            p.product_id, p.sku, p.name, p.description, p.price,
            i.quantity AS physical_stock,
            i.reserved_quantity,
            (i.quantity - i.reserved_quantity) AS available_stock,
            i.last_updated
        FROM products AS p
        JOIN inventory AS i ON p.product_id = i.product_id
        WHERE p.product_id = ?;
        """;

        try (Connection conn = connect(); PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, productId);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    Map<String, Object> product = new HashMap<>();
                    product.put("product_id", rs.getString("product_id"));
                    product.put("sku", rs.getString("sku"));
                    product.put("name", rs.getString("name"));
                    product.put("description", rs.getString("description"));
                    product.put("price", rs.getDouble("price"));
                    product.put("physical_stock", rs.getInt("physical_stock"));
                    product.put("reserved_quantity", rs.getInt("reserved_quantity"));
                    product.put("available_stock", rs.getInt("available_stock"));
                    product.put("last_updated", rs.getTimestamp("last_updated"));
                    
                    LOGGER.finer("Found product details: " + productId);
                    return product;
                } else {
                    LOGGER.warning("No product or inventory record found for: " + productId);
                    return null;
                }
            }
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error fetching product details for " + productId, e);
            return null;
        }
    }


    // --- 5. ORDER SERVICE LAYER (Embedded) ---

    /**
     * The most complex method in the system: places an order for a user.
     * This function demonstrates high-level functional and SQL interdependency.
     *
     * **FUNCTIONAL INTERDEPENDENCY:**
     * 1. This method *first* calls `getProductDetails()` to get prices.
     * 2. It *then* calls `reserveStock()` for each item to "hold" it.
     * 3. It *then* calls `connect()` to start a manual transaction.
     * 4. It *then* calls `updatePhysicalStock()` to finalize the sale.
     * 5. It *then* calls `auditLog()` multiple times.
     *
     * **SQL INTERDEPENDENCY (TRANSACTIONAL):**
     * This method performs several SQL operations that MUST all succeed or fail together.
     * A. `UPDATE inventory` (to reserve stock, via `reserveStock()`)
     * B. `INSERT INTO orders` (native SQL, within transaction)
     * C. `INSERT INTO order_items` (native SQL, within transaction)
     * D. `UPDATE inventory` (to reduce physical stock, via `updatePhysicalStock()`)
     *
     * This method manually controls a transaction to ensure data integrity
     * across the 'orders', 'order_items', and 'inventory' tables.
     *
     * @param userId The user placing the order.
     * @param cart A map of {product_id, quantity}
     * @return The new Order ID if successful, or null if failed.
     */
    public String placeOrder(String userId, Map<String, Integer> cart) {
        String newOrderId = "ORD-" + UUID.randomUUID().toString().substring(0, 12);
        LOGGER.info(String.format("Attempting to place order %s for user %s", newOrderId, userId));
        
        if (cart == null || cart.isEmpty()) {
            LOGGER.warning("Cart is empty. Order cannot be placed.");
            return null;
        }

        // --- 1. Price-Check and Reservation Phase ---
        // We must first "reserve" the stock for all items.
        // If any item fails, we must roll back ALL reservations.
        
        double calculatedTotalAmount = 0.0;
        Map<String, Double> pricesAtPurchase = new HashMap<>();
        List<String> successfullyReservedProducts = new ArrayList<>();

        try {
            for (Map.Entry<String, Integer> item : cart.entrySet()) {
                String productId = item.getKey();
                int requiredQuantity = item.getValue();
                
                // FUNCTIONAL DEPENDENCY: Get product price and check stock in one go
                Map<String, Object> productDetails = getProductDetails(productId);
                
                if (productDetails == null) {
                    throw new Exception("Product not found: " + productId);
                }
                
                int availableStock = (int) productDetails.get("available_stock");
                if (availableStock < requiredQuantity) {
                    throw new Exception(String.format("Insufficient stock for %s: need %d, have %d",
                        productId, requiredQuantity, availableStock));
                }
                
                // Store price for later
                double price = (double) productDetails.get("price");
                pricesAtPurchase.put(productId, price);
                calculatedTotalAmount += (price * requiredQuantity);
                
                // FUNCTIONAL DEPENDENCY: Reserve the stock
                if (reserveStock(productId, requiredQuantity)) {
                    successfullyReservedProducts.add(productId);
                } else {
                    // This is the critical failure point
                    throw new Exception("Failed to reserve stock for " + productId + " (race condition)");
                }
            }
            
            // If we are here, all stock is successfully reserved.
            LOGGER.info("All items successfully reserved for order " + newOrderId);
            
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Order " + newOrderId + " failed during reservation phase: " + e.getMessage());
            // **ROLLBACK LOGIC (Functional Interdependency)**
            // We must "un-reserve" any stock we successfully reserved.
            for (String productId : successfullyReservedProducts) {
                int quantityToUnreserve = cart.get(productId) * -1; // Negative value
                // FUNCTIONAL DEPENDENCY
                reserveStock(productId, quantityToUnreserve); 
            }
            LOGGER.warning("All reservations for " + newOrderId + " have been rolled back.");
            return null;
        }

        // --- 2. Transactional Phase: Create Order and Finalize Stock ---
        // Now that stock is reserved, we can create the order records
        // and convert reservations to actual stock reduction.
        
        Connection conn = null;
        try {
            // FUNCTIONAL DEPENDENCY: Get raw connection
            conn = connect();
            conn.setAutoCommit(false); // START TRANSACTION
            
            LOGGER.info("Started main order transaction for " + newOrderId);

            // --- 2a. Create the Order Record (SQL Dependency 1) ---
            String orderSQL = "INSERT INTO orders (order_id, user_id, status, total_amount) VALUES (?, ?, 'PROCESSING', ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(orderSQL)) {
                pstmt.setString(1, newOrderId);
                pstmt.setString(2, userId);
                pstmt.setDouble(3, calculatedTotalAmount);
                pstmt.executeUpdate();
            }
            LOGGER.finer("Order record created: " + newOrderId);

            // --- 2b. Create the Order Item Records (SQL Dependency 2) ---
            // This INSERT depends on the 'orders' INSERT above.
            String itemsSQL = "INSERT INTO order_items (order_id, product_id, quantity, price_at_purchase) VALUES (?, ?, ?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(itemsSQL)) {
                for (Map.Entry<String, Integer> item : cart.entrySet()) {
                    pstmt.setString(1, newOrderId);
                    pstmt.setString(2, item.getKey());
                    pstmt.setInt(3, item.getValue());
                    pstmt.setDouble(4, pricesAtPurchase.get(item.getKey()));
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
            LOGGER.finer("Order item records created for: " + newOrderId);

            // --- 2c. Finalize Inventory (SQL Dependency 3) ---
            // Atomically convert "reserved" stock into "sold" stock.
            // This means:
            // 1. physical quantity = physical quantity - sold_quantity
            // 2. reserved quantity = reserved quantity - sold_quantity
            String stockFinalizeSql = """
            UPDATE inventory
            SET
                quantity = quantity - ?,
                reserved_quantity = reserved_quantity - ?
            WHERE
                product_id = ?;
            """;
            
            try (PreparedStatement pstmt = conn.prepareStatement(stockFinalizeSql)) {
                for (Map.Entry<String, Integer> item : cart.entrySet()) {
                    int quantity = item.getValue();
                    pstmt.setInt(1, quantity);
                    pstmt.setInt(2, quantity);
                    pstmt.setString(3, item.getKey());
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
            LOGGER.finer("Inventory finalized for: " + newOrderId);
            
            // --- 3. Commit Phase ---
            // All interdependent SQL statements were successful.
            conn.commit(); // COMMIT TRANSACTION
            
            LOGGER.info(String.format("Successfully placed order %s with total %.2f", newOrderId, calculatedTotalAmount));
            // METHOD INTERDEPENDENCY: Call audit log
            auditLog("ORDER", newOrderId, "PLACE_ORDER", "User: " + userId + ", Total: " + calculatedTotalAmount);
            return newOrderId;

        } catch (SQLException e) {
            // --- 4. Rollback Phase ---
            LOGGER.log(Level.SEVERE, "Transaction for order " + newOrderId + " failed. Rolling back.", e);
            try {
                if (conn != null) {
                    conn.rollback(); // ROLLBACK TRANSACTION
                    LOGGER.warning("Database transaction rolled back for order " + newOrderId);
                }
            } catch (SQLException rollbackEx) {
                LOGGER.log(Level.SEVERE, "Critical error: Failed to roll back transaction.", rollbackEx);
            }
            
            // **CRITICAL FAILURE:**
            // The database transaction rolled back, but the stock is *still reserved*
            // from Phase 1. We must now un-reserve it.
            // This shows a complex dependency between different failure/recovery modes.
            for (String productId : successfullyReservedProducts) {
                int quantityToUnreserve = cart.get(productId) * -1; // Negative value
                // FUNCTIONAL DEPENDENCY
                reserveStock(productId, quantityToUnreserve); 
            }
            LOGGER.severe("Reservation for " + newOrderId + " has been rolled back due to DB failure.");
            return null;
        } finally {
            try {
                if (conn != null) {
                    conn.setAutoCommit(true);
                    conn.close();
                }
            } catch (SQLException finalEx) {
                LOGGER.log(Level.WARNING, "Error closing connection.", finalEx);
            }
        }
    }

    /**
     * Retrieves the full details of a specific order.
     *
     * **SQL INTERDEPENDENCY (JOIN):**
     * This method is dependent on JOINs across 'orders', 'users',
     * 'order_items', and 'products'.
     *
     * @param orderId The ID of the order to fetch.
     * @return A Map containing nested order data, or null.
     */
    public Map<String, Object> getOrderDetails(String orderId) {
        LOGGER.finer("Fetching details for order: " + orderId);
        
        Map<String, Object> orderDetails = new HashMap<>();
        
        // SQL 1: Get Order Header
        String orderSql = """
        SELECT o.order_id, o.status, o.total_amount, o.created_at,
               u.user_id, u.email, u.username
        FROM orders AS o
        JOIN users AS u ON o.user_id = u.user_id
        WHERE o.order_id = ?;
        """;
        
        try (Connection conn = connect(); PreparedStatement pstmt = conn.prepareStatement(orderSql)) {
            pstmt.setString(1, orderId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    orderDetails.put("order_id", rs.getString("order_id"));
                    orderDetails.put("status", rs.getString("status"));
                    orderDetails.put("total_amount", rs.getDouble("total_amount"));
                    orderDetails.put("created_at", rs.getTimestamp("created_at"));
                    
                    Map<String, Object> user = new HashMap<>();
                    user.put("user_id", rs.getString("user_id"));
                    user.put("email", rs.getString("email"));
                    user.put("username", rs.getString("username"));
                    orderDetails.put("user", user);
                } else {
                    LOGGER.warning("No order found with ID: " + orderId);
                    return null;
                }
            }
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error fetching order header for " + orderId, e);
            return null;
        }

        // SQL 2: Get Order Line Items
        // This query demonstrates a LEFT JOIN, in case a product
        // has been deleted from the catalog.
        String itemsSql = """
        SELECT
            oi.quantity, oi.price_at_purchase,
            p.product_id, p.sku, p.name AS product_name
        FROM order_items AS oi
        LEFT JOIN products AS p ON oi.product_id = p.product_id
        WHERE oi.order_id = ?
        ORDER BY p.name;
        """;
        
        List<Map<String, Object>> items = new ArrayList<>();
        try (Connection conn = connect(); PreparedStatement pstmt = conn.prepareStatement(itemsSql)) {
            pstmt.setString(1, orderId);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> item = new HashMap<>();
                    item.put("product_id", rs.getString("product_id"));
                    item.put("sku", rs.getString("sku"));
                    item.put("product_name", rs.getString("product_name"));
                    item.put("quantity", rs.getInt("quantity"));
                    item.put("price_at_purchase", rs.getDouble("price_at_purchase"));
                    items.add(item);
                }
            }
            orderDetails.put("items", items);
            
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error fetching order items for " + orderId, e);
            return null;
        }

        LOGGER.finer("Successfully fetched all details for order: " + orderId);
        return orderDetails;
    }


    // --- 6. REPORTING SERVICE LAYER (Embedded) ---

    /**
     * Generates a complex dashboard report with multiple interdependent metrics.
     *
     * **FUNCTIONAL INTERDEPENDENCY:**
     * This method is an orchestrator that calls multiple private
     * SQL-executing helper methods and combines their results.
     *
     * @return A Map containing various sales and inventory reports.
     */
    public Map<String, Object> generateDashboardReport() {
        LOGGER.info("Generating dashboard report...");
        Map<String, Object> report = new HashMap<>();

        try {
            // --- Interdependent Calls ---
            
            // 1. Get Sales Overview
            //    METHOD INTERDEPENDENCY
            report.put("sales_overview", getSalesReport(30));
            
            // 2. Get Top Selling Products
            //    METHOD INTERDEPENDENCY
            report.put("top_selling_products", getTopSellingProducts(30, 5));
            
            // 3. Get Low Stock Warning
            //    METHOD INTERDEPENDENCY
            report.put("low_stock_items", getLowStockItems(10));
            
            // 4. Get Latest Orders
            //    METHOD INTERDEPENDENCY
            report.put("latest_orders", getLatestOrders(5));

            LOGGER.info("Dashboard report generated successfully.");
            return report;
            
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to generate dashboard report.", e);
            return null;
        }
    }

    /**
     * Private helper for generateDashboardReport.
     *
     * **SQL INTERDEPENDENCY (AGGREGATION):**
     * This is a time-windowed aggregate query.
     */
    private Map<String, Object> getSalesReport(int days) {
        LOGGER.finer("Generating sales report for last " + days + " days.");
        String sql = """
        SELECT
            COUNT(order_id) AS total_orders,
            SUM(total_amount) AS total_revenue
        FROM orders
        WHERE created_at > ? AND status != 'CANCELLED';
        """;
        Map<String, Object> salesReport = new HashMap<>();
        Timestamp timeWindow = Timestamp.from(Instant.now().minus(days, ChronoUnit.DAYS));

        try (Connection conn = connect(); PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setTimestamp(1, timeWindow);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    salesReport.put("total_orders", rs.getInt("total_orders"));
                    salesReport.put("total_revenue", rs.getDouble("total_revenue"));
                }
            }
        } catch (SQLException e) {
            LOGGER.log(Level.WARNING, "Could not generate sales report.", e);
        }
        return salesReport;
    }

    /**
     * Private helper for generateDashboardReport.
     *
     * **SQL INTERDEPENDENCY (COMPLEX JOIN & AGGREGATION):**
     * This query is highly interdependent. It JOINS orders, order_items,
     * and products. It filters by time. It GROUPS by product.
     * It ORDERS by the aggregate sum.
     */
    private List<Map<String, Object>> getTopSellingProducts(int days, int limit) {
        LOGGER.finer(String.format("Getting top %d selling products for last %d days.", limit, days));
        String sql = """
        SELECT
            p.product_id,
            p.name,
            p.sku,
            SUM(oi.quantity) AS total_sold
        FROM order_items AS oi
        JOIN orders AS o ON oi.order_id = o.order_id
        JOIN products AS p ON oi.product_id = p.product_id
        WHERE o.created_at > ? AND o.status != 'CANCELLED'
        GROUP BY p.product_id, p.name, p.sku
        ORDER BY total_sold DESC
        LIMIT ?;
        """;
        List<Map<String, Object>> products = new ArrayList<>();
        Timestamp timeWindow = Timestamp.from(Instant.now().minus(days, ChronoUnit.DAYS));

        try (Connection conn = connect(); PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setTimestamp(1, timeWindow);
            pstmt.setInt(2, limit);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> product = new HashMap<>();
                    product.put("product_id", rs.getString("product_id"));
                    product.put("name", rs.getString("name"));
                    product.put("sku", rs.getString("sku"));
                    product.put("total_sold", rs.getInt("total_sold"));
                    products.add(product);
                }
            }
        } catch (SQLException e) {
            LOGGER.log(Level.WARNING, "Could not generate top selling products report.", e);
        }
        return products;
    }

    /**
     * Private helper for generateDashboardReport.
     *
     * **SQL INTERDEPENDENCY (JOIN & CONDITIONAL):**
     * Joins products and inventory, filters on a calculated value.
     */
    private List<Map<String, Object>> getLowStockItems(int threshold) {
        LOGGER.finer("Getting low stock items (threshold: " + threshold + ").");
        String sql = """
        SELECT
            p.product_id, p.name, p.sku,
            (i.quantity - i.reserved_quantity) AS available_stock
        FROM inventory AS i
        JOIN products AS p ON i.product_id = p.product_id
        WHERE (i.quantity - i.reserved_quantity) <= ?
        ORDER BY available_stock ASC;
        """;
        List<Map<String, Object>> items = new ArrayList<>();
        
        try (Connection conn = connect(); PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, threshold);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> item = new HashMap<>();
                    item.put("product_id", rs.getString("product_id"));
                    item.put("name", rs.getString("name"));
                    item.put("sku", rs.getString("sku"));
                    item.put("available_stock", rs.getInt("available_stock"));
                    items.add(item);
                }
            }
        } catch (SQLException e) {
            LOGGER.log(Level.WARNING, "Could not generate low stock items report.", e);
        }
        return items;
    }
    
    /**
     * Private helper for generateDashboardReport.
     * Gets the most recent N orders.
     */
    private List<Map<String, Object>> getLatestOrders(int limit) {
        LOGGER.finer("Getting latest " + limit + " orders.");
        String sql = """
        SELECT o.order_id, o.status, o.total_amount, o.created_at, u.username
        FROM orders AS o
        JOIN users AS u ON o.user_id = u.user_id
        ORDER BY o.created_at DESC
        LIMIT ?;
        """;
        List<Map<String, Object>> orders = new ArrayList<>();

        try (Connection conn = connect(); PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, limit);
            try (ResultSet rs = pstmt.executeQuery()) {
                while(rs.next()) {
                    Map<String, Object> order = new HashMap<>();
                    order.put("order_id", rs.getString("order_id"));
                    order.put("status", rs.getString("status"));
                    order.put("total_amount", rs.getDouble("total_amount"));
                    order.put("created_at", rs.getTimestamp("created_at"));
                    order.put("username", rs.getString("username"));
                    orders.add(order);
                }
            }
        } catch (SQLException e) {
             LOGGER.log(Level.WARNING, "Could not get latest orders.", e);
        }
        return orders;
    }
    
    // --- 7. MAIN METHOD (Entry Point) ---

    /**
     * Main method to run a demo of the monolithic system.
     * This demonstrates the interdependencies in action.
     *
     * @param args Command line arguments (not used).
     */
    public static void main(String[] args) {
        LOGGER.info("--- Starting Monolithic E-Commerce System Demo ---");
        
        // 1. Initialize the system
        MonolithicECommerce system = new MonolithicECommerce();
        
        // 2. Initialize the schema (crucial first step)
        //    FUNCTIONAL DEPENDENCY
        system.initializeSchema();
        
        LOGGER.info("--- 1. Populating Data ---");
        
        // 3. Create users
        //    FUNCTIONAL DEPENDENCY
        system.createUser("u-001", "alice@example.com", "alice");
        system.createUser("u-002", "bob@example.com", "bob");
        
        // 4. Create products
        //    FUNCTIONAL DEPENDENCY
        system.addNewProduct("p-001", "SKU-A-100", "Laptop", 1200.00, 20);
        system.addNewProduct("p-002", "SKU-B-200", "Mouse", 25.00, 100);
        system.addNewProduct("p-003", "SKU-C-300", "Keyboard", 75.00, 50);
        system.addNewProduct("p-004", "SKU-D-400", "Monitor", 300.00, 15);
        system.addNewProduct("p-005", "SKU-E-500", "Docking Station", 150.00, 5); // Low stock

        LOGGER.info("--- 2. Running Simulation ---");

        // 5. Alice places an order
        //    This demonstrates the most complex functional interdependency.
        LOGGER.info("--- Placing Alice's first order ---");
        Map<String, Integer> alicesCart1 = new HashMap<>();
        alicesCart1.put("p-001", 1); // 1 Laptop
        alicesCart1.put("p-002", 1); // 1 Mouse
        //    FUNCTIONAL DEPENDENCY
        String alicesOrderId = system.placeOrder("u-001", alicesCart1);
        if (alicesOrderId != null) {
            LOGGER.info("Alice's first order placed successfully: " + alicesOrderId);
        } else {
            LOGGER.severe("Alice's first order FAILED.");
        }

        // 6. Bob tries to buy out all the docking stations (p-005)
        LOGGER.info("--- Placing Bob's order (should fail) ---");
        Map<String, Integer> bobsCart = new HashMap<>();
        bobsCart.put("p-005", 10); // Tries to buy 10, only 5 in stock
        //    FUNCTIONAL DEPENDENCY
        String bobsOrderId = system.placeOrder("u-002", bobsCart);
        if (bobsOrderId == null) {
            LOGGER.info("Bob's order FAILED as expected (insufficient stock).");
        } else {
            LOGGER.severe("Bob's order " + bobsOrderId + " placed, but should have failed!");
        }
        
        // 7. Alice places a second, smaller order
        LOGGER.info("--- Placing Alice's second order ---");
        Map<String, Integer> alicesCart2 = new HashMap<>();
        alicesCart2.put("p-003", 2); // 2 Keyboards
        //    FUNCTIONAL DEPENDENCY
        String alicesOrderId2 = system.placeOrder("u-001", alicesCart2);
        if (alicesOrderId2 != null) {
            LOGGER.info("Alice's second order placed successfully: " + alicesOrderId2);
        } else {
            LOGGER.severe("Alice's second order FAILED.");
        }

        LOGGER.info("--- 3. Generating Reports ---");
        
        // 8. Get order details for Alice's first order
        //    FUNCTIONAL DEPENDENCY
        Map<String, Object> orderDetails = system.getOrderDetails(alicesOrderId);
        if (orderDetails != null) {
            // This would be serialized to JSON in a real app
            LOGGER.info("Details for order " + alicesOrderId + ":");
            LOGGER.info(orderDetails.toString());
        }

        // 9. Generate the main dashboard
        //    This shows the reporting interdependency
        //    FUNCTIONAL DEPENDENCY
        Map<String, Object> report = system.generateDashboardReport();
        if (report != null) {
            LOGGER.info("--- Dashboard Report ---");
            // This would be serialized to JSON in a real app
            LOGGER.info(report.toString());
            LOGGER.info("--- End of Report ---");
        } else {
            LOGGER.severe("Failed to generate dashboard report.");
        }
        
        LOGGER.info("--- Monolithic E-Commerce System Demo Finished ---");
    }
}