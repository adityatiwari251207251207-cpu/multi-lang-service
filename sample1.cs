/*
 * ====================================================================================
 * COMPLEX C# E-COMMERCE BACKEND SIMULATION
 * ====================================================================================
 * * This file simulates a multi-layered backend system for an e-commerce platform.
 * It is designed to be complex and demonstrate high levels of interdependency
 * between different components (services, repositories, models).
 * * Features:
 * - Multiple namespaces to organize code logically.
 * - Data Models (POCOs) for entities.
 * - Interfaces for dependency injection (DI) and testability.
 * - Mock implementations of interfaces.
 * - Repository pattern for data access simulation.
 * - Service layer for business logic.
 * - Simulated asynchronous operations (async/await).
 * - Placeholder SQL queries as string constants.
 * - Inter-service communication (e.g., Order service calls Product,
 * Customer, and Notification services).
 * - Basic error handling and logging simulation.
 * - A (very) simple in-memory cache simulation.
 * * Note: This is a single-file simulation. In a real-world application, each
 * class and interface would be in its own file. All database connections
 * and external API calls are mocked.
 * * ====================================================================================
 */

// --- ROOT USING STATEMENTS ---
using System;
using System.Collections.Generic;
using System.Data; // Faking ADO.NET types
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

/*
 * ====================================================================================
 * NAMESPACE: ECommerce.Core
 * DESCRIPTION: Contains core utilities like logging, caching, and constants.
 * ====================================================================================
 */
namespace ECommerce.Core
{
    /// <summary>
    /// Static logger class for simulation purposes.
    /// In a real app, this would be an instance injected via DI (e.g., ILogger).
    /// </summary>
    public static class Logger
    {
        public enum LogLevel
        {
            Debug,
            Info,
            Warning,
            Error,
            Fatal
        }

        public static void Log(LogLevel level, string message, Exception ex = null)
        {
            string logEntry = $"[{DateTime.UtcNow:O}] [{level,-7}] {message}";
            if (ex != null)
            {
                logEntry += $"\nEXCEPTION: {ex.Message}\nSTACK TRACE: {ex.StackTrace}";
            }

            // Using Debug.WriteLine to output to VS Output window
            Debug.WriteLine(logEntry);

            // In a real app, this would write to a file, database, or logging service
            // (e.g., Serilog, NLog, Application Insights).
        }

        public static void Info(string message) => Log(LogLevel.Info, message);
        public static void Warn(string message) => Log(LogLevel.Warning, message);
        public static void Error(string message, Exception ex = null) => Log(LogLevel.Error, message, ex);
        public static void Debug(string message) => Log(LogLevel.Debug, message);
    }

    /// <summary>
    /// A very simple, non-thread-safe in-memory cache simulation.
    /// In a real app, use IMemoryCache, IDistributedCache (Redis), or a dedicated library.
    /// </summary>
    public static class CacheManager
    {
        private static readonly Dictionary<string, (object data, DateTime expiry)> _cache = new Dictionary<string, (object, DateTime)>();
        private const int DefaultCacheDurationMinutes = 5;

        public static boolTryGet<T>(string key, out T value)
        {
            if (_cache.TryGetValue(key, out var cachedItem))
            {
                if (cachedItem.expiry > DateTime.UtcNow)
                {
                    value = (T)cachedItem.data;
                    Logger.Debug($"Cache HIT for key: {key}");
                    return true;
                }
                else
                {
                    // Cache item expired, remove it
                    _cache.Remove(key);
                    Logger.Debug($"Cache EXPIRED for key: {key}");
                }
            }

            Logger.Debug($"Cache MISS for key: {key}");
            value = default;
            return false;
        }

        public static void Set<T>(string key, T value, int? durationMinutes = null)
        {
            if (key == null) return;

            var expiry = DateTime.UtcNow.AddMinutes(durationMinutes ?? DefaultCacheDurationMinutes);
            _cache[key] = (value, expiry);
            Logger.Debug($"Cache SET for key: {key}");
        }

        public static void Clear(string key)
        {
            if (_cache.ContainsKey(key))
            {
                _cache.Remove(key);
                Logger.Debug($"Cache CLEARED for key: {key}");
            }
        }

        public static void ClearAll()
        {
            _cache.Clear();
            Logger.Info("Full cache cleared.");
        }
    }

    /// <summary>
    /// Centralized constants, including mock SQL queries.
    /// </summary>
    public static class Constants
    {
        public const string ConfigKeyDbConnectionString = "DbConnection";
        public const string ConfigKeySmtpHost = "SmtpHost";
        public const string ConfigKeySmtpUser = "SmtpUser";
        public const string ConfigKeySmtpPass = "SmtpPass";

        public static class SqlQueries
        {
            // --- Product Queries ---
            public const string GetProductById = "SELECT ProductID, Sku, ProductName, [Description], UnitPrice, StockQuantity, IsActive FROM Products WHERE ProductID = @ProductID AND IsActive = 1;";
            public const string GetProductsBySku = "SELECT ProductID, Sku, ProductName, [Description], UnitPrice, StockQuantity, IsActive FROM Products WHERE Sku = @Sku AND IsActive = 1;";
            public const string UpdateProductStock = "UPDATE Products SET StockQuantity = @NewStockQuantity WHERE ProductID = @ProductID;";
            public const string GetFeaturedProducts = "SELECT TOP (@Count) p.ProductID, p.Sku, p.ProductName, p.UnitPrice, p.StockQuantity FROM Products p JOIN FeaturedProducts f ON p.ProductID = f.ProductID WHERE p.IsActive = 1 ORDER BY f.SortOrder;";
            public const string CheckStockForUpdate = "SELECT StockQuantity FROM Products WITH (UPDLOCK) WHERE ProductID = @ProductID;"; // Simulate locking

            // --- Customer Queries ---
            public const string GetCustomerById = "SELECT CustomerID, FirstName, LastName, Email, CreatedDate, Street, City, State, ZipCode, Country FROM Customers WHERE CustomerID = @CustomerID;";
            public const string GetCustomerByEmail = "SELECT CustomerID, FirstName, LastName, Email, CreatedDate, Street, City, State, ZipCode, Country FROM Customers WHERE Email = @Email;";
            public const string CreateCustomer = "INSERT INTO Customers (FirstName, LastName, Email, CreatedDate, Street, City, State, ZipCode, Country) OUTPUT INSERTED.CustomerID VALUES (@FirstName, @LastName, @Email, @CreatedDate, @Street, @City, @State, @ZipCode, @Country);";
            public const string UpdateCustomerAddress = "UPDATE Customers SET Street = @Street, City = @City, State = @State, ZipCode = @ZipCode, Country = @Country WHERE CustomerID = @CustomerID;";
            public const string GetCustomerLogin = "SELECT CustomerID, HashedPassword, Salt FROM CustomerLogins WHERE Email = @Email;";

            // --- Order Queries ---
            public const string CreateOrderHeader = "INSERT INTO Orders (CustomerID, OrderDate, OrderStatus, TotalAmount, ShippingStreet, ShippingCity, ShippingState, ShippingZipCode, ShippingCountry) OUTPUT INSERTED.OrderID VALUES (@CustomerID, @OrderDate, @OrderStatus, @TotalAmount, @ShippingStreet, @ShippingCity, @ShippingState, @ShippingZipCode, @ShippingCountry);";
            public const string CreateOrderItem = "INSERT INTO OrderItems (OrderID, ProductID, Quantity, UnitPrice) VALUES (@OrderID, @ProductID, @Quantity, @UnitPrice);";
            public const string GetOrderById = "SELECT OrderID, CustomerID, OrderDate, OrderStatus, TotalAmount, ShippingStreet, ShippingCity, ShippingState, ShippingZipCode, ShippingCountry FROM Orders WHERE OrderID = @OrderID;";
            public li.GetOrdersByCustomerId = "SELECT OrderID, CustomerID, OrderDate, OrderStatus, TotalAmount FROM Orders WHERE CustomerID = @CustomerID ORDER BY OrderDate DESC;";
            public const string GetOrderItemsByOrderId = "SELECT ItemID, OrderID, ProductID, Quantity, UnitPrice FROM OrderItems WHERE OrderID = @OrderID;";
            public const string UpdateOrderStatus = "UPDATE Orders SET OrderStatus = @OrderStatus WHERE OrderID = @OrderID;";
            
            // --- Transaction Control ---
            public const string BeginTransaction = "BEGIN TRANSACTION;";
            public const string CommitTransaction = "COMMIT TRANSACTION;";
            public const string RollbackTransaction = "ROLLBACK TRANSACTION;";
        }
    }

    /// <summary>
    _    /// Custom exception for business logic failures (e.g., out of stock).
    /// </summary>
    public class BusinessLogicException : Exception
    {
        public BusinessLogicException(string message) : base(message) { }
        public BusinessLogicException(string message, Exception innerException) : base(message, innerException) { }
    }

    /// <summary>
    /// Custom exception for data access failures.
    /// </summary>
    public class DataAccessException : Exception
    {
        public string FailedSql { get; private set; }
        public DataAccessException(string message, string sql, Exception innerException) : base(message, innerException)
        {
            FailedSql = sql;
        }
    }
}

/*
 * ====================================================================================
 * NAMESPACE: ECommerce.Models
 * DESCRIPTION: Contains the Plain Old C# Objects (POCOs) or data entities.
 * ====================================================================================
 */
namespace ECommerce.Models
{
    using ECommerce.Core; // Use core utilities

    public class Address
    {
        public string Street { get; set; }
        public string City { get; set; }
        public string State { get; set; }
        public string ZipCode { get; set; }
        public string Country { get; set; }

        public override string ToString()
        {
            return $"{Street}, {City}, {State} {ZipCode}, {Country}";
        }

        public bool IsValid()
        {
            return !string.IsNullOrWhiteSpace(Street) &&
                   !string.IsNullOrWhiteSpace(City) &&
                   !string.IsNullOrWhiteSpace(State) &&
                   !string.IsNullOrWhiteSpace(ZipCode) &&
                   !string.IsNullOrWhiteSpace(Country);
        }
    }

    public class Customer
    {
        public int CustomerId { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string Email { get; set; }
        public DateTime CreatedDate { get; set; }
        public Address BillingAddress { get; set; }
        
        public Customer()
        {
            BillingAddress = new Address();
        }
    }

    public class Product
    {
        public int ProductId { get; set; }
        public string Sku { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public decimal Price { get; set; }
        public int StockQuantity { get; set; }
        public bool IsActive { get; set; }

        /// <summary>
        /// Validates if the product can be sold.
        /// </summary>
        public bool IsSellable()
        {
            return IsActive && Price > 0;
        }
    }

    public enum OrderStatus
    {
        Pending = 0,
        AwaitingPayment = 1,
        Processing = 2,
        Shipped = 3,
        Delivered = 4,
        Cancelled = 5,
        FraudReview = 6
    }

    public class Order
    {
        public int OrderId { get; set; }
        public int CustomerId { get; set; }
        public DateTime OrderDate { get; set; }
        public OrderStatus Status { get; set; }
        public decimal TotalAmount { get; set; }
        public Address ShippingAddress { get; set; }
        public List<OrderItem> Items { get; set; }

        public Order()
        {
            Items = new List<OrderItem>();
            ShippingAddress = new Address();
        }
    }

    public class OrderItem
    {
        public int OrderItemId { get; set; }
        public int OrderId { get; set; }
        public int ProductId { get; set; }
        public int Quantity { get; set; }
        public decimal UnitPrice { get; set; } // Price at the time of purchase
        public decimal LineTotal => Quantity * UnitPrice;

        // Navigation property (simulated)
        public Product Product { get; set; }
    }
}

/*
 * ====================================================================================
 * NAMESPACE: ECommerce.Data
 * DESCRIPTION: Data Access Layer simulation. Contains interfaces and repositories.
 * ====================================================================================
 */
namespace ECommerce.Data
{
    using ECommerce.Core;
    using ECommerce.Models;
    using System.Data.Common; // Faking ADO.NET

    /// <summary>
    /// Interface for a database connection. This would be implemented
    /// by a class that manages (e.g.) a SqlConnection.
    /// </summary>
    public interface IDatabaseConnection : IDisposable
    {
        Task<object> ExecuteScalarAsync(string sql, Dictionary<string, object> parameters);
        Task<int> ExecuteNonQueryAsync(string sql, Dictionary<string, object> parameters);
        Task<DataTable> ExecuteQueryAsync(string sql, Dictionary<string, object> parameters); // Using DataTable for simulation
        
        // Transaction control
        Task BeginTransactionAsync();
        Task CommitTransactionAsync();
        Task RollbackTransactionAsync();
    }

    /// <summary>
    /// Mock implementation of the database connection.
    /// This simulates database calls with delays and mock data.
    /// </summary>
    public class SqlConnectionMock : IDatabaseConnection
    {
        private bool _isTransactionActive = false;

        public SqlConnectionMock(string connectionString)
        {
            // In a real app, the connectionString would be used to open a connection.
            Logger.Info($"MockSqlConnection created. ConnectionString: '{connectionString.Substring(0, 10)}...'");
        }

        private async Task SimulateNetworkDelay()
        {
            // Simulate 50-150ms network latency
            await Task.Delay(new Random().Next(50, 150));
        }
        
        public async Task<object> ExecuteScalarAsync(string sql, Dictionary<string, object> parameters)
        {
            await SimulateNetworkDelay();
            Logger.Debug($"ExecuteScalarAsync: {sql.Split('\n').First()}");
            
            // Simulate returning a new ID
            if (sql.StartsWith("INSERT", StringComparison.OrdinalIgnoreCase))
            {
                return new Random().Next(1000, 9999); // Return a new mock ID
            }
            if (sql.StartsWith("SELECT StockQuantity", StringComparison.OrdinalIgnoreCase))
            {
                return 100; // Always say 100 in stock
            }
            return null;
        }

        public async Task<int> ExecuteNonQueryAsync(string sql, Dictionary<string, object> parameters)
        {
            await SimulateNetworkDelay();
            Logger.Debug($"ExecuteNonQueryAsync: {sql.Split('\n').First()}");
            
            // Simulate rows affected
            if (sql.StartsWith("UPDATE", StringComparison.OrdinalIgnoreCase) || 
                sql.StartsWith("INSERT", StringComparison.OrdinalIgnoreCase))
            {
                return 1; // 1 row affected
            }
            return 0;
        }

        public async Task<DataTable> ExecuteQueryAsync(string sql, Dictionary<string, object> parameters)
        {
            await SimulateNetworkDelay();
            Logger.Debug($"ExecuteQueryAsync: {sql.Split('\n').First()}");

            // This is the most complex part to mock. We'll just return
            // mock data based on the query constant.
            var dt = new DataTable();

            if (sql == Constants.SqlQueries.GetProductById)
            {
                dt.Columns.Add("ProductID", typeof(int));
                dt.Columns.Add("Sku", typeof(string));
                dt.Columns.Add("ProductName", typeof(string));
                dt.Columns.Add("UnitPrice", typeof(decimal));
                dt.Columns.Add("StockQuantity", typeof(int));
                dt.Rows.Add(parameters["@ProductID"], "MOCK-SKU-001", "Mock Product", 99.99m, 50);
            }
            else if (sql == Constants.SqlQueries.GetCustomerById)
            {
                dt.Columns.Add("CustomerID", typeof(int));
                dt.Columns.Add("FirstName", typeof(string));
                dt.Columns.Add("LastName", typeof(string));
                dt.Columns.Add("Email", typeof(string));
                dt.Columns.Add("Street", typeof(string));
                dt.Columns.Add("City", typeof(string));
                dt.Rows.Add(parameters["@CustomerID"], "John", "Doe", "john.doe@example.com", "123 Main St", "Anytown");
            }
            // ... etc. for other queries.
            
            return dt;
        }
        
        public async Task BeginTransactionAsync()
        {
            if (_isTransactionActive)
            {
                throw new InvalidOperationException("Transaction already in progress.");
            }
            await SimulateNetworkDelay();
            _isTransactionActive = true;
            Logger.Info("Mock DB Transaction BEGUN.");
        }

        public async Task CommitTransactionAsync()
        {
            if (!_isTransactionActive)
            {
                throw new InvalidOperationException("No transaction to commit.");
            }
            await SimulateNetworkDelay();
            _isTransactionActive = false;
            Logger.Info("Mock DB Transaction COMMITTED.");
        }

        public async Task RollbackTransactionAsync()
        {
            if (!_isTransactionActive)
            {
                // It's often safe to allow this, but for simulation we'll be strict
                Logger.Warn("Rollback called without an active transaction.");
                return;
            }
            await SimulateNetworkDelay();
            _isTransactionActive = false;
            Logger.Warn("Mock DB Transaction ROLLED BACK.");
        }

        public void Dispose()
        {
            // In a real app, this would close the connection.
            // If a transaction was active, it should be rolled back.
            if(_isTransactionActive)
            {
                Logger.Error("Connection disposed with an active transaction! Auto-rolling back.");
                // Note: Can't call async method in Dispose, this is a design flaw
                // in this simple mock. A real implementation would handle this.
                _isTransactionActive = false;
            }
            Logger.Debug("MockSqlConnection disposed.");
        }
    }

    // --- REPOSITORY INTERFACES ---

    public interface IProductRepository
    {
        Task<Product> GetProductByIdAsync(int id);
        Task<IEnumerable<Product>> GetProductsBySkuAsync(string sku);
        Task<bool> UpdateProductStockAsync(int productId, int newStock, IDatabaseConnection connection = null);
        Task<IEnumerable<Product>> GetFeaturedProductsAsync(int count);
        Task<int?> GetStockLevelForUpdateAsync(int productId, IDatabaseConnection connection);
    }

    public interface ICustomerRepository
    {
        Task<Customer> GetCustomerByIdAsync(int id);
        Task<Customer> GetCustomerByEmailAsync(string email);
        Task<int> CreateCustomerAsync(Customer customer);
        Task<bool> UpdateCustomerAddressAsync(int customerId, Address newAddress);
    }

    public interface IOrderRepository
    {
        Task<Order> GetOrderByIdAsync(int id);
        Task<IEnumerable<Order>> GetOrdersByCustomerIdAsync(int customerId);
        Task<int> CreateOrderInTransactionAsync(Order order, IDatabaseConnection connection);
        Task<bool> UpdateOrderStatusAsync(int orderId, OrderStatus newStatus);
    }

    // --- REPOSITORY IMPLEMENTATIONS ---

    /// <summary>
    /// Product Repository Simulation
    /// </summary>
    public class ProductRepository : IProductRepository
    {
        private readonly Func<IDatabaseConnection> _dbConnectionFactory;

        public ProductRepository(Func<IDatabaseConnection> dbConnectionFactory)
        {
            _dbConnectionFactory = dbConnectionFactory;
        }

        public async Task<Product> GetProductByIdAsync(int id)
        {
            string cacheKey = $"product:{id}";
            if (CacheManager.TryGet(cacheKey, out Product cachedProduct))
            {
                return cachedProduct;
            }

            var sql = Constants.SqlQueries.GetProductById;
            var parameters = new Dictionary<string, object> { { "@ProductID", id } };

            try
            {
                using (var db = _dbConnectionFactory())
                {
                    var dt = await db.ExecuteQueryAsync(sql, parameters);
                    if (dt.Rows.Count > 0)
                    {
                        var row = dt.Rows[0];
                        var product = new Product
                        {
                            ProductId = (int)row["ProductID"],
                            Sku = (string)row["Sku"],
                            Name = (string)row["ProductName"],
                            Price = (decimal)row["UnitPrice"],
                            StockQuantity = (int)row["StockQuantity"],
                            IsActive = true // From query
                        };
                        
                        CacheManager.Set(cacheKey, product, 5); // Cache for 5 mins
                        return product;
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to get product by ID: {id}", ex);
                throw new DataAccessException("Error retrieving product.", sql, ex);
            }
            return null;
        }

        public async Task<IEnumerable<Product>> GetProductsBySkuAsync(string sku)
        {
            // This method would be more complex, just simulating
            var product = await GetProductByIdAsync(123); // Fake it
            if(product != null && product.Sku == sku)
                return new List<Product> { product };
            return new List<Product>();
        }
        
        public async Task<int?> GetStockLevelForUpdateAsync(int productId, IDatabaseConnection connection)
        {
            var sql = Constants.SqlQueries.CheckStockForUpdate;
            var parameters = new Dictionary<string, object> { { "@ProductID", productId } };

            if (connection == null)
            {
                throw new ArgumentNullException(nameof(connection), "This method must be called within a transaction.");
            }

            try
            {
                var stock = await connection.ExecuteScalarAsync(sql, parameters);
                if (stock != null && stock != DBNull.Value)
                {
                    return (int)stock;
                }
                return null; // Product not found
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to get stock for update: {productId}", ex);
                throw new DataAccessException("Error getting stock for update.", sql, ex);
            }
        }

        public async Task<bool> UpdateProductStockAsync(int productId, int newStock, IDatabaseConnection connection = null)
        {
            var sql = Constants.SqlQueries.UpdateProductStock;
            var parameters = new Dictionary<string, object>
            {
                { "@ProductID", productId },
                { "@NewStockQuantity", newStock }
            };

            try
            {
                int rowsAffected;
                if (connection != null)
                {
                    // Use existing connection (part of a transaction)
                    rowsAffected = await connection.ExecuteNonQueryAsync(sql, parameters);
                }
                else
                {
                    // Create a new connection
                    using (var db = _dbConnectionFactory())
                    {
                        rowsAffected = await db.ExecuteNonQueryAsync(sql, parameters);
                    }
                }
                
                if(rowsAffected > 0)
                {
                    CacheManager.Clear($"product:{productId}"); // Invalidate cache
                    return true;
                }
                return false;
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to update stock for product: {productId}", ex);
                throw new DataAccessException("Error updating product stock.", sql, ex);
            }
        }

        public async Task<IEnumerable<Product>> GetFeaturedProductsAsync(int count)
        {
            // This would have its own logic, for now we just return one product
            var product = await GetProductByIdAsync(123); // Fake
            return new List<Product> { product };
        }
    }

    /// <summary>
    /// Customer Repository Simulation
    /// </summary>
    public class CustomerRepository : ICustomerRepository
    {
        private readonly Func<IDatabaseConnection> _dbConnectionFactory;

        public CustomerRepository(Func<IDatabaseConnection> dbConnectionFactory)
        {
            _dbConnectionFactory = dbConnectionFactory;
        }

        public async Task<Customer> GetCustomerByIdAsync(int id)
        {
            string cacheKey = $"customer:{id}";
            if (CacheManager.TryGet(cacheKey, out Customer cachedCustomer))
            {
                return cachedCustomer;
            }

            var sql = Constants.SqlQueries.GetCustomerById;
            var parameters = new Dictionary<string, object> { { "@CustomerID", id } };

            try
            {
                using (var db = _dbConnectionFactory())
                {
                    var dt = await db.ExecuteQueryAsync(sql, parameters);
                    if (dt.Rows.Count > 0)
                    {
                        var row = dt.Rows[0];
                        var customer = new Customer
                        {
                            CustomerId = (int)row["CustomerID"],
                            FirstName = (string)row["FirstName"],
                            LastName = (string)row["LastName"],
                            Email = (string)row["Email"],
                            BillingAddress = new Address
                            {
                                Street = row["Street"]?.ToString(),
                                City = row["City"]?.ToString(),
                                // ... etc
                            }
                        };
                        
                        CacheManager.Set(cacheKey, customer, 30); // Cache for 30 mins
                        return customer;
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to get customer by ID: {id}", ex);
                throw new DataAccessException("Error retrieving customer.", sql, ex);
            }
            return null;
        }

        public async Task<Customer> GetCustomerByEmailAsync(string email)
        {
            // Similar to GetCustomerByIdAsync, but queries by email
            // ... implementation omitted for brevity ...
            Logger.Debug($"Simulating GetCustomerByEmailAsync for {email}");
            if (email == "john.doe@example.com")
            {
                return await GetCustomerByIdAsync(1); // Return mock customer
            }
            return null;
        }

        public async Task<int> CreateCustomerAsync(Customer customer)
        {
            var sql = Constants.SqlQueries.CreateCustomer;
            var parameters = new Dictionary<string, object>
            {
                { "@FirstName", customer.FirstName },
                { "@LastName", customer.LastName },
                { "@Email", customer.Email },
                { "@CreatedDate", DateTime.UtcNow },
                { "@Street", customer.BillingAddress.Street },
                { "@City", customer.BillingAddress.City },
                { "@State", customer.BillingAddress.State },
                { "@ZipCode", customer.BillingAddress.ZipCode },
                { "@Country", customer.BillingAddress.Country }
            };

            try
            {
                using (var db = _dbConnectionFactory())
                {
                    var newId = await db.ExecuteScalarAsync(sql, parameters);
                    return (int)newId;
                }
            }
            catch (Exception ex)
            {
                // Handle unique constraint violation, etc.
                Logger.Error($"Failed to create customer: {customer.Email}", ex);
                throw new DataAccessException("Error creating customer.", sql, ex);
            }
        }

        public async Task<bool> UpdateCustomerAddressAsync(int customerId, Address newAddress)
        {
            var sql = Constants.SqlQueries.UpdateCustomerAddress;
             var parameters = new Dictionary<string, object>
            {
                { "@CustomerID", customerId },
                { "@Street", newAddress.Street },
                { "@City", newAddress.City },
                { "@State", newAddress.State },
                { "@ZipCode", newAddress.ZipCode },
                { "@Country", newAddress.Country }
            };

            try
            {
                using (var db = _dbConnectionFactory())
                {
                    var rowsAffected = await db.ExecuteNonQueryAsync(sql, parameters);
                    if(rowsAffected > 0)
                    {
                        CacheManager.Clear($"customer:{customerId}"); // Invalidate cache
                        return true;
                    }
                    return false;
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to update address for customer: {customerId}", ex);
                throw new DataAccessException("Error updating customer address.", sql, ex);
            }
        }
    }
    
    /// <summary>
    /// Order Repository Simulation
    /// </summary>
    public class OrderRepository : IOrderRepository
    {
        private readonly Func<IDatabaseConnection> _dbConnectionFactory;

        public OrderRepository(Func<IDatabaseConnection> dbConnectionFactory)
        {
            _dbConnectionFactory = dbConnectionFactory;
        }

        public async Task<int> CreateOrderInTransactionAsync(Order order, IDatabaseConnection connection)
        {
            if (connection == null)
            {
                throw new ArgumentNullException(nameof(connection), "This method must be called within a transaction.");
            }

            // 1. Create Order Header
            var headerSql = Constants.SqlQueries.CreateOrderHeader;
            var headerParams = new Dictionary<string, object>
            {
                { "@CustomerID", order.CustomerId },
                { "@OrderDate", order.OrderDate },
                { "@OrderStatus", (int)order.Status },
                { "@TotalAmount", order.TotalAmount },
                { "@ShippingStreet", order.ShippingAddress.Street },
                { "@ShippingCity", order.ShippingAddress.City },
                { "@ShippingState", order.ShippingAddress.State },
                { "@ShippingZipCode", order.ShippingAddress.ZipCode },
                { "@ShippingCountry", order.ShippingAddress.Country }
            };

            try
            {
                var newOrderId = (int)await connection.ExecuteScalarAsync(headerSql, headerParams);
                order.OrderId = newOrderId;

                // 2. Create Order Items
                var itemSql = Constants.SqlQueries.CreateOrderItem;
                foreach (var item in order.Items)
                {
                    item.OrderId = newOrderId;
                    var itemParams = new Dictionary<string, object>
                    {
                        { "@OrderID", item.OrderId },
                        { "@ProductID", item.ProductId },
                        { "@Quantity", item.Quantity },
                        { "@UnitPrice", item.UnitPrice }
                    };
                    await connection.ExecuteNonQueryAsync(itemSql, itemParams);
                }
                
                return newOrderId;
            }
            catch (Exception ex)
            {
                // The transaction will be rolled back by the calling service
                Logger.Error($"Failed to create order items for new order: {order.CustomerId}", ex);
                throw new DataAccessException("Error creating order items.", headerSql, ex);
            }
        }

        public async Task<Order> GetOrderByIdAsync(int id)
        {
            // Complex logic to get order header AND order items
            // ... implementation omitted for brevity ...
            Logger.Debug($"Simulating GetOrderByIdAsync for {id}");
            return new Order
            {
                OrderId = id,
                CustomerId = 1,
                OrderDate = DateTime.UtcNow.AddDays(-1),
                Status = OrderStatus.Shipped,
                TotalAmount = 199.98m,
                Items = new List<OrderItem>
                {
                    new OrderItem { OrderItemId = 1, OrderId = id, ProductId = 123, Quantity = 2, UnitPrice = 99.99m }
                }
            };
        }

        public async Task<IEnumerable<Order>> GetOrdersByCustomerIdAsync(int customerId)
        {
            // ... implementation omitted for brevity ...
            Logger.Debug($"Simulating GetOrdersByCustomerIdAsync for {customerId}");
            var order = await GetOrderByIdAsync(9876); // Get mock order
            order.CustomerId = customerId;
            return new List<Order> { order };
        }

        public async Task<bool> UpdateOrderStatusAsync(int orderId, OrderStatus newStatus)
        {
            var sql = Constants.SqlQueries.UpdateOrderStatus;
            var parameters = new Dictionary<string, object>
            {
                { "@OrderID", orderId },
                { "@OrderStatus", (int)newStatus }
            };

            try
            {
                using (var db = _dbConnectionFactory())
                {
                    var rowsAffected = await db.ExecuteNonQueryAsync(sql, parameters);
                    return rowsAffected > 0;
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to update status for order: {orderId}", ex);
                throw new DataAccessException("Error updating order status.", sql, ex);
staticlass ECommerceService
{
    // ... (rest of the OrderRepository class) ...
}
}

/*
 * ====================================================================================
 * NAMESPACE: ECommerce.Services
 * DESCRIPTION: Business Logic Layer. Contains interfaces and services.
 * ====================================================================================
 */
namespace ECommerce.Services
{
    using ECommerce.Core;
    using ECommerce.Data;
    using ECommerce.Models;
    using System.Net.Mail; // For simulation

    // --- SERVICE INTERFACES ---

    public interface IEmailSender
    {
        Task SendEmailAsync(string to, string from, string subject, string body);
    }

    public interface INotificationService
    {
        Task<bool> SendOrderConfirmationAsync(Customer customer, Order order);
        Task<bool> SendShippingNotificationAsync(Customer customer, Order order, string trackingNumber);
        Task<bool> SendPasswordResetEmailAsync(Customer customer, string resetToken);
        Task<bool> NotifyAdminOfFraudReview(Order order);
    }

    public interface IInventoryService
    {
        Task<bool> CheckStockAsync(int productId, int quantity);
        Task ReserveStockInTransactionAsync(int productId, int quantity, IDatabaseConnection connection);
        Task ReleaseStockAsync(int productId, int quantity);
    }

    public interface IPricingService
    {
        Task<decimal> CalculateProductPriceAsync(Product product, Customer customer);
        Task<decimal> CalculateShippingCostAsync(Address shippingAddress, decimal orderSubtotal);
        Task<decimal> CalculateTaxAsync(Address shippingAddress, decimal orderSubtotal);
    }

    public interface IFraudDetectionService
    {
        Task<(bool IsFraudulent, string Reason)> CheckOrderForFraudAsync(Order order, Customer customer);
    }
    
    public interface IOrderProcessingService
    {
        Task<(bool Success, Order NewOrder, string ErrorMessage)> PlaceOrderAsync(int customerId, Dictionary<int, int> cart, Address shippingAddress);
        Task<bool> CancelOrderAsync(int orderId, string reason, string cancelledByUserId);
        Task<bool> FulfillOrderAsync(int orderId, string trackingNumber);
    }

    // --- SERVICE IMPLEMENTATIONS ---

    /// <summary>
    /// Mock Email Sender
    /// </summary>
    public class SmtpEmailSenderMock : IEmailSender
    {
        private readonly string _host;
        private readonly string _user;

        public SmtpEmailSenderMock(string host, string user, string pass)
        {
            _host = host;
            _user = user;
            // pass is stored but not used in mock
            Logger.Info($"SmtpEmailSenderMock configured for host {_host} with user {_user}");
        }

        public async Task SendEmailAsync(string to, string from, string subject, string body)
        {
            // Simulate 100-300ms network call
            await Task.Delay(new Random().Next(100, 300));
            
            // Log the email instead of sending it
            Logger.Info($"--- MOCK EMAIL ---");
            Logger.Info($"To: {to}");
            Logger.Info($"From: {from}");
            Logger.Info($"Subject: {subject}");
            Logger.Info($"Body: {body.Substring(0, Math.Min(body.Length, 50))}...");
            Logger.Info($"--- END MOCK EMAIL ---");
            
            // Simulate a possible failure
            if (to.Contains("fail"))
            {
                throw new SmtpException("Simulated SMTP failure for user 'fail'.");
            }
        }
    }

    /// <summary>
    /// Notification Service (depends on IEmailSender)
    /// </summary>
    public class NotificationService : INotificationService
    {
        private readonly IEmailSender _emailSender;
        private const string AdminEmail = "admin@ecommerce.com";
        private const string FromEmail = "noreply@ecommerce.com";

        public NotificationService(IEmailSender emailSender)
        {
            _emailSender = emailSender;
        }

        public async Task<bool> SendOrderConfirmationAsync(Customer customer, Order order)
        {
            var subject = $"Your order #{order.OrderId} is confirmed!";
            var body = new StringBuilder();
            body.AppendLine($"Hi {customer.FirstName},");
            body.AppendLine($"<p>Thank you for your order! We're getting it ready.</p>");
            body.AppendLine($"<p><b>Order ID:</b> {order.OrderId}</p>");
            body.AppendLine($"<p><b>Total:</b> {order.TotalAmount:C}</p>");
            body.AppendLine("<p>We'll notify you when it ships.</p>");

            try
            {
                await _emailSender.SendEmailAsync(customer.Email, FromEmail, subject, body.ToString());
                return true;
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to send order confirmation for order {order.OrderId}", ex);
                return false; // Non-critical failure
            }
        }

        public async Task<bool> SendShippingNotificationAsync(Customer customer, Order order, string trackingNumber)
        {
            var subject = $"Your order #{order.OrderId} has shipped!";
            var body = $"Hi {customer.FirstName},\n\nGood news! Your order has shipped.\n\nTracking Number: {trackingNumber}\n";

            try
            {
                await _emailSender.SendEmailAsync(customer.Email, FromEmail, subject, body.ToString());
                return true;
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to send shipping notification for order {order.OrderId}", ex);
                return false;
            }
        }

        public async Task<bool> NotifyAdminOfFraudReview(Order order)
        {
            var subject = $"[ACTION REQUIRED] Order #{order.OrderId} flagged for fraud review.";
            var body = $"Order {order.OrderId} for customer {order.CustomerId} totaling {order.TotalAmount:C} was flagged for manual fraud review.";
            
            try
            {
                await _emailSender.SendEmailAsync(AdminEmail, FromEmail, subject, body.ToString());
                return true;
            }
            catch (Exception ex)
            {
                Logger.Error($"CRITICAL: Failed to send fraud alert email for order {order.OrderId}", ex);
                // This might trigger a different kind of alert (e.g., PagerDuty)
                return false;
            }
        }

        public async Task<bool> SendPasswordResetEmailAsync(Customer customer, string resetToken)
        {
            var subject = "Your password reset request";
            var body = $"Hi {customer.FirstName},\n\nClick this link to reset your password: https://example.com/reset?token={resetToken}";
            
            try
            {
                await _emailSender.SendEmailAsync(customer.Email, FromEmail, subject, body.ToString());
                return true;
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to send password reset for customer {customer.CustomerId}", ex);
                return false;
            }
        }
    }

    /// <summary>
    /// Inventory Service (depends on IProductRepository)
    /// </summary>
    public class InventoryService : IInventoryService
    {
        private readonly IProductRepository _productRepo;

        public InventoryService(IProductRepository productRepo)
        {
            _productRepo = productRepo;
        }

        public async Task<bool> CheckStockAsync(int productId, int quantity)
        {
            var product = await _productRepo.GetProductByIdAsync(productId);
            if (product == null)
            {
                Logger.Warn($"Stock check failed: Product {productId} not found.");
                return false;
            }
            return product.StockQuantity >= quantity;
        }

        public async Task ReserveStockInTransactionAsync(int productId, int quantity, IDatabaseConnection connection)
        {
            if (connection == null)
            {
                throw new ArgumentNullException(nameof(connection), "ReserveStock must be called within a transaction.");
            }

            // 1. Get current stock with a row lock
            var currentStock = await _productRepo.GetStockLevelForUpdateAsync(productId, connection);

            if (!currentStock.HasValue)
            {
                throw new BusinessLogicException($"Product {productId} not found for stock reservation.");
            }

            // 2. Check if stock is sufficient
            if (currentStock.Value < quantity)
            {
                throw new BusinessLogicException($"Insufficient stock for product {productId}. Requested: {quantity}, Available: {currentStock.Value}");
            }

            // 3. Update stock
            int newStock = currentStock.Value - quantity;
            var success = await _productRepo.UpdateProductStockAsync(productId, newStock, connection);

            if (!success)
            {
                // Should not happen if lock was successful, but good to check
                throw new DataAccessException("Failed to update stock even after acquiring lock.", "StockUpdate", null);
            }
            
            Logger.Info($"Stock reserved for product {productId}. New stock: {newStock}");
        }

        public async Task ReleaseStockAsync(int productId, int quantity)
        {
            // This is complex: "releasing" stock (e.g., from a cancelled order)
            // needs to be idempotent and safe.
            // For simulation, we just add it back.
            // A real system would use a transaction and row locking here too.
            var product = await _productRepo.GetProductByIdAsync(productId);
            if (product != null)
            {
                int newStock = product.StockQuantity + quantity;
                await _productRepo.UpdateProductStockAsync(productId, newStock);
                Logger.Info($"Stock released for product {productId}. New stock: {newStock}");
            }
            else
            {
                Logger.Error($"Failed to release stock: Product {productId} not found.", null);
            }
        }
    }
    
    /// <summary>
    /// Pricing Service (simulated)
    /// </summary>
    public class PricingService : IPricingService
    {
        public async Task<decimal> CalculateProductPriceAsync(Product product, Customer customer)
        {
            // Simulate customer-specific pricing or sales
            await Task.Delay(10); // Simulate some logic
            
            // 10% off for "John Doe"
            if (customer.FirstName == "John")
            {
                return product.Price * 0.90m;
            }
            
            return product.Price;
        }

        public async Task<decimal> CalculateShippingCostAsync(Address shippingAddress, decimal orderSubtotal)
        {
            // Simulate complex shipping calculation
            await Task.Delay(20); // Simulate API call to UPS/FedEx
            
            if (orderSubtotal > 100.00m)
            {
                return 0.00m; // Free shipping
            }
            
            if(shippingAddress.Country == "USA")
            {
                return 5.99m;
            }
            
            return 19.99m; // International
        }

        public async Task<decimal> CalculateTaxAsync(Address shippingAddress, decimal orderSubtotal)
        {
            // Simulate complex tax calculation (e.g., Avalara API call)
            await Task.Delay(50);
            
            if (shippingAddress.State == "CA")
            {
                return orderSubtotal * 0.0725m; // 7.25%
            }
            if (shippingAddress.State == "NY")
            {
                return orderSubtotal * 0.04m; // 4%
            }
            
            return 0.00m; // No tax for other states
        }
    }
    
    /// <summary>
    /// Fraud Detection Service (simulated)
    /// </summary>
    public class FraudDetectionService : IFraudDetectionService
    {
        public async Task<(bool IsFraudulent, string Reason)> CheckOrderForFraudAsync(Order order, Customer customer)
        {
            // Simulate a complex fraud check (e.g., call to Sift/Signifyd)
            await Task.Delay(new Random().Next(200, 500)); // Simulate slow API

            // Rule 1: Order total over $1000
            if (order.TotalAmount > 1000.00m)
            {
                return (true, "Order total exceeds $1000 threshold.");
            }

            // Rule 2: Shipping address != billing address
            if (order.ShippingAddress.ToString() != customer.BillingAddress.ToString())
            {
                // This is a weak rule, but good for simulation
                Logger.Warn($"Fraud check: Shipping address does not match billing for customer {customer.CustomerId}");
            }
            
            // Rule 3: Customer email is known fraudulent
            if (customer.Email.Contains("fraudster@bad.com"))
            {
                return (true, "Customer email is on blocklist.");
            }
            
            return (false, null);
        }
    }

    /// <summary>
    /// Order Processing Service
    /// THIS IS THE MOST COMPLEX, INTERDEPENDENT SERVICE
    /// </summary>
    public class OrderProcessingService : IOrderProcessingService
    {
        // --- DEPENDENCIES ---
        private readonly Func<IDatabaseConnection> _dbConnectionFactory;
        private readonly ICustomerRepository _customerRepo;
        private readonly IProductRepository _productRepo;
        private readonly IOrderRepository _orderRepo;
        private readonly IInventoryService _inventoryService;
        private readonly IPricingService _pricingService;
        private readonly IFraudDetectionService _fraudService;
        private readonly INotificationService _notificationService;

        public OrderProcessingService(
            Func<IDatabaseConnection> dbConnectionFactory,
            ICustomerRepository customerRepo,
            IProductRepository productRepo,
            IOrderRepository orderRepo,
            IInventoryService inventoryService,
            IPricingService pricingService,
            IFraudDetectionService fraudService,
            INotificationService notificationService)
        {
            _dbConnectionFactory = dbConnectionFactory;
            _customerRepo = customerRepo;
            _productRepo = productRepo;
            _orderRepo = orderRepo;
            _inventoryService = inventoryService;
            _pricingService = pricingService;
            _fraudService = fraudService;
            _notificationService = notificationService;
        }

        /// <summary>
        /// The main complex method. Places a new order.
        /// This entire method should be wrapped in a database transaction.
        /// </summary>
        public async Task<(bool Success, Order NewOrder, string ErrorMessage)> PlaceOrderAsync(
            int customerId, 
            Dictionary<int, int> cart, // Key: ProductId, Value: Quantity
            Address shippingAddress)
        {
            if (cart == null || !cart.Any())
            {
                return (false, null, "Cart is empty.");
            }
            
            if(!shippingAddress.IsValid())
            {
                return (false, null, "Shipping address is invalid.");
            }

            // 1. Get Customer
            var customer = await _customerRepo.GetCustomerByIdAsync(customerId);
            if (customer == null)
            {
                return (false, null, $"Customer {customerId} not found.");
            }

            var newOrder = new Order
            {
                CustomerId = customerId,
                OrderDate = DateTime.UtcNow,
                ShippingAddress = shippingAddress,
                Status = OrderStatus.Pending // Start as pending
            };

            decimal subtotal = 0;

            // 2. Get product data and calculate prices
            try
            {
                foreach (var cartItem in cart)
                {
                    int productId = cartItem.Key;
                    int quantity = cartItem.Value;

                    if (quantity <= 0)
                    {
                        return (false, null, $"Invalid quantity for product {productId}.");
                    }
                    
                    var product = await _productRepo.GetProductByIdAsync(productId);
                    if (product == null || !product.IsSellable())
                    {
                        return (false, null, $"Product {productId} is not available.");
                    }
                    
                    // Get customer-specific price
                    var unitPrice = await _pricingService.CalculateProductPriceAsync(product, customer);
                    
                    newOrder.Items.Add(new OrderItem
                    {
                        ProductId = productId,
                        Quantity = quantity,
                        UnitPrice = unitPrice,
                        Product = product // For internal use (stock check)
                    });

                    subtotal += (unitPrice * quantity);
                }
            }
            catch(DataAccessException ex)
            {
                Logger.Error("Failed during product retrieval for new order.", ex);
                return (false, null, "An error occurred while validating products.");
            }
            
            // 3. Calculate Shipping and Tax
            var shippingCost = await _pricingService.CalculateShippingCostAsync(shippingAddress, subtotal);
            var taxCost = await _pricingService.CalculateTaxAsync(shippingAddress, subtotal);
            
            newOrder.TotalAmount = subtotal + shippingCost + taxCost;
            
            // 4. Fraud Check
            var (isFraud, fraudReason) = await _fraudService.CheckOrderForFraudAsync(newOrder, customer);
            if (isFraud)
            {
                newOrder.Status = OrderStatus.FraudReview;
                // Save order but don't process. We'll skip the transaction for this demo.
                // In a real app, we'd still save the order.
                Logger.Warn($"Order {newOrder.OrderId} flagged for fraud: {fraudReason}");
                
                // We'd still save the order, just with a "FraudReview" status.
                // For this simulation, we'll just stop here.
                
                // Alert admin
                await _notificationService.NotifyAdminOfFraudReview(newOrder);
                
                return (false, null, $"Order flagged for fraud review: {fraudReason}");
            }

            // 5. --- BEGIN DATABASE TRANSACTION ---
            // This is the critical part. Stock reservation and order creation
            // must happen atomically.
            using (var db = _dbConnectionFactory())
            {
                try
                {
                    await db.BeginTransactionAsync();

                    // 5a. Reserve stock for each item *within the transaction*
                    foreach (var item in newOrder.Items)
                    {
                        // This method gets a lock and updates stock
                        await _inventoryService.ReserveStockInTransactionAsync(item.ProductId, item.Quantity, db);
                    }
                    
                    // 5b. Set order status to "Processing"
                    newOrder.Status = OrderStatus.Processing;

                    // 5c. Create the order and order items *within the transaction*
                    int newOrderId = await _orderRepo.CreateOrderInTransactionAsync(newOrder, db);
                    newOrder.OrderId = newOrderId;

                    // 5d. Commit the transaction
                    await db.CommitTransactionAsync();
                }
                catch (BusinessLogicException ex) // e.g., Out of stock
                {
                    await db.RollbackTransactionAsync();
                    Logger.Warn($"Failed to place order: {ex.Message}");
                    return (false, null, ex.Message); // Pass clean error to user
                }
                catch (Exception ex)
                {
                    await db.RollbackTransactionAsync();
                    Logger.Error("Catastrophic failure during order transaction.", ex);
                    return (false, null, "A critical error occurred. Your order was not placed.");
                }
            }
            // --- END DATABASE TRANSACTION ---

            // 6. Post-Processing (outside transaction)
            // Send email confirmation. If this fails, the order is still valid.
            try
            {
                await _notificationService.SendOrderConfirmationAsync(customer, newOrder);
            }
            catch(Exception ex)
            {
                // Log and ignore. The order is placed.
                Logger.Error($"Failed to send confirmation email for order {newOrder.OrderId}", ex);
            }

            Logger.Info($"Successfully placed order {newOrder.OrderId} for customer {customerId}.");
            
            // Clear product caches since stock changed
            foreach (var item in newOrder.Items)
            {
                CacheManager.Clear($"product:{item.ProductId}");
            }
            
            return (true, newOrder, "Order placed successfully.");
        }

        public async Task<bool> CancelOrderAsync(int orderId, string reason, string cancelledByUserId)
        {
            Logger.Info($"Attempting to cancel order {orderId} by user {cancelledByUserId} for reason: {reason}");
            
            // 1. Get Order
            var order = await _orderRepo.GetOrderByIdAsync(orderId);
            if (order == null)
            {
                Logger.Error($"CancelOrder: Order {orderId} not found.");
                return false;
            }

            // 2. Check status
            if (order.Status == OrderStatus.Shipped || order.Status == OrderStatus.Delivered)
            {
                Logger.Warn($"Cannot cancel order {orderId}: already shipped.");
                return false; // Cannot cancel a shipped order
            }
            
            if (order.Status == OrderStatus.Cancelled)
            {
                Logger.Info($"Order {orderId} is already cancelled.");
                return true; // Already done
            }
            
            // 3. --- BEGIN TRANSACTION ---
            // This is not fully implemented in the mock, but in a real system:
            // - Update order status to Cancelled
            // - Release payment authorization (call payment gateway)
            // - Add stock back to inventory
            // All of this should be in a transaction
            
            using (var db = _dbConnectionFactory())
            {
                try
                {
                    await db.BeginTransactionAsync();
                    
                    // 3a. Update order status
                    await _orderRepo.UpdateOrderStatusAsync(orderId, OrderStatus.Cancelled);
                    
                    // 3b. Release stock
                    foreach (var item in order.Items)
                    {
                        // This should ideally use the transaction
                        // Our mock InventoryService doesn't fully support this
                        // await _inventoryService.ReleaseStockInTransactionAsync(item.ProductId, item.Quantity, db);
                        
                        // We'll call the non-transactional version for this sim
                        await _inventoryService.ReleaseStockAsync(item.ProductId, item.Quantity);
                    }
                    
                    // 3c. TODO: Release payment hold (call payment service)
                    // PaymentService.ReleaseHold(order.PaymentTransactionId);
                    
                    await db.CommitTransactionAsync();
                }
                catch (Exception ex)
                {
                    await db.RollbackTransactionAsync();
                    Logger.Error($"Failed to cancel order {orderId}", ex);
                    return false;
                }
            }
            // --- END TRANSACTION ---
            
            // 4. Send notification (non-critical)
            var customer = await _customerRepo.GetCustomerByIdAsync(order.CustomerId);
            // ... await _notificationService.SendOrderCancelledEmailAsync(customer, order); ...
            
            return true;
        }

        public async Task<bool> FulfillOrderAsync(int orderId, string trackingNumber)
        {
            // 1. Get Order
            var order = await _orderRepo.GetOrderByIdAsync(orderId);
            if (order == null)
            {
                Logger.Error($"FulfillOrder: Order {orderId} not found.");
                return false;
            }
            
            // 2. Check status
            if (order.Status != OrderStatus.Processing)
            {
                Logger.Warn($"Cannot fulfill order {orderId}: status is {order.Status}, not 'Processing'.");
                return false;
            }

            // 3. Update status in DB
            bool success = await _orderRepo.UpdateOrderStatusAsync(orderId, OrderStatus.Shipped);
            if (!success)
            {
                Logger.Error($"Failed to update order status to Shipped for {orderId}");
                return false;
            }

            // 4. Send notification
            var customer = await _customerRepo.GetCustomerByIdAsync(order.CustomerId);
            if (customer != null)
            {
                await _notificationService.SendShippingNotificationAsync(customer, order, trackingNumber);
            }
            else
            {
                Logger.Error($"Could not send shipping email: Customer {order.CustomerId} not found for order {orderId}.");
            }

            Logger.Info($"Order {orderId} fulfilled and shipped with tracking {trackingNumber}.");
            return true;
        }
    }
}


/*
 * ====================================================================================
 * NAMESPACE: ECommerce.Main
 * DESCRIPTION: Main entry point to tie everything together (like a Web API controller)
 * ====================================================================================
 */
namespace ECommerce.Main
{
    using ECommerce.Core;
    using ECommerce.Data;
    using ECommerce.Models;
    using ECommerce.Services;

    /// <summary>
    /// This class simulates the "Composition Root" of the application
    /// (e.g., in Startup.cs or Program.cs with DI)
    /// where all dependencies are wired together.
    /// </summary>
    public class ApplicationHost
    {
        // --- Injected Services ---
        private readonly IOrderProcessingService _orderService;
        private readonly ICustomerRepository _customerRepo;
        
        // --- Static Factory for DI Simulation ---
        public static ApplicationHost CreateApplication()
        {
            Logger.Info("=======================================");
            Logger.Info("=    ApplicationHost Starting Up...   =");
            Logger.Info("=======================================");

            // --- Configuration Simulation ---
            string dbConnection = "Server=myServer;Database=myDb;User=myUser;Password=myPassword;";
            string smtpHost = "smtp.example.com";
            string smtpUser = "user@example.com";
            string smtpPass = "password123";

            // --- Dependency Injection Setup (Manual) ---
            
            // 1. Create the DB connection factory
            Func<IDatabaseConnection> dbConnectionFactory = () => new SqlConnectionMock(dbConnection);

            // 2. Create Repositories
            var customerRepo = new CustomerRepository(dbConnectionFactory);
            var productRepo = new ProductRepository(dbConnectionFactory);
            var orderRepo = new OrderRepository(dbConnectionFactory);

            // 3. Create External Services
            var emailSender = new SmtpEmailSenderMock(smtpHost, smtpUser, smtpPass);

            // 4. Create Business Logic Services
            var notificationService = new NotificationService(emailSender);
            var inventoryService = new InventoryService(productRepo);
            var pricingService = new PricingService();
            var fraudService = new FraudDetectionService();

            // 5. Create the main, complex service
            var orderService = new OrderProcessingService(
                dbConnectionFactory,
                customerRepo,
                productRepo,
                orderRepo,
                inventoryService,
                pricingService,
                fraudService,
                notificationService
            );

            // 6. Create the "Host" and inject services
            return new ApplicationHost(orderService, customerRepo);
        }

        // --- Constructor (simulates DI) ---
        public ApplicationHost(IOrderProcessingService orderService, ICustomerRepository customerRepo)
        {
            _orderService = orderService;
            _customerRepo = customerRepo;
            Logger.Info("ApplicationHost Created and Dependencies Injected.");
        }

        // --- Public API Methods (simulates API endpoints) ---

        /// <summary>
        /// Simulates an API call to POST /api/orders
        /// </summary>
        public async Task RunOrderPlacementDemo()
        {
            Logger.Info("\n--- Running Order Placement Demo ---");
            
            // 1. Define the order
            int customerId = 1; // Mock customer John Doe
            var cart = new Dictionary<int, int>
            {
                { 123, 2 } // 2 of "Mock Product" (ID 123)
            };
            var shippingAddress = new Address
            {
                Street = "123 Main St",
                City = "Anytown",
                State = "CA",
                ZipCode = "12345",
                Country = "USA"
            };

            // 2. Call the service
            var (success, order, message) = await _orderService.PlaceOrderAsync(customerId, cart, shippingAddress);

            // 3. Log result
            if (success)
            {
                Logger.Info($"--->>> ORDER PLACED SUCCESSFULLY <<<---");
                Logger.Info($"     Order ID: {order.OrderId}");
                Logger.Info($"     Total: {order.TotalAmount:C}");
            }
            else
            {
                Logger.Error($"--->>> ORDER FAILED <<<---");
                Logger.Error($"     Message: {message}");
            }
        }
        
        /// <summary>
        /// Simulates an API call to PUT /api/orders/{id}/fulfill
        /// </summary>
        public async Task RunOrderFulfillmentDemo()
        {
            Logger.Info("\n--- Running Order Fulfillment Demo ---");
            
            int orderIdToFulfill = 9876; // A mock existing order
            string tracking = "1Z999AA10123456789";

            bool success = await _orderService.FulfillOrderAsync(orderIdToFulfill, tracking);

            if (success)
            {
                Logger.Info($"--->>> ORDER FULFILLED SUCCESSFULLY (ID: {orderIdToFulfill}) <<<---");
            }
            else
            {
                Logger.Error($"--->>> ORDER FULFILLMENT FAILED (ID: {orderIdToFulfill}) <<<---");
            }
        }
        
        /// <summary>
        /// Simulates an API call to GET /api/customers/{id}
        /// </summary>
        public async Task RunGetCustomerDemo()
        {
            Logger.Info("\n--- Running Get Customer Demo ---");
            
            int customerId = 1;
            var customer = await _customerRepo.GetCustomerByIdAsync(customerId);
            
            // This will be a cache MISS first
            if(customer != null)
            {
                Logger.Info($"Got customer (Cache MISS): {customer.FirstName} {customer.LastName}");
            }
            else
            {
                Logger.Error($"Could not find customer {customerId}");
            }
            
            // This will be a cache HIT
            customer = await _customerRepo.GetCustomerByIdAsync(customerId);
            if(customer != null)
            {
                Logger.Info($"Got customer (Cache HIT): {customer.Email}");
            }
        }
    }

    /// <summary>
    /// Static Program entry point (for a console app simulation)
    /// </summary>
    public static class Program
    {
        // We can't use a real "Main" method, but we can simulate it.
        // To run this, you would call:
        // ECommerce.Main.Program.RunSimulation().Wait();
        
        public static async Task RunSimulation()
        {
            try
            {
                var app = ApplicationHost.CreateApplication();
                
                await app.RunGetCustomerDemo();
                
                await app.RunOrderPlacementDemo();
                
                await app.RunOrderFulfillmentDemo();
            }
            catch (Exception ex)
            {
                Logger.Log(LogLevel.Fatal, "An unhandled exception occurred in the simulation.", ex);
            }
            finally
            {
                 Logger.Info("=======================================");
                 Logger.Info("=       Simulation Finished.        =");
                 Logger.Info("=======================================");
            }
        }
    }
} // End of all namespaces