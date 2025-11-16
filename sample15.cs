/*
 * ERPCore.cs
 *
 * This file represents a core module of a monolithic Enterprise Resource Planning (ERP) system.
 * It focuses on the interdependent services required for supply chain management and manufacturing.
 *
 * Features:
 * - Complex, interdependent services (Inventory, Procurement, Manufacturing, Planning).
 * - Data models (entities) for products, materials, orders, etc.
 * - A mock database connection to simulate executing interdependent SQL queries.
 * - Asynchronous method signatures (Task<T>) to mimic real-world async operations.
 * - Event handling using delegates for cross-service communication (e.g., "OnStockLow").
 * - A main simulation method to demonstrate the workflow.
 */

using System;
using System.Collections.Generic;
using System.Data; // For mock data tables
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

// --- Primary Namespace for the ERP System ---
namespace ERPSystem.Core
{
    // --- Logging Utility ---
    public static class ErpLogger
    {
        // Simple console logger
        public static void Log(string level, string service, string message)
        {
            string logEntry = $"{DateTime.UtcNow:O} [{level.ToUpper()}] [{service}] {message}";
            Console.WriteLine(logEntry);
        }

        public static void Info(string service, string message) => Log("INFO", service, message);
        public static void Warn(string service, string message) => Log("WARN", service, message);
        public static void Error(string service, string message, Exception ex = null)
        {
            string errorMsg = ex != null ? $"{message} | Exception: {ex.Message}" : message;
            Log("ERROR", service, errorMsg);
        }
    }

    // --- Mock Database Context ---
    public class MockDbConnection
    {
        private readonly string _connectionString;
        private bool _isTransactionActive = false;
        private static readonly string ServiceName = "Database";

        // Mock data tables to simulate the database state
        private Dictionary<string, InventoryItem> MockInventoryTable;
        private Dictionary<string, Product> MockProductTable;
        private Dictionary<string, RawMaterial> MockRawMaterialTable;
        private Dictionary<string, Supplier> MockSupplierTable;
        private Dictionary<string, BillOfMaterials> MockBomTable;
        private List<PurchaseOrder> MockPurchaseOrderTable;
        private List<WorkOrder> MockWorkOrderTable;

        public MockDbConnection(string connectionString)
        {
            _connectionString = connectionString;
            ErpLogger.Info(ServiceName, $"Initializing mock database connection: {_connectionString}");
            InitializeMockData();
        }

        private void InitializeMockData()
        {
            ErpLogger.Info(ServiceName, "Seeding mock data...");

            // --- Products (Finished Goods) ---
            MockProductTable = new Dictionary<string, Product>
            {
                ["PROD-001"] = new Product { ProductId = "PROD-001", Name = "Deluxe Wooden Chair", Description = "A comfortable chair." },
                ["PROD-002"] = new Product { ProductId = "PROD-002", Name = "Large Oak Table", Description = "A sturdy dining table." }
            };

            // --- Raw Materials ---
            MockRawMaterialTable = new Dictionary<string, RawMaterial>
            {
                ["MAT-001"] = new RawMaterial { MaterialId = "MAT-001", Name = "Oak Wood Plank", UnitOfMeasure = "plank" },
                ["MAT-002"] = new RawMaterial { MaterialId = "MAT-002", Name = "Varnish", UnitOfMeasure = "liter" },
                ["MAT-003"] = new RawMaterial { MaterialId = "MAT-003", Name = "Wood Screw (Box)", UnitOfMeasure = "box" },
                ["MAT-004"] = new RawMaterial { MaterialId = "MAT-004", Name = "Cushion", UnitOfMeasure = "unit" }
            };

            // --- Suppliers ---
            MockSupplierTable = new Dictionary<string, Supplier>
            {
                ["SUP-1001"] = new Supplier { SupplierId = "SUP-1001", Name = "Forestry Supplies Inc.", PreferredMaterialId = "MAT-001" },
                ["SUP-1002"] = new Supplier { SupplierId = "SUP-1002", Name = "ChemCo", PreferredMaterialId = "MAT-002" },
                ["SUP-1003"] = new Supplier { SupplierId = "SUP-1003", Name = "Hardware Express", PreferredMaterialId = "MAT-003" },
                ["SUP-1004"] = new Supplier { SupplierId = "SUP-1004", Name = "Comfort Textiles", PreferredMaterialId = "MAT-004" }
            };

            // --- Inventory (Stock Levels) ---
            MockInventoryTable = new Dictionary<string, InventoryItem>
            {
                // Raw Materials
                ["MAT-001"] = new InventoryItem { ItemSku = "MAT-001", QuantityOnHand = 100, ReorderPoint = 50, Location = "WH-A" },
                ["MAT-002"] = new InventoryItem { ItemSku = "MAT-002", QuantityOnHand = 40, ReorderPoint = 20, Location = "WH-B" },
                ["MAT-003"] = new InventoryItem { ItemSku = "MAT-003", QuantityOnHand = 200, ReorderPoint = 100, Location = "WH-A" },
                ["MAT-004"] = new InventoryItem { ItemSku = "MAT-004", QuantityOnHand = 10, ReorderPoint = 30, Location = "WH-C" }, // Low stock
                // Finished Goods
                ["PROD-001"] = new InventoryItem { ItemSku = "PROD-001", QuantityOnHand = 20, ReorderPoint = 10, Location = "FG-1" },
                ["PROD-002"] = new InventoryItem { ItemSku = "PROD-002", QuantityOnHand = 5, ReorderPoint = 5, Location = "FG-1" }
            };

            // --- Bills of Materials (BOMs) ---
            MockBomTable = new Dictionary<string, BillOfMaterials>
            {
                ["BOM-PROD-001"] = new BillOfMaterials
                {
                    BomId = "BOM-PROD-001",
                    FinishedGoodProductId = "PROD-001",
                    RequiredMaterials = new List<BomItem>
                    {
                        new BomItem { MaterialId = "MAT-001", Quantity = 4 }, // 4 Oak Planks
                        new BomItem { MaterialId = "MAT-002", Quantity = 1 }, // 1 Liter Varnish
                        new BomItem { MaterialId = "MAT-003", Quantity = 1 }, // 1 Box Screws
                        new BomItem { MaterialId = "MAT-004", Quantity = 1 }  // 1 Cushion
                    }
                },
                ["BOM-PROD-002"] = new BillOfMaterials
                {
                    BomId = "BOM-PROD-002",
                    FinishedGoodProductId = "PROD-002",
                    RequiredMaterials = new List<BomItem>
                    {
                        new BomItem { MaterialId = "MAT-001", Quantity = 10 }, // 10 Oak Planks
                        new BomItem { MaterialId = "MAT-002", Quantity = 2 }, // 2 Liters Varnish
                        new BomItem { MaterialId = "MAT-003", Quantity = 2 }  // 2 Boxes Screws
                    }
                }
            };

            // --- Order Tables ---
            MockPurchaseOrderTable = new List<PurchaseOrder>();
            MockWorkOrderTable = new List<WorkOrder>();

            ErpLogger.Info(ServiceName, "Mock data seeding complete.");
        }

        // --- Transaction Simulation ---
        public void BeginTransaction()
        {
            _isTransactionActive = true;
            ErpLogger.Info(ServiceName, "Transaction BEGAN.");
        }

        public void CommitTransaction()
        {
            _isTransactionActive = false;
            ErpLogger.Info(ServiceName, "Transaction COMMITTED.");
        }

        public void RollbackTransaction()
        {
            _isTransactionActive = false;
            ErpLogger.Warn(ServiceName, "Transaction ROLLED BACK.");
            // In a real app, we would restore state, but here we just log it.
        }

        // --- Mock Query Execution ---
        public async Task<T> QueryFirstOrDefaultAsync<T>(string sql, object param)
        {
            // Simulate async delay
            await Task.Delay(50); 
            ErpLogger.Info(ServiceName, $"Executing SQL Query: {sql.Trim().Split('\n')[0]}... (Params: {param})");

            // --- SQL Query Simulation ---
            // This is where the "interdependent SQL" logic is simulated.
            try
            {
                if (sql.Contains("SELECT * FROM Inventory WHERE ItemSku"))
                {
                    string sku = (param as dynamic).ItemSku;
                    if (MockInventoryTable.ContainsKey(sku))
                        return (T)(object)MockInventoryTable[sku];
                }
                if (sql.Contains("SELECT * FROM BillOfMaterials WHERE FinishedGoodProductId"))
                {
                    string pid = (param as dynamic).ProductId;
                    var bom = MockBomTable.Values.FirstOrDefault(b => b.FinishedGoodProductId == pid);
                    if (bom != null)
                        return (T)(object)bom;
                }
                if (sql.Contains("SELECT * FROM Suppliers WHERE PreferredMaterialId"))
                {
                    string mid = (param as dynamic).MaterialId;
                    var supplier = MockSupplierTable.Values.FirstOrDefault(s => s.PreferredMaterialId == mid);
                    if (supplier != null)
                        return (T)(object)supplier;
                }
                 if (sql.Contains("SELECT * FROM RawMaterials WHERE MaterialId"))
                {
                    string mid = (param as dynamic).MaterialId;
                    if (MockRawMaterialTable.ContainsKey(mid))
                        return (T)(object)MockRawMaterialTable[mid];
                }
            }
            catch (Exception ex)
            {
                ErpLogger.Error(ServiceName, $"Mock query failed for SQL: {sql}", ex);
            }
            
            return default(T); // Not found
        }

        public async Task<int> ExecuteAsync(string sql, object param)
        {
            // Simulate async delay
            await Task.Delay(30);
            ErpLogger.Info(ServiceName, $"Executing SQL Commit: {sql.Trim().Split('\n')[0]}... (Params: {param})");

            if (!_isTransactionActive)
            {
                ErpLogger.Error(ServiceName, "Attempted to execute a command outside of a transaction.");
                throw new InvalidOperationException("No active transaction.");
            }

            try
            {
                // --- UPDATE Inventory (Interdependent) ---
                if (sql.Contains("UPDATE Inventory SET QuantityOnHand"))
                {
                    string sku = (param as dynamic).ItemSku;
                    int newQuantity = (param as dynamic).Quantity;
                    if (MockInventoryTable.ContainsKey(sku))
                    {
                        var oldQty = MockInventoryTable[sku].QuantityOnHand;
                        MockInventoryTable[sku].QuantityOnHand = newQuantity;
                        ErpLogger.Info(ServiceName, $"Updated stock for {sku} from {oldQty} to {newQuantity}");
                        return 1; // 1 row affected
                    }
                }
                // --- INSERT Purchase Order ---
                if (sql.Contains("INSERT INTO PurchaseOrders"))
                {
                    var po = param as PurchaseOrder;
                    po.PurchaseOrderId = $"PO-{MockPurchaseOrderTable.Count + 1000}";
                    MockPurchaseOrderTable.Add(po);
                    ErpLogger.Info(ServiceName, $"Created {po.PurchaseOrderId} for supplier {po.SupplierId}");
                    return 1; // 1 row affected
                }
                // --- INSERT Work Order ---
                if (sql.Contains("INSERT INTO WorkOrders"))
                {
                    var wo = param as WorkOrder;
                    wo.WorkOrderId = $"WO-{MockWorkOrderTable.Count + 5000}";
                    MockWorkOrderTable.Add(wo);
                    ErpLogger.Info(ServiceName, $"Created {wo.WorkOrderId} to produce {wo.Quantity} of {wo.ProductId}");
                    return 1; // 1 row affected
                }
                // --- UPDATE Work Order Status (Interdependent) ---
                if (sql.Contains("UPDATE WorkOrders SET Status"))
                {
                     string woid = (param as dynamic).WorkOrderId;
                     string status = (param as dynamic).Status;
                     var wo = MockWorkOrderTable.FirstOrDefault(w => w.WorkOrderId == woid);
                     if(wo != null)
                     {
                         wo.Status = status;
                         ErpLogger.Info(ServiceName, $"Updated status for {woid} to {status}");
                         return 1;
                     }
                }
            }
            catch (Exception ex)
            {
                ErpLogger.Error(ServiceName, $"Mock execute failed for SQL: {sql}", ex);
                return 0;
            }

            return 0; // 0 rows affected
        }
    }

    // --- Data Models / Entities ---

    public class Product
    {
        public string ProductId { get; set; } // e.g., "PROD-001"
        public string Name { get; set; }
        public string Description { get; set; }
    }

    public class RawMaterial
    {
        public string MaterialId { get; set; } // e.g., "MAT-001"
        public string Name { get; set; }
        public string UnitOfMeasure { get; set; }
    }

    public class Supplier
    {
        public string SupplierId { get; set; }
        public string Name { get; set; }
        public string PreferredMaterialId { get; set; } // Simplification
    }

    public class InventoryItem
    {
        public string ItemSku { get; set; } // Can be ProductId or MaterialId
        public int QuantityOnHand { get; set; }
        public int ReorderPoint { get; set; }
        public string Location { get; set; }
    }

    public class BillOfMaterials
    {
        public string BomId { get; set; }
        public string FinishedGoodProductId { get; set; }
        public List<BomItem> RequiredMaterials { get; set; }
    }

    public class BomItem
    {
        public string MaterialId { get; set; }
        public int Quantity { get; set; }
    }

    public class PurchaseOrder
    {
        public string PurchaseOrderId { get; set; }
        public string SupplierId { get; set; }
        public string MaterialId { get; set; }
        public int Quantity { get; set; }
        public DateTime OrderDate { get; set; }
        public string Status { get; set; } // e.g., "Pending", "Submitted", "Received"
    }

    public class WorkOrder
    {
        public string WorkOrderId { get; set; }
        public string ProductId { get; set; }
        public int Quantity { get; set; }
        public DateTime CreationDate { get; set; }
        public string Status { get; set; } // e.g., "Pending", "InProgress", "Completed"
    }

    // --- Module: Inventory Service ---
    public class InventoryService
    {
        private readonly MockDbConnection _db;
        private static readonly string ServiceName = "Inventory";

        // Event for other services to subscribe to
        public delegate void StockLowEventHandler(string itemSku, int currentQuantity, int reorderPoint);
        public event StockLowEventHandler OnStockLow;

        public InventoryService(MockDbConnection db)
        {
            _db = db;
            ErpLogger.Info(ServiceName, "Service Initialized.");
        }

        public async Task<InventoryItem> GetStockLevelAsync(string itemSku)
        {
            // --- Interdependent SQL Query ---
            string sql = "SELECT * FROM Inventory WHERE ItemSku = @ItemSku;";
            var item = await _db.QueryFirstOrDefaultAsync<InventoryItem>(sql, new { ItemSku = itemSku });

            if (item == null)
            {
                ErpLogger.Warn(ServiceName, $"No inventory record found for SKU: {itemSku}");
                return new InventoryItem { ItemSku = itemSku, QuantityOnHand = 0, ReorderPoint = 0 };
            }

            // Check if stock is low and fire event
            if (item.QuantityOnHand <= item.ReorderPoint)
            {
                // Fire the event for subscribers (like ProcurementService)
                OnStockLow?.Invoke(item.ItemSku, item.QuantityOnHand, item.ReorderPoint);
            }

            return item;
        }

        public async Task<bool> AdjustStockAsync(string itemSku, int quantityChange)
        {
            ErpLogger.Info(ServiceName, $"Requesting stock adjustment for {itemSku}: {quantityChange:+#;-#;0}");
            // This function is transactional and interdependent
            
            // 1. Get current stock
            var item = await GetStockLevelAsync(itemSku);
            if (item == null && quantityChange < 0)
            {
                ErpLogger.Error(ServiceName, $"Cannot decrement stock for {itemSku}: Item does not exist.");
                return false;
            }
            
            int newQuantity = item.QuantityOnHand + quantityChange;
            
            if (newQuantity < 0)
            {
                ErpLogger.Error(ServiceName, $"Cannot adjust stock for {itemSku}: Not enough quantity on hand (Have: {item.QuantityOnHand}, Need: {Math.Abs(quantityChange)}).");
                return false;
            }

            // --- Interdependent SQL Query ---
            string sql = "UPDATE Inventory SET QuantityOnHand = @Quantity WHERE ItemSku = @ItemSku;";
            int rowsAffected = await _db.ExecuteAsync(sql, new { Quantity = newQuantity, ItemSku = itemSku });

            if(rowsAffected > 0)
            {
                ErpLogger.Info(ServiceName, $"Stock for {itemSku} adjusted to {newQuantity}.");
                // Check if the *new* quantity is low
                if (newQuantity <= item.ReorderPoint)
                {
                    OnStockLow?.Invoke(itemSku, newQuantity, item.ReorderPoint);
                }
                return true;
            }
            else
            {
                ErpLogger.Error(ServiceName, $"Failed to update stock for {itemSku} in database.");
                return false;
            }
        }
    }

    // --- Module: Supplier & Procurement Service ---
    public class ProcurementService
    {
        private readonly MockDbConnection _db;
        private readonly InventoryService _inventoryService; // Dependency
        private static readonly string ServiceName = "Procurement";
        private HashSet<string> _pendingOrders = new HashSet<string>(); // Prevent duplicate auto-orders

        public ProcurementService(MockDbConnection db, InventoryService inventoryService)
        {
            _db = db;
            _inventoryService = inventoryService;
            
            // --- Interdependent Event Subscription ---
            // Procurement subscribes to Inventory's OnStockLow event.
            _inventoryService.OnStockLow += HandleLowStockEvent;
            
            ErpLogger.Info(ServiceName, "Service Initialized. Subscribed to OnStockLow event.");
        }

        // Event handler for low stock
        private async void HandleLowStockEvent(string itemSku, int currentQuantity, int reorderPoint)
        {
            // Check if it's a raw material (this service only handles materials)
            if (itemSku.StartsWith("MAT-"))
            {
                ErpLogger.Warn(ServiceName, $"LOW STOCK EVENT: {itemSku} is at {currentQuantity} (Reorder point: {reorderPoint}).");
                
                // Check if an order is already pending to avoid duplicates
                if(_pendingOrders.Contains(itemSku))
                {
                    ErpLogger.Info(ServiceName, $"Auto-reorder for {itemSku} is already pending. Skipping.");
                    return;
                }
                
                await CreateAutoPurchaseOrderAsync(itemSku);
            }
        }

        public async Task<bool> CreateAutoPurchaseOrderAsync(string materialId)
        {
            if(_pendingOrders.Contains(materialId)) return false; // Already handling
            
            _pendingOrders.Add(materialId);
            ErpLogger.Info(ServiceName, $"Attempting to create auto-purchase order for {materialId}...");
            
            try
            {
                // 1. Get material details (for default reorder quantity, etc. - simplified here)
                // --- Interdependent SQL Query ---
                string matSql = "SELECT * FROM RawMaterials WHERE MaterialId = @MaterialId;";
                var material = await _db.QueryFirstOrDefaultAsync<RawMaterial>(matSql, new { MaterialId = materialId });
                if(material == null) throw new Exception($"Material {materialId} not found.");

                // 2. Find the preferred supplier for this material
                // --- Interdependent SQL Query ---
                string supSql = "SELECT * FROM Suppliers WHERE PreferredMaterialId = @MaterialId;";
                var supplier = await _db.QueryFirstOrDefaultAsync<Supplier>(supSql, new { MaterialId = materialId });
                if (supplier == null)
                {
                    throw new Exception($"No supplier found for material {materialId}.");
                }

                // 3. Create the Purchase Order
                var purchaseOrder = new PurchaseOrder
                {
                    SupplierId = supplier.SupplierId,
                    MaterialId = materialId,
                    Quantity = 100, // Mock reorder quantity
                    OrderDate = DateTime.UtcNow,
                    Status = "Pending"
                };

                // --- Interdependent SQL Query ---
                string poSql = "INSERT INTO PurchaseOrders (SupplierId, MaterialId, Quantity, OrderDate, Status) VALUES (...);";
                int rowsAffected = await _db.ExecuteAsync(poSql, purchaseOrder);

                if (rowsAffected > 0)
                {
                    ErpLogger.Info(ServiceName, $"Successfully created Purchase Order for {materialId} from {supplier.Name}.");
                    return true;
                }
                else
                {
                    throw new Exception("Failed to insert Purchase Order into database.");
                }
            }
            catch (Exception ex)
            {
                ErpLogger.Error(ServiceName, $"Failed to create auto-PO for {materialId}", ex);
                return false;
            }
            finally
            {
                _pendingOrders.Remove(materialId); // Allow new orders
            }
        }
    }

    // --- Module: Manufacturing Service ---
    public class ManufacturingService
    {
        private readonly MockDbConnection _db;
        private readonly InventoryService _inventoryService; // Dependency
        private static readonly string ServiceName = "Manufacturing";

        public ManufacturingService(MockDbConnection db, InventoryService inventoryService)
        {
            _db = db;
            _inventoryService = inventoryService;
            ErpLogger.Info(ServiceName, "Service Initialized.");
        }

        public async Task<WorkOrder> CreateWorkOrderAsync(string productId, int quantity)
        {
            ErpLogger.Info(ServiceName, $"Request to create Work Order for {quantity} of {productId}.");
            var wo = new WorkOrder
            {
                ProductId = productId,
                Quantity = quantity,
                CreationDate = DateTime.UtcNow,
                Status = "Pending"
            };

            // --- Interdependent SQL Query ---
            string sql = "INSERT INTO WorkOrders (ProductId, Quantity, CreationDate, Status) VALUES (...);";
            int rowsAffected = await _db.ExecuteAsync(sql, wo);
            
            if(rowsAffected > 0)
            {
                ErpLogger.Info(ServiceName, $"Work Order {wo.WorkOrderId} created.");
                return wo;
            }
            
            ErpLogger.Error(ServiceName, "Failed to create Work Order in database.");
            return null;
        }

        public async Task<bool> CompleteWorkOrderAsync(string workOrderId)
        {
            // This is a highly interdependent function.
            // It must:
            // 1. Fetch the Work Order
            // 2. Fetch the Bill of Materials for the product
            // 3. Consume the raw materials (call InventoryService)
            // 4. Create the finished goods (call InventoryService)
            // 5. Update the Work Order status
            
            ErpLogger.Info(ServiceName, $"Attempting to complete Work Order {workOrderId}...");
            
            // In a real app, we'd query the WO from the DB. We'll simulate finding it.
            // var workOrder = await _db.QueryFirstOrDefaultAsync<WorkOrder>(...);
            // For this sim, we assume workOrderId is valid and find it in the mock list
            var workOrder = (typeof(MockDbConnection).GetField("MockWorkOrderTable", 
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
                .GetValue(_db) as List<WorkOrder>)
                .FirstOrDefault(wo => wo.WorkOrderId == workOrderId);

            if (workOrder == null)
            {
                ErpLogger.Error(ServiceName, $"Work Order {workOrderId} not found.");
                return false;
            }

            if (workOrder.Status == "Completed")
            {
                ErpLogger.Warn(ServiceName, $"Work Order {workOrderId} is already completed.");
                return true;
            }

            // 1. Get the Bill of Materials
            // --- Interdependent SQL Query ---
            string bomSql = "SELECT * FROM BillOfMaterials WHERE FinishedGoodProductId = @ProductId;";
            var bom = await _db.QueryFirstOrDefaultAsync<BillOfMaterials>(bomSql, new { ProductId = workOrder.ProductId });

            if (bom == null)
            {
                ErpLogger.Error(ServiceName, $"No Bill of Materials found for product {workOrder.ProductId}. Cannot complete WO.");
                return false;
            }

            // 2. Consume Raw Materials (Interdependent call to InventoryService)
            ErpLogger.Info(ServiceName, $"Consuming raw materials for {workOrderId}...");
            foreach (var item in bom.RequiredMaterials)
            {
                int quantityToConsume = item.Quantity * workOrder.Quantity;
                bool success = await _inventoryService.AdjustStockAsync(item.MaterialId, -quantityToConsume);
                if (!success)
                {
                    // This is a critical failure. The transaction would roll back.
                    ErpLogger.Error(ServiceName, $"Failed to consume {quantityToConsume} of {item.MaterialId}. Rolling back WO completion.");
                    // We are already in a transaction (started by ProductionPlanner), so we just need to return false
                    // The caller (ProductionPlanner) will handle the rollback.
                    return false;
                }
            }
            ErpLogger.Info(ServiceName, $"All raw materials consumed for {workOrderId}.");

            // 3. Create Finished Goods (Interdependent call to InventoryService)
            ErpLogger.Info(ServiceName, $"Creating finished goods for {workOrderId}...");
            bool createSuccess = await _inventoryService.AdjustStockAsync(workOrder.ProductId, workOrder.Quantity);
            if (!createSuccess)
            {
                ErpLogger.Error(ServiceName, $"Failed to create finished goods {workOrder.ProductId}. Critical error. Rolling back.");
                return false;
            }
            ErpLogger.Info(ServiceName, $"{workOrder.Quantity} of {workOrder.ProductId} added to inventory.");

            // 4. Update Work Order Status
            // --- Interdependent SQL Query ---
            string updateSql = "UPDATE WorkOrders SET Status = @Status WHERE WorkOrderId = @WorkOrderId;";
            await _db.ExecuteAsync(updateSql, new { Status = "Completed", WorkOrderId = workOrderId });
            
            ErpLogger.Info(ServiceName, $"Work Order {workOrderId} marked as COMPLETED.");
            return true;
        }
    }

    // --- Module: Production Planning Service ---
    // This service ties all other services together.
    public class ProductionPlanner
    {
        private readonly MockDbConnection _db;
        private readonly InventoryService _inventoryService;
        private readonly ProcurementService _procurementService;
        private readonly ManufacturingService _manufacturingService;
        private static readonly string ServiceName = "ProductionPlanner";
        
        public ProductionPlanner(MockDbConnection db, InventoryService inventory, ProcurementService procurement, ManufacturingService manufacturing)
        {
            _db = db;
            _inventoryService = inventory;
            _procurementService = procurement;
            _manufacturingService = manufacturing;
            ErpLogger.Info(ServiceName, "Service Initialized. All dependencies injected.");
        }

        public async Task<Dictionary<string, int>> CheckMaterialAvailabilityAsync(string productId, int quantityToProduce)
        {
            ErpLogger.Info(ServiceName, $"Checking material availability for {quantityToProduce} of {productId}...");
            
            // 1. Get the Bill of Materials
            // --- Interdependent SQL Query ---
            string bomSql = "SELECT * FROM BillOfMaterials WHERE FinishedGoodProductId = @ProductId;";
            var bom = await _db.QueryFirstOrDefaultAsync<BillOfMaterials>(bomSql, new { ProductId = productId });
            if (bom == null)
            {
                ErpLogger.Error(ServiceName, $"No BOM for {productId}.");
                return null; // Or throw
            }

            var shortfalls = new Dictionary<string, int>();

            // 2. Check stock for each required material (Interdependent call)
            foreach (var item in bom.RequiredMaterials)
            {
                int required = item.Quantity * quantityToProduce;
                var stock = await _inventoryService.GetStockLevelAsync(item.MaterialId);
                
                if (stock.QuantityOnHand < required)
                {
                    int shortfall = required - stock.QuantityOnHand;
                    ErpLogger.Warn(ServiceName, $"SHORTFALL: Need {required} of {item.MaterialId}, have {stock.QuantityOnHand}. Short by {shortfall}.");
                    shortfalls[item.MaterialId] = shortfall;
                }
                else
                {
                     ErpLogger.Info(ServiceName, $"OK: Need {required} of {item.MaterialId}, have {stock.QuantityOnHand}.");
                }
            }

            return shortfalls;
        }

        public async Task<bool> ScheduleProductionRunAsync(string productId, int quantity)
        {
            ErpLogger.Info(ServiceName, $"--- NEW PRODUCTION RUN REQUESTED: {quantity} of {productId} ---");
            
            // This entire function MUST be transactional.
            _db.BeginTransaction();
            
            try
            {
                // 1. Check material availability (Interdependent call)
                var shortfalls = await CheckMaterialAvailabilityAsync(productId, quantity);
                
                if (shortfalls == null) throw new Exception("BOM not found.");
                
                if (shortfalls.Any())
                {
                    // Materials are NOT available.
                    ErpLogger.Error(ServiceName, $"Cannot schedule production: Material shortfall detected.");
                    
                    // 2. Trigger procurement for missing items (Interdependent call)
                    foreach (var shortfall in shortfalls)
                    {
                        ErpLogger.Info(ServiceName, $"Triggering emergency procurement for {shortfall.Key}...");
                        // This uses the event system implicitly, but we can also call it directly
                        await _procurementService.CreateAutoPurchaseOrderAsync(shortfall.Key);
                    }
                    
                    // We must fail the production run for now.
                    throw new Exception("Material shortfall. Purchase orders created. Reschedule when materials arrive.");
                }
                
                // 3. Materials ARE available. Create the Work Order.
                ErpLogger.Info(ServiceName, "All materials are available. Creating Work Order...");
                var workOrder = await _manufacturingService.CreateWorkOrderAsync(productId, quantity);
                if (workOrder == null)
                {
                    throw new Exception("Failed to create work order.");
                }
                
                // 4. Complete the Work Order (This consumes materials and creates goods)
                // This is the most complex interdependent call.
                ErpLogger.Info(ServiceName, $"Immediately processing Work Order {workOrder.WorkOrderId}...");
                bool success = await _manufacturingService.CompleteWorkOrderAsync(workOrder.WorkOrderId);
                
                if (!success)
                {
                    // This will happen if (e.g.) stock changed between our check and the consumption
                    throw new Exception("Failed to complete work order (e.g., race condition). Transaction will be rolled back.");
                }
                
                // 5. If all is successful, commit the transaction.
                _db.CommitTransaction();
                ErpLogger.Info(ServiceName, $"--- PRODUCTION RUN for {workOrder.WorkOrderId} COMPLETED SUCCESSFULLY ---");
                return true;
            }
            catch (Exception ex)
            {
                ErpLogger.Error(ServiceName, $"Production run failed: {ex.Message}");
                _db.RollbackTransaction();
                return false;
            }
        }
    }


    // --- Main Simulation Class ---
    public class ErpSimulation
    {
        public static async Task Run()
        {
            Console.WriteLine("--- ERP SYSTEM SIMULATION START ---");

            // --- 1. Dependency Injection ---
            // All services share the *same* database connection
            var db = new MockDbConnection("Server=erp.db.internal;Database=ProdERP;User=svc_erp;");
            
            var inventory = new InventoryService(db);
            var procurement = new ProcurementService(db, inventory);
            var manufacturing = new ManufacturingService(db, inventory);
            var planner = new ProductionPlanner(db, inventory, procurement, manufacturing);

            Console.WriteLine("\n--- All services initialized. ---\n");
            
            // --- 2. Simulation 1: Stock is low, triggers auto-reorder ---
            Console.WriteLine("--- SIMULATION 1: Check low stock item (MAT-004) ---");
            // The mock data for MAT-004 (Cushion) is 10, with reorder at 30.
            // Just *checking* the stock will trigger the OnStockLow event.
            _ = await inventory.GetStockLevelAsync("MAT-004");
            // This will (asynchronously) fire the event, which procurement handles.
            await Task.Delay(500); // Give time for async event to process
            Console.WriteLine("--- SIMULATION 1 Complete ---\n");
            
            
            // --- 3. Simulation 2: Schedule production run WITH shortfalls ---
            Console.WriteLine("--- SIMULATION 2: Schedule production of 20 chairs (PROD-001) ---");
            // BOM for 1 chair needs 1 cushion. We need 20 cushions.
            // We only have 10. This should fail and trigger another PO.
            bool run1Success = await planner.ScheduleProductionRunAsync("PROD-001", 20);
            Console.WriteLine($"Simulation 2 Result (Should Fail): {run1Success}");
            await Task.Delay(500);
            Console.WriteLine("--- SIMULATION 2 Complete ---\n");
            
            
            // --- 4. Simulation 3: Manually "Receive" materials ---
            Console.WriteLine("--- SIMULATION 3: Manually receive materials for POs ---");
            // We'll just adjust the stock directly, as if the POs were received.
            // We'll need 20 cushions for Sim 2. Let's add 50.
            // We also need 4 * 5 = 20 Oak Planks for Sim 4.
            db.BeginTransaction(); // Manual adjustment, needs a transaction
            await inventory.AdjustStockAsync("MAT-004", 50); // Add 50 cushions
            await inventory.AdjustStockAsync("MAT-001", 20); // Add 20 oak planks
            db.CommitTransaction();
            Console.WriteLine("--- SIMULATION 3 Complete ---\n");
            
            
            // --- 5. Simulation 4: Schedule production run, SUCCESS ---
            Console.WriteLine("--- SIMULATION 4: Re-schedule production of 5 chairs (PROD-001) ---");
            // BOM for 1 chair: 4 Oak, 1 Varnish, 1 ScrewBox, 1 Cushion
            // To make 5: 20 Oak, 5 Varnish, 5 ScrewBox, 5 Cushion
            // Stock:
            // MAT-001 (Oak): 100 + 20 = 120 (Have 120, Need 20) -> OK
            // MAT-002 (Varnish): 40 (Have 40, Need 5) -> OK
            // MAT-003 (Screws): 200 (Have 200, Need 5) -> OK
            // MAT-004 (Cushion): 10 + 50 = 60 (Have 60, Need 5) -> OK
            
            bool run2Success = await planner.ScheduleProductionRunAsync("PROD-001", 5);
            Console.WriteLine($"Simulation 4 Result (Should Succeed): {run2Success}");
            
            // Check final stock levels
            Console.WriteLine("\n--- Final Stock Levels Post-Production ---");
            var oak = await inventory.GetStockLevelAsync("MAT-001");
            Console.WriteLine($"MAT-001 (Oak) should be 100 (120 - 20): {oak.QuantityOnHand}");
            var cushion = await inventory.GetStockLevelAsync("MAT-004");
            Console.WriteLine($"MAT-004 (Cushion) should be 55 (60 - 5): {cushion.QuantityOnHand}");
            var chair = await inventory.GetStockLevelAsync("PROD-001");
            Console.WriteLine($"PROD-001 (Chair) should be 25 (20 + 5): {chair.QuantityOnHand}");
            
            Console.WriteLine("--- SIMULATION 4 Complete ---\n");
            
            
            Console.WriteLine("--- ERP SYSTEM SIMULATION END ---");
        }
    }
}