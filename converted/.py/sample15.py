import asyncio
import datetime
import math
from typing import List, Dict, Set, Optional, Callable, Awaitable

# --- Primary Namespace for the ERP System ---
# (Simulated by module)

# --- Logging Utility ---
class ErpLogger:
    
    @staticmethod
    def log(level: str, service: str, message: str):
        """Simple console logger"""
        log_entry = f"{datetime.datetime.utcnow().isoformat()}Z [{level.upper()}] [{service}] {message}"
        print(log_entry)

    @staticmethod
    def info(service: str, message: str):
        ErpLogger.log("INFO", service, message)

    @staticmethod
    def warn(service: str, message: str):
        ErpLogger.log("WARN", service, message)

    @staticmethod
    def error(service: str, message: str, ex: Optional[Exception] = None):
        error_msg = f"{message} | Exception: {ex}" if ex else message
        ErpLogger.log("ERROR", service, error_msg)

# --- Data Models / Entities ---

class Product:
    def __init__(self, ProductId: str = None, Name: str = None, Description: str = None):
        self.ProductId = ProductId
        self.Name = Name
        self.Description = Description

class RawMaterial:
    def __init__(self, MaterialId: str = None, Name: str = None, UnitOfMeasure: str = None):
        self.MaterialId = MaterialId
        self.Name = Name
        self.UnitOfMeasure = UnitOfMeasure

class Supplier:
    def __init__(self, SupplierId: str = None, Name: str = None, PreferredMaterialId: str = None):
        self.SupplierId = SupplierId
        self.Name = Name
        self.PreferredMaterialId = PreferredMaterialId

class InventoryItem:
    def __init__(self, ItemSku: str = None, QuantityOnHand: int = 0, ReorderPoint: int = 0, Location: str = None):
        self.ItemSku = ItemSku
        self.QuantityOnHand = QuantityOnHand
        self.ReorderPoint = ReorderPoint
        self.Location = Location

class BomItem:
    def __init__(self, MaterialId: str = None, Quantity: int = 0):
        self.MaterialId = MaterialId
        self.Quantity = Quantity

class BillOfMaterials:
    def __init__(self, BomId: str = None, FinishedGoodProductId: str = None, RequiredMaterials: List[BomItem] = None):
        self.BomId = BomId
        self.FinishedGoodProductId = FinishedGoodProductId
        self.RequiredMaterials = RequiredMaterials if RequiredMaterials is not None else []

class PurchaseOrder:
    def __init__(self, PurchaseOrderId: str = None, SupplierId: str = None, MaterialId: str = None, Quantity: int = 0, OrderDate: datetime.datetime = None, Status: str = None):
        self.PurchaseOrderId = PurchaseOrderId
        self.SupplierId = SupplierId
        self.MaterialId = MaterialId
        self.Quantity = Quantity
        self.OrderDate = OrderDate
        self.Status = Status

class WorkOrder:
    def __init__(self, WorkOrderId: str = None, ProductId: str = None, Quantity: int = 0, CreationDate: datetime.datetime = None, Status: str = None):
        self.WorkOrderId = WorkOrderId
        self.ProductId = ProductId
        self.Quantity = Quantity
        self.CreationDate = CreationDate
        self.Status = Status


# --- Mock Database Context ---
class MockDbConnection:
    
    _service_name = "Database"

    def __init__(self, connection_string: str):
        self._connection_string = connection_string
        self._is_transaction_active = False
        
        # Mock data tables to simulate the database state
        self._mock_inventory_table: Dict[str, InventoryItem] = {}
        self._mock_product_table: Dict[str, Product] = {}
        self._mock_raw_material_table: Dict[str, RawMaterial] = {}
        self._mock_supplier_table: Dict[str, Supplier] = {}
        self._mock_bom_table: Dict[str, BillOfMaterials] = {}
        self._mock_purchase_order_table: List[PurchaseOrder] = []
        self._mock_work_order_table: List[WorkOrder] = []
        
        ErpLogger.info(self._service_name, f"Initializing mock database connection: {self._connection_string}")
        self._initialize_mock_data()

    def _initialize_mock_data(self):
        ErpLogger.info(self._service_name, "Seeding mock data...")

        # --- Products (Finished Goods) ---
        self._mock_product_table = {
            "PROD-001": Product(ProductId="PROD-001", Name="Deluxe Wooden Chair", Description="A comfortable chair."),
            "PROD-002": Product(ProductId="PROD-002", Name="Large Oak Table", Description="A sturdy dining table.")
        }

        # --- Raw Materials ---
        self._mock_raw_material_table = {
            "MAT-001": RawMaterial(MaterialId="MAT-001", Name="Oak Wood Plank", UnitOfMeasure="plank"),
            "MAT-002": RawMaterial(MaterialId="MAT-002", Name="Varnish", UnitOfMeasure="liter"),
            "MAT-003": RawMaterial(MaterialId="MAT-003", Name="Wood Screw (Box)", UnitOfMeasure="box"),
            "MAT-004": RawMaterial(MaterialId="MAT-004", Name="Cushion", UnitOfMeasure="unit")
        }

        # --- Suppliers ---
        self._mock_supplier_table = {
            "SUP-1001": Supplier(SupplierId="SUP-1001", Name="Forestry Supplies Inc.", PreferredMaterialId="MAT-001"),
            "SUP-1002": Supplier(SupplierId="SUP-1002", Name="ChemCo", PreferredMaterialId="MAT-002"),
            "SUP-1003": Supplier(SupplierId="SUP-1003", Name="Hardware Express", PreferredMaterialId="MAT-003"),
            "SUP-1004": Supplier(SupplierId="SUP-1004", Name="Comfort Textiles", PreferredMaterialId="MAT-004")
        }

        # --- Inventory (Stock Levels) ---
        self._mock_inventory_table = {
            # Raw Materials
            "MAT-001": InventoryItem(ItemSku="MAT-001", QuantityOnHand=100, ReorderPoint=50, Location="WH-A"),
            "MAT-002": InventoryItem(ItemSku="MAT-002", QuantityOnHand=40, ReorderPoint=20, Location="WH-B"),
            "MAT-003": InventoryItem(ItemSku="MAT-003", QuantityOnHand=200, ReorderPoint=100, Location="WH-A"),
            "MAT-004": InventoryItem(ItemSku="MAT-004", QuantityOnHand=10, ReorderPoint=30, Location="WH-C"), # Low stock
            # Finished Goods
            "PROD-001": InventoryItem(ItemSku="PROD-001", QuantityOnHand=20, ReorderPoint=10, Location="FG-1"),
            "PROD-002": InventoryItem(ItemSku="PROD-002", QuantityOnHand=5, ReorderPoint=5, Location="FG-1")
        }

        # --- Bills of Materials (BOMs) ---
        self._mock_bom_table = {
            "BOM-PROD-001": BillOfMaterials(
                BomId="BOM-PROD-001",
                FinishedGoodProductId="PROD-001",
                RequiredMaterials=[
                    BomItem(MaterialId="MAT-001", Quantity=4), # 4 Oak Planks
                    BomItem(MaterialId="MAT-002", Quantity=1), # 1 Liter Varnish
                    BomItem(MaterialId="MAT-003", Quantity=1), # 1 Box Screws
                    BomItem(MaterialId="MAT-004", Quantity=1)  # 1 Cushion
                ]
            ),
            "BOM-PROD-002": BillOfMaterials(
                BomId="BOM-PROD-002",
                FinishedGoodProductId="PROD-002",
                RequiredMaterials=[
                    BomItem(MaterialId="MAT-001", Quantity=10), # 10 Oak Planks
                    BomItem(MaterialId="MAT-002", Quantity=2), # 2 Liters Varnish
                    BomItem(MaterialId="MAT-003", Quantity=2)  # 2 Boxes Screws
                ]
            )
        }

        # --- Order Tables ---
        self._mock_purchase_order_table = []
        self._mock_work_order_table = []

        ErpLogger.info(self._service_name, "Mock data seeding complete.")

    # --- Transaction Simulation ---
    def BeginTransaction(self):
        self._is_transaction_active = True
        ErpLogger.info(self._service_name, "Transaction BEGAN.")

    def CommitTransaction(self):
        self._is_transaction_active = False
        ErpLogger.info(self._service_name, "Transaction COMMITTED.")

    def RollbackTransaction(self):
        self._is_transaction_active = False
        ErpLogger.warn(self._service_name, "Transaction ROLLED BACK.")
        # In a real app, we would restore state, but here we just log it.

    # --- Mock Query Execution ---
    async def QueryFirstOrDefaultAsync(self, sql: str, param: dict):
        # Simulate async delay
        await asyncio.sleep(0.05)
        ErpLogger.info(self._service_name, f"Executing SQL Query: {sql.strip().splitlines()[0]}... (Params: {param})")

        # --- SQL Query Simulation ---
        try:
            if "SELECT * FROM Inventory WHERE ItemSku" in sql:
                sku = param["ItemSku"]
                if sku in self._mock_inventory_table:
                    return self._mock_inventory_table[sku]
            
            if "SELECT * FROM BillOfMaterials WHERE FinishedGoodProductId" in sql:
                pid = param["ProductId"]
                bom = next((b for b in self._mock_bom_table.values() if b.FinishedGoodProductId == pid), None)
                if bom:
                    return bom
                    
            if "SELECT * FROM Suppliers WHERE PreferredMaterialId" in sql:
                mid = param["MaterialId"]
                supplier = next((s for s in self._mock_supplier_table.values() if s.PreferredMaterialId == mid), None)
                if supplier:
                    return supplier
                    
            if "SELECT * FROM RawMaterials WHERE MaterialId" in sql:
                mid = param["MaterialId"]
                if mid in self._mock_raw_material_table:
                    return self._mock_raw_material_table[mid]
                    
        except Exception as ex:
            ErpLogger.error(self._service_name, f"Mock query failed for SQL: {sql}", ex)
        
        return None # Not found

    async def ExecuteAsync(self, sql: str, param) -> int:
        # Simulate async delay
        await asyncio.sleep(0.03)
        ErpLogger.info(self._service_name, f"Executing SQL Commit: {sql.strip().splitlines()[0]}... (Params: {param})")

        if not self._is_transaction_active:
            ErpLogger.error(self._service_name, "Attempted to execute a command outside of a transaction.")
            raise Exception("No active transaction.")

        try:
            # --- UPDATE Inventory (param is dict) ---
            if "UPDATE Inventory SET QuantityOnHand" in sql:
                sku = param["ItemSku"]
                new_quantity = param["Quantity"]
                if sku in self._mock_inventory_table:
                    old_qty = self._mock_inventory_table[sku].QuantityOnHand
                    self._mock_inventory_table[sku].QuantityOnHand = new_quantity
                    ErpLogger.info(self._service_name, f"Updated stock for {sku} from {old_qty} to {new_quantity}")
                    return 1 # 1 row affected
            
            # --- INSERT Purchase Order (param is PurchaseOrder object) ---
            if "INSERT INTO PurchaseOrders" in sql and isinstance(param, PurchaseOrder):
                po = param
                po.PurchaseOrderId = f"PO-{len(self._mock_purchase_order_table) + 1000}"
                self._mock_purchase_order_table.append(po)
                ErpLogger.info(self._service_name, f"Created {po.PurchaseOrderId} for supplier {po.SupplierId}")
                return 1 # 1 row affected
                
            # --- INSERT Work Order (param is WorkOrder object) ---
            if "INSERT INTO WorkOrders" in sql and isinstance(param, WorkOrder):
                wo = param
                wo.WorkOrderId = f"WO-{len(self._mock_work_order_table) + 5000}"
                self._mock_work_order_table.append(wo)
                ErpLogger.info(self._service_name, f"Created {wo.WorkOrderId} to produce {wo.Quantity} of {wo.ProductId}")
                return 1 # 1 row affected
                
            # --- UPDATE Work Order Status (param is dict) ---
            if "UPDATE WorkOrders SET Status" in sql:
                woid = param["WorkOrderId"]
                status = param["Status"]
                wo = next((w for w in self._mock_work_order_table if w.WorkOrderId == woid), None)
                if wo:
                    wo.Status = status
                    ErpLogger.info(self._service_name, f"Updated status for {woid} to {status}")
                    return 1
                    
        except Exception as ex:
            ErpLogger.error(self._service_name, f"Mock execute failed for SQL: {sql}", ex)
            return 0

        return 0 # 0 rows affected


# --- Module: Inventory Service ---
class InventoryService:
    
    _service_name = "Inventory"
    
    # Python equivalent of C# event
    # (itemSku: str, currentQuantity: int, reorderPoint: int)
    StockLowEventHandler = Callable[[str, int, int], Awaitable[None]]

    def __init__(self, db: MockDbConnection):
        self._db = db
        # Event for other services to subscribe to
        self.OnStockLow: List[InventoryService.StockLowEventHandler] = []
        ErpLogger.info(self._service_name, "Service Initialized.")

    def _FireOnStockLow(self, item_sku: str, current_quantity: int, reorder_point: int):
        """Fires the OnStockLow event, creating tasks for async handlers."""
        for handler in self.OnStockLow:
            # This replicates 'async void' by firing and forgetting the task
            asyncio.create_task(handler(item_sku, current_quantity, reorder_point))

    async def GetStockLevelAsync(self, itemSku: str) -> InventoryItem:
        # --- Interdependent SQL Query ---
        sql = "SELECT * FROM Inventory WHERE ItemSku = @ItemSku;"
        item = await self._db.QueryFirstOrDefaultAsync(sql, {"ItemSku": itemSku})

        if item is None:
            ErpLogger.warn(self._service_name, f"No inventory record found for SKU: {itemSku}")
            return InventoryItem(ItemSku=itemSku, QuantityOnHand=0, ReorderPoint=0)

        # Check if stock is low and fire event
        if item.QuantityOnHand <= item.ReorderPoint:
            # Fire the event for subscribers (like ProcurementService)
            self._FireOnStockLow(item.ItemSku, item.QuantityOnHand, item.ReorderPoint)
            
        return item

    async def AdjustStockAsync(self, itemSku: str, quantityChange: int) -> bool:
        ErpLogger.info(self._service_name, f"Requesting stock adjustment for {itemSku}: {quantityChange:+}")
        # This function is transactional and interdependent
        
        # 1. Get current stock
        item = await self.GetStockLevelAsync(itemSku)
        if item is None and quantityChange < 0:
            ErpLogger.error(self._service_name, f"Cannot decrement stock for {itemSku}: Item does not exist.")
            return False
            
        new_quantity = item.QuantityOnHand + quantityChange
        
        if new_quantity < 0:
            ErpLogger.error(self._service_name, f"Cannot adjust stock for {itemSku}: Not enough quantity on hand (Have: {item.QuantityOnHand}, Need: {abs(quantityChange)}).")
            return False

        # --- Interdependent SQL Query ---
        sql = "UPDATE Inventory SET QuantityOnHand = @Quantity WHERE ItemSku = @ItemSku;"
        rows_affected = await self._db.ExecuteAsync(sql, {"Quantity": new_quantity, "ItemSku": itemSku})

        if rows_affected > 0:
            ErpLogger.info(self._service_name, f"Stock for {itemSku} adjusted to {new_quantity}.")
            # Check if the *new* quantity is low
            if new_quantity <= item.ReorderPoint:
                self._FireOnStockLow(itemSku, new_quantity, item.ReorderPoint)
            return True
        else:
            ErpLogger.error(self._service_name, f"Failed to update stock for {itemSku} in database.")
            return False

# --- Module: Supplier & Procurement Service ---
class ProcurementService:
    
    _service_name = "Procurement"

    def __init__(self, db: MockDbConnection, inventoryService: InventoryService):
        self._db = db
        self._inventoryService = inventoryService # Dependency
        self._pendingOrders: Set[str] = set() # Prevent duplicate auto-orders
        
        # --- Interdependent Event Subscription ---
        # Procurement subscribes to Inventory's OnStockLow event.
        self._inventoryService.OnStockLow.append(self.HandleLowStockEvent)
        
        ErpLogger.info(self._service_name, "Service Initialized. Subscribed to OnStockLow event.")

    # Event handler for low stock (async void equivalent)
    async def HandleLowStockEvent(self, itemSku: str, currentQuantity: int, reorderPoint: int):
        # Check if it's a raw material (this service only handles materials)
        if itemSku.startswith("MAT-"):
            ErpLogger.warn(self._service_name, f"LOW STOCK EVENT: {itemSku} is at {currentQuantity} (Reorder point: {reorderPoint}).")
            
            # Check if an order is already pending to avoid duplicates
            if itemSku in self._pendingOrders:
                ErpLogger.info(self._service_name, f"Auto-reorder for {itemSku} is already pending. Skipping.")
                return
                
            await self.CreateAutoPurchaseOrderAsync(itemSku)

    async def CreateAutoPurchaseOrderAsync(self, materialId: str) -> bool:
        if materialId in self._pendingOrders:
            return False # Already handling
        
        self._pendingOrders.add(materialId)
        ErpLogger.info(self._service_name, f"Attempting to create auto-purchase order for {materialId}...")
        
        try:
            # 1. Get material details
            # --- Interdependent SQL Query ---
            mat_sql = "SELECT * FROM RawMaterials WHERE MaterialId = @MaterialId;"
            material = await self._db.QueryFirstOrDefaultAsync(mat_sql, {"MaterialId": materialId})
            if material is None:
                raise Exception(f"Material {materialId} not found.")

            # 2. Find the preferred supplier for this material
            # --- Interdependent SQL Query ---
            sup_sql = "SELECT * FROM Suppliers WHERE PreferredMaterialId = @MaterialId;"
            supplier = await self._db.QueryFirstOrDefaultAsync(sup_sql, {"MaterialId": materialId})
            if supplier is None:
                raise Exception(f"No supplier found for material {materialId}.")

            # 3. Create the Purchase Order
            purchase_order = PurchaseOrder(
                SupplierId=supplier.SupplierId,
                MaterialId=materialId,
                Quantity=100, # Mock reorder quantity
                OrderDate=datetime.datetime.utcnow(),
                Status="Pending"
            )

            # --- Interdependent SQL Query ---
            po_sql = "INSERT INTO PurchaseOrders (SupplierId, MaterialId, Quantity, OrderDate, Status) VALUES (...);"
            rows_affected = await self._db.ExecuteAsync(po_sql, purchase_order)

            if rows_affected > 0:
                ErpLogger.info(self._service_name, f"Successfully created Purchase Order for {materialId} from {supplier.Name}.")
                return True
            else:
                raise Exception("Failed to insert Purchase Order into database.")
                
        except Exception as ex:
            ErpLogger.error(self._service_name, f"Failed to create auto-PO for {materialId}", ex)
            return False
        finally:
            if materialId in self._pendingOrders:
                self._pendingOrders.remove(materialId) # Allow new orders

# --- Module: Manufacturing Service ---
class ManufacturingService:
    
    _service_name = "Manufacturing"

    def __init__(self, db: MockDbConnection, inventoryService: InventoryService):
        self._db = db
        self._inventoryService = inventoryService # Dependency
        ErpLogger.info(self._service_name, "Service Initialized.")

    async def CreateWorkOrderAsync(self, productId: str, quantity: int) -> Optional[WorkOrder]:
        ErpLogger.info(self._service_name, f"Request to create Work Order for {quantity} of {productId}.")
        wo = WorkOrder(
            ProductId=productId,
            Quantity=quantity,
            CreationDate=datetime.datetime.utcnow(),
            Status="Pending"
        )

        # --- Interdependent SQL Query ---
        sql = "INSERT INTO WorkOrders (ProductId, Quantity, CreationDate, Status) VALUES (...);"
        rows_affected = await self._db.ExecuteAsync(sql, wo)
        
        if rows_affected > 0:
            ErpLogger.info(self._service_name, f"Work Order {wo.WorkOrderId} created.")
            return wo
            
        ErpLogger.error(self._service_name, "Failed to create Work Order in database.")
        return None

    async def CompleteWorkOrderAsync(self, workOrderId: str) -> bool:
        # This is a highly interdependent function.
        ErpLogger.info(self._service_name, f"Attempting to complete Work Order {workOrderId}...")
        
        # In Python, we can directly access the (conventionally private) mock list
        work_order = next((wo for wo in self._db._mock_work_order_table if wo.WorkOrderId == workOrderId), None)

        if work_order is None:
            ErpLogger.error(self._service_name, f"Work Order {workOrderId} not found.")
            return False

        if work_order.Status == "Completed":
            ErpLogger.warn(self._service_name, f"Work Order {workOrderId} is already completed.")
            return True

        # 1. Get the Bill of Materials
        # --- Interdependent SQL Query ---
        bom_sql = "SELECT * FROM BillOfMaterials WHERE FinishedGoodProductId = @ProductId;"
        bom = await self._db.QueryFirstOrDefaultAsync(bom_sql, {"ProductId": work_order.ProductId})

        if bom is None:
            ErpLogger.error(self._service_name, f"No Bill of Materials found for product {work_order.ProductId}. Cannot complete WO.")
            return False

        # 2. Consume Raw Materials (Interdependent call to InventoryService)
        ErpLogger.info(self._service_name, f"Consuming raw materials for {workOrderId}...")
        for item in bom.RequiredMaterials:
            quantity_to_consume = item.Quantity * work_order.Quantity
            success = await self._inventoryService.AdjustStockAsync(item.MaterialId, -quantity_to_consume)
            if not success:
                # This is a critical failure. The transaction would roll back.
                ErpLogger.error(self._service_name, f"Failed to consume {quantity_to_consume} of {item.MaterialId}. Rolling back WO completion.")
                # The caller (ProductionPlanner) will handle the rollback.
                return False
                
        ErpLogger.info(self._service_name, f"All raw materials consumed for {workOrderId}.")

        # 3. Create Finished Goods (Interdependent call to InventoryService)
        ErpLogger.info(self._service_name, f"Creating finished goods for {workOrderId}...")
        create_success = await self._inventoryService.AdjustStockAsync(work_order.ProductId, work_order.Quantity)
        if not create_success:
            ErpLogger.error(self._service_name, f"Failed to create finished goods {work_order.ProductId}. Critical error. Rolling back.")
            return False
        ErpLogger.info(self._service_name, f"{work_order.Quantity} of {work_order.ProductId} added to inventory.")

        # 4. Update Work Order Status
        # --- Interdependent SQL Query ---
        update_sql = "UPDATE WorkOrders SET Status = @Status WHERE WorkOrderId = @WorkOrderId;"
        await self._db.ExecuteAsync(update_sql, {"Status": "Completed", "WorkOrderId": workOrderId})
        
        ErpLogger.info(self._service_name, f"Work Order {workOrderId} marked as COMPLETED.")
        return True


# --- Module: Production Planning Service ---
# This service ties all other services together.
class ProductionPlanner:

    _service_name = "ProductionPlanner"

    def __init__(self, db: MockDbConnection, inventory: InventoryService, procurement: ProcurementService, manufacturing: ManufacturingService):
        self._db = db
        self._inventoryService = inventory
        self._procurementService = procurement
        self._manufacturingService = manufacturing
        ErpLogger.info(self._service_name, "Service Initialized. All dependencies injected.")

    async def CheckMaterialAvailabilityAsync(self, productId: str, quantityToProduce: int) -> Optional[Dict[str, int]]:
        ErpLogger.info(self._service_name, f"Checking material availability for {quantityToProduce} of {productId}...")
        
        # 1. Get the Bill of Materials
        # --- Interdependent SQL Query ---
        bom_sql = "SELECT * FROM BillOfMaterials WHERE FinishedGoodProductId = @ProductId;"
        bom = await self._db.QueryFirstOrDefaultAsync(bom_sql, {"ProductId": productId})
        if bom is None:
            ErpLogger.error(self._service_name, f"No BOM for {productId}.")
            return None # Or raise

        shortfalls: Dict[str, int] = {}

        # 2. Check stock for each required material (Interdependent call)
        for item in bom.RequiredMaterials:
            required = item.Quantity * quantityToProduce
            stock = await self._inventoryService.GetStockLevelAsync(item.MaterialId)
            
            if stock.QuantityOnHand < required:
                shortfall = required - stock.QuantityOnHand
                ErpLogger.warn(self._service_name, f"SHORTFALL: Need {required} of {item.MaterialId}, have {stock.QuantityOnHand}. Short by {shortfall}.")
                shortfalls[item.MaterialId] = shortfall
            else:
                ErpLogger.info(self._service_name, f"OK: Need {required} of {item.MaterialId}, have {stock.QuantityOnHand}.")
                
        return shortfalls

    async def ScheduleProductionRunAsync(self, productId: str, quantity: int) -> bool:
        ErpLogger.info(self._service_name, f"--- NEW PRODUCTION RUN REQUESTED: {quantity} of {productId} ---")
        
        # This entire function MUST be transactional.
        self._db.BeginTransaction()
        
        try:
            # 1. Check material availability (Interdependent call)
            shortfalls = await self.CheckMaterialAvailabilityAsync(productId, quantity)
            
            if shortfalls is None:
                raise Exception("BOM not found.")
                
            if shortfalls:
                # Materials are NOT available.
                ErpLogger.error(self._service_name, "Cannot schedule production: Material shortfall detected.")
                
                # 2. Trigger procurement for missing items (Interdependent call)
                for material_id, shortfall_amount in shortfalls.items():
                    ErpLogger.info(self._service_name, f"Triggering emergency procurement for {material_id}...")
                    # This uses the event system implicitly, but we can also call it directly
                    await self._procurementService.CreateAutoPurchaseOrderAsync(material_id)
                    
                # We must fail the production run for now.
                raise Exception("Material shortfall. Purchase orders created. Reschedule when materials arrive.")
                
            # 3. Materials ARE available. Create the Work Order.
            ErpLogger.info(self._service_name, "All materials are available. Creating Work Order...")
            work_order = await self._manufacturingService.CreateWorkOrderAsync(productId, quantity)
            if work_order is None:
                raise Exception("Failed to create work order.")
                
            # 4. Complete the Work Order (This consumes materials and creates goods)
            # This is the most complex interdependent call.
            ErpLogger.info(self._service_name, f"Immediately processing Work Order {work_order.WorkOrderId}...")
            success = await self._manufacturingService.CompleteWorkOrderAsync(work_order.WorkOrderId)
            
            if not success:
                # This will happen if (e.g.) stock changed between our check and the consumption
                raise Exception("Failed to complete work order (e.g., race condition). Transaction will be rolled back.")
                
            # 5. If all is successful, commit the transaction.
            self._db.CommitTransaction()
            ErpLogger.info(self._service_name, f"--- PRODUCTION RUN for {work_order.WorkOrderId} COMPLETED SUCCESSFULLY ---")
            return True
            
        except Exception as ex:
            ErpLogger.error(self._service_name, f"Production run failed: {ex}")
            self._db.RollbackTransaction()
            return False


# --- Main Simulation Class ---
class ErpSimulation:
    
    @staticmethod
    async def Run():
        print("--- ERP SYSTEM SIMULATION START ---")

        # --- 1. Dependency Injection ---
        # All services share the *same* database connection
        db = MockDbConnection("Server=erp.db.internal;Database=ProdERP;User=svc_erp;")
        
        inventory = InventoryService(db)
        procurement = ProcurementService(db, inventory)
        manufacturing = ManufacturingService(db, inventory)
        planner = ProductionPlanner(db, inventory, procurement, manufacturing)

        print("\n--- All services initialized. ---\n")
        
        # --- 2. Simulation 1: Stock is low, triggers auto-reorder ---
        print("--- SIMULATION 1: Check low stock item (MAT-004) ---")
        # The mock data for MAT-004 (Cushion) is 10, with reorder at 30.
        # Just *checking* the stock will trigger the OnStockLow event.
        await inventory.GetStockLevelAsync("MAT-004")
        # This will (asynchronously) fire the event, which procurement handles.
        await asyncio.sleep(0.5) # Give time for async event to process
        print("--- SIMULATION 1 Complete ---\n")
        
        
        # --- 3. Simulation 2: Schedule production run WITH shortfalls ---
        print("--- SIMULATION 2: Schedule production of 20 chairs (PROD-001) ---")
        # BOM for 1 chair needs 1 cushion. We need 20 cushions.
        # We only have 10. This should fail and trigger another PO.
        run1_success = await planner.ScheduleProductionRunAsync("PROD-001", 20)
        print(f"Simulation 2 Result (Should Fail): {run1_success}")
        await asyncio.sleep(0.5)
        print("--- SIMULATION 2 Complete ---\n")
        
        
        # --- 4. Simulation 3: Manually "Receive" materials ---
        print("--- SIMULATION 3: Manually receive materials for POs ---")
        # We'll just adjust the stock directly, as if the POs were received.
        # We'll need 20 cushions for Sim 2. Let's add 50.
        # We also need 4 * 5 = 20 Oak Planks for Sim 4.
        db.BeginTransaction() # Manual adjustment, needs a transaction
        await inventory.AdjustStockAsync("MAT-004", 50) # Add 50 cushions
        await inventory.AdjustStockAsync("MAT-001", 20) # Add 20 oak planks
        db.CommitTransaction()
        print("--- SIMULATION 3 Complete ---\n")
        
        
        # --- 5. Simulation 4: Schedule production run, SUCCESS ---
        print("--- SIMULATION 4: Re-schedule production of 5 chairs (PROD-001) ---")
        # BOM for 1 chair: 4 Oak, 1 Varnish, 1 ScrewBox, 1 Cushion
        # To make 5: 20 Oak, 5 Varnish, 5 ScrewBox, 5 Cushion
        # Stock:
        # MAT-001 (Oak): 100 + 20 = 120 (Have 120, Need 20) -> OK
        # MAT-002 (Varnish): 40 (Have 40, Need 5) -> OK
        # MAT-003 (Screws): 200 (Have 200, Need 5) -> OK
        # MAT-004 (Cushion): 10 + 50 = 60 (Have 60, Need 5) -> OK
        
        run2_success = await planner.ScheduleProductionRunAsync("PROD-001", 5)
        print(f"Simulation 4 Result (Should Succeed): {run2_success}")
        
        # Check final stock levels
        print("\n--- Final Stock Levels Post-Production ---")
        oak = await inventory.GetStockLevelAsync("MAT-001")
        print(f"MAT-001 (Oak) should be 100 (120 - 20): {oak.QuantityOnHand}")
        cushion = await inventory.GetStockLevelAsync("MAT-004")
        print(f"MAT-004 (Cushion) should be 55 (60 - 5): {cushion.QuantityOnHand}")
        chair = await inventory.GetStockLevelAsync("PROD-001")
        print(f"PROD-001 (Chair) should be 25 (20 + 5): {chair.QuantityOnHand}")
        
        print("--- SIMULATION 4 Complete ---\n")
        
        
        print("--- ERP SYSTEM SIMULATION END ---")


# --- Main execution ---
async def main():
    await ErpSimulation.Run()

if __name__ == "__main__":
    asyncio.run(main())
