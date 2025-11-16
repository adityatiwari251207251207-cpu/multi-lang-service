/*
 * HospitalManagementSystem.java
 *
 * This is a monolithic file representing a complex, interdependent backend
 * for a hospital information system (HIS). It includes services for managing
 * patients, doctors, appointments, billing, and pharmacy.
 *
 * All services interact with a single database manager and often call
 * each other, creating a high degree of interdependence.
 *
 * For a real application, these would be in separate files and packages.
 *
 * Author: AI (Gemini)
 * Version: 1.0.0
 *
 * Note: This example uses an in-memory H2 database.
 * To run this, you would need the H2 database JAR (h2-*.jar) in your classpath.
 * (e.g., compile with `javac -cp .:"h2-*.jar" HospitalManagementSystem.java`
 * and run with `java -cp .:"h2-*.jar" HospitalManagementSystem`)
 */

// --- Imports ---
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.math.BigDecimal;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main class to run the Hospital Management System simulation.
 * This class initializes all services and demonstrates their interdependent usage.
 */
public class HospitalManagementSystem {

    private static final Logger LOGGER = Logger.getLogger(HospitalManagementSystem.class.getName());
    private static final String DB_URL = "jdbc:h2:mem:hospital;DB_CLOSE_DELAY=-1";
    private static final String DB_USER = "sa";
    private static final String DB_PASSWORD = "";

    public static void main(String[] args) {
        LOGGER.info("--- Starting Hospital Management System ---");

        try (Connection conn = DatabaseManager.getConnection()) {
            
            // 1. Set up the database schema
            // This is the first level of interdependence (foreign keys)
            SchemaBuilder.createTables(conn);
            LOGGER.info("Database schema created successfully.");

            // 2. Initialize all services
            // Services are interdependent (e.g., BillingService needs PatientService)
            PatientService patientService = new PatientService(conn);
            DoctorService doctorService = new DoctorService(conn);
            PharmacyService pharmacyService = new PharmacyService(conn);
            
            // BillingService is interdependent on PatientService and PharmacyService
            BillingService billingService = new BillingService(conn, patientService, pharmacyService);
            
            // AppointmentService is interdependent on PatientService and DoctorService
            AppointmentService appointmentService = new AppointmentService(conn, patientService, doctorService);
            
            // PatientService needs BillingService for discharge logic
            patientService.setBillingService(billingService); 
            
            // ReportingService needs all other services/tables
            ReportingService reportingService = new ReportingService(conn);

            LOGGER.info("All services initialized.");

            // 3. Run Demo Scenario
            runSimulation(patientService, doctorService, appointmentService, billingService, pharmacyService, reportingService);

        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Database connection failed.", e);
        } catch (DatabaseException e) {
            LOGGER.log(Level.SEVERE, "Failed to create schema.", e);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "An unexpected error occurred in main.", e);
        } finally {
            LOGGER.info("--- Hospital Management System Shutting Down ---");
        }
    }

    /**
     * Runs a simulation to demonstrate the interdependent service logic.
     */
    private static void runSimulation(PatientService patientService,
                                      DoctorService doctorService,
                                      AppointmentService appointmentService,
                                      BillingService billingService,
                                      PharmacyService pharmacyService,
                                      ReportingService reportingService) {
        
        LOGGER.info("--- Starting System Simulation ---");
        try {
            // --- Setup: Add Doctors and Medication ---
            LOGGER.info("[SIM] Adding doctors...");
            int drCardioId = doctorService.addDoctor("Elara", "Vance", "Cardiology");
            int drNeuroId = doctorService.addDoctor("Marcus", "Reed", "Neurology");
            int drOncoId = doctorService.addDoctor("Jina", "Chen", "Oncology");
            doctorService.setDoctorOnCallStatus(drCardioId, true);

            LOGGER.info("[SIM] Stocking pharmacy...");
            int aspirinId = pharmacyService.addMedication("Aspirin 100mg", new BigDecimal("10.50"), 1000);
            int chemoDrugId = pharmacyService.addMedication("Chemo-X 500mg", new BigDecimal("1200.00"), 50);
            int neuroDrugId = pharmacyService.addMedication("Neuro-Block 50mg", new BigDecimal("350.75"), 200);

            // --- Scenario 1: Admit Patient & Book Appointment ---
            LOGGER.info("[SIM] Admitting Patient 'John Doe'...");
            int johnDoeId = patientService.admitPatient("John", "Doe", LocalDate.of(1980, 5, 15), "O+", "Admitted for chest pains.");
            
            LOGGER.info("[SIM] Booking appointment for John Doe with Dr. Vance...");
            int apptId1 = appointmentService.bookAppointment(johnDoeId, drCardioId, LocalDateTime.now().plusDays(1), "Initial Consultation", new BigDecimal("150.00"));

            // --- Scenario 2: Admit another patient, book, and prescribe ---
            LOGGER.info("[SIM] Admitting Patient 'Jane Smith'...");
            int janeSmithId = patientService.admitPatient("Jane", "Smith", LocalDate.of(1992, 11, 2), "A-", "Scheduled for neurological exam.");
            
            LOGGER.info("[SIM] Booking appointment for Jane Smith with Dr. Reed...");
            int apptId2 = appointmentService.bookAppointment(janeSmithId, drNeuroId, LocalDateTime.now().plusDays(2), "Neurological Exam", new BigDecimal("450.00"));
            
            LOGGER.info("[SIM] Dr. Reed prescribes 'Neuro-Block' for Jane Smith...");
            // This call is interdependent: it checks doctor/patient, then adds a prescription,
            // which the billing service will later pick up.
            int prescId1 = pharmacyService.createPrescription(janeSmithId, drNeuroId, neuroDrugId, "1 tablet per day", 30);
            
            // This call is also interdependent: it checks stock and logs the fill.
            pharmacyService.fillPrescription(prescId1);
            LOGGER.info("[SIM] Pharmacy stock for Neuro-Block now: " + pharmacyService.getMedicationStock(neuroDrugId));

            // --- Scenario 3: Oncology Patient ---
            LOGGER.info("[SIM] Admitting Patient 'Bob Brown'...");
            int bobBrownId = patientService.admitPatient("Bob", "Brown", LocalDate.of(1965, 3, 20), "B+", "Oncology treatment.");
            int apptId3 = appointmentService.bookAppointment(bobBrownId, drOncoId, LocalDateTime.now().plusDays(1), "Chemotherapy Session 1", new BigDecimal("2500.00"));
            int prescId2 = pharmacyService.createPrescription(bobBrownId, drOncoId, chemoDrugId, "1 infusion", 1);
            pharmacyService.fillPrescription(prescId2);
            
            // --- Scenario 4: Discharge Patient (Triggers Billing) ---
            LOGGER.info("[SIM] Discharging Patient 'John Doe'...");
            // THIS IS A KEY INTERDEPENDENT CALL.
            // patientService.dischargePatient() -> triggers billingService.generateFinalInvoice()
            // which then queries appointments and prescriptions.
            patientService.dischargePatient(johnDoeId, "Stable, follow up in 2 weeks.");
            
            LOGGER.info("[SIM] 'John Doe' has been discharged.");
            
            // --- Scenario 5: Check Invoice and Pay Bill ---
            LOGGER.info("[SIM] Retrieving outstanding invoices for John Doe...");
            List<Invoice> johnsInvoices = billingService.getOutstandingInvoicesForPatient(johnDoeId);
            
            if (!johnsInvoices.isEmpty()) {
                Invoice invoice = johnsInvoices.get(0);
                LOGGER.info(String.format("[SIM] Found Invoice %s for $%.2f", invoice.invoiceUuid, invoice.totalAmount));
                
                // Get detailed invoice (shows interdependent SQL joins)
                Invoice detailedInvoice = billingService.getInvoiceDetails(invoice.invoiceId);
                LOGGER.info("[SIM] Invoice details: " + detailedInvoice); // Shows line items
                
                LOGGER.info("[SIM] Paying invoice...");
                billingService.markInvoiceAsPaid(invoice.invoiceId, "CC_TRANS_456789");
                LOGGER.info("[SIM] Invoice status: " + billingService.getInvoiceDetails(invoice.invoiceId).status);
            } else {
                LOGGER.warning("[SIM] No invoice was generated for John Doe.");
            }

            // --- Scenario 6: Run Reports (Complex Interdependent SQL) ---
            LOGGER.info("[SIM] --- Generating System Reports ---");
            
            // Report 1: Bed Occupancy
            BedOccupancyReport occupancyReport = reportingService.getBedOccupancyReport(100); // Assume 100 total beds
            LOGGER.info(String.format("[REPORT] Bed Occupancy: %d admitted / %d total (%.2f%%)", 
                occupancyReport.admittedPatients, occupancyReport.totalBeds, occupancyReport.occupancyRate * 100));

            // Report 2: Billing Report
            BillingSummaryReport billingReport = reportingService.getBillingSummaryByDateRange(LocalDate.now().minusDays(1), LocalDate.now().plusDays(5));
            LOGGER.info(String.format("[REPORT] Billing Summary: Total Billed: $%.2f, Total Paid: $%.2f, Outstanding: $%.2f",
                billingReport.totalBilled, billingReport.totalPaid, billingReport.totalOutstanding));

            // Report 3: Doctor Workload
            List<DoctorWorkload> workloadReport = reportingService.getDoctorWorkloadReport(LocalDate.now().minusDays(1), LocalDate.now().plusDays(5));
            LOGGER.info("[REPORT] Doctor Workload:");
            for (DoctorWorkload workload : workloadReport) {
                LOGGER.info(String.format("  - Dr. %s (%s): %d appointments, $%.2f revenue",
                    workload.doctorName, workload.specialty, workload.appointmentCount, workload.totalRevenue));
            }
            
            // Report 4: Pharmacy Stock
            List<MedicationStock> stockReport = reportingService.getLowStockMedicationReport(500);
            LOGGER.info("[REPORT] Low Stock Medications (Threshold 500):");
            for (MedicationStock stock : stockReport) {
                LOGGER.info(String.format("  - %s (ID: %d): %d remaining", stock.medicationName, stock.medicationId, stock.quantity));
            }

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "[SIM] Simulation failed.", e);
        }
    }
}

// =================================================================================
// --- UTILITY / CORE CLASSES ---
// =================================================================================

/**
 * DatabaseManager
 * Handles all low-level JDBC connections.
 */
class DatabaseManager {
    private static final String DB_URL = "jdbc:h2:mem:hospital;DB_CLOSE_DELAY=-1";
    private static final String DB_USER = "sa";
    private static final String DB_PASSWORD = "";
    private static final Logger LOGGER = Logger.getLogger(DatabaseManager.class.getName());

    static {
        try {
            // Load the H2 driver
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            LOGGER.log(Level.SEVERE, "Failed to load H2 driver.", e);
        }
    }

    /**
     * Gets a new connection to the database.
     * @return A new Connection object.
     * @throws SQLException if a database access error occurs.
     */
    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
    }

    /**
     * Executes an update query (INSERT, UPDATE, DELETE).
     * @param conn The connection to use.
     * @param sql The SQL query.
     * @param params The query parameters.
     * @return The number of rows affected.
     * @throws DatabaseException if the update fails.
     */
    public static int executeUpdate(Connection conn, String sql, Object... params) throws DatabaseException {
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            setParameters(pstmt, params);
            return pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new DatabaseException("Failed to execute update: " + e.getMessage(), e);
        }
    }

    /**
     * Executes an insert query and returns the generated key.
     * @param conn The connection to use.
     * @param sql The SQL query.
     * @param params The query parameters.
     * @return The generated primary key (ID).
     * @throws DatabaseException if the insert fails.
     */
    public static int executeInsertGetId(Connection conn, String sql, Object... params) throws DatabaseException {
        try (PreparedStatement pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            setParameters(pstmt, params);
            int affectedRows = pstmt.executeUpdate();

            if (affectedRows == 0) {
                throw new DatabaseException("Insert failed, no rows affected.");
            }

            try (ResultSet generatedKeys = pstmt.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    return generatedKeys.getInt(1);
                } else {
                    throw new DatabaseException("Insert failed, no ID obtained.");
                }
            }
        } catch (SQLException e) {
            throw new DatabaseException("Failed to execute insert: " + e.getMessage(), e);
        }
    }

    /**
     * Helper to set parameters on a PreparedStatement.
     * @param pstmt The statement.
     * @param params The parameters to set.
     * @throws SQLException if a parameter is invalid.
     */
    private static void setParameters(PreparedStatement pstmt, Object... params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            Object param = params[i];
            if (param instanceof String) {
                pstmt.setString(i + 1, (String) param);
            } else if (param instanceof Integer) {
                pstmt.setInt(i + 1, (Integer) param);
            } else if (param instanceof Long) {
                pstmt.setLong(i + 1, (Long) param);
            } else if (param instanceof BigDecimal) {
                pstmt.setBigDecimal(i + 1, (BigDecimal) param);
            } else if (param instanceof Date) {
                pstmt.setDate(i + 1, (Date) param);
            } else if (param instanceof Timestamp) {
                pstmt.setTimestamp(i + 1, (Timestamp) param);
            } else if (param instanceof Boolean) {
                pstmt.setBoolean(i + 1, (Boolean) param);
            } else if (param instanceof LocalDate) {
                pstmt.setDate(i + 1, Date.valueOf((LocalDate) param));
            } else if (param instanceof LocalDateTime) {
                pstmt.setTimestamp(i + 1, Timestamp.valueOf((LocalDateTime) param));
            } else if (param == null) {
                pstmt.setNull(i + 1, java.sql.Types.NULL);
            } else {
                throw new SQLException("Unsupported parameter type: " + param.getClass().getName());
            }
        }
    }
}

/**
 * SchemaBuilder
 * Contains all DDL (CREATE TABLE) statements, showing the
 * complex foreign key interdependencies.
 */
class SchemaBuilder {
    private static final Logger LOGGER = Logger.getLogger(SchemaBuilder.class.getName());

    public static void createTables(Connection conn) throws DatabaseException {
        // This list of SQL statements shows the interdependencies.
        // e.g., 'appointments' depends on 'patients' and 'doctors'.
        // 'invoice_items' depends on 'invoices', 'appointments', and 'prescriptions'.
        String[] createTableSQLs = {
            // Patients Table
            """
            CREATE TABLE patients (
                patient_id INT AUTO_INCREMENT PRIMARY KEY,
                first_name VARCHAR(100) NOT NULL,
                last_name VARCHAR(100) NOT NULL,
                date_of_birth DATE NOT NULL,
                blood_type VARCHAR(5),
                status VARCHAR(20) NOT NULL DEFAULT 'ADMITTED', -- ADMITTED, DISCHARGED, DECEASED
                admission_date TIMESTAMP NOT NULL,
                discharge_date TIMESTAMP,
                admission_notes TEXT
            );
            """,
            // Doctors Table
            """
            CREATE TABLE doctors (
                doctor_id INT AUTO_INCREMENT PRIMARY KEY,
                first_name VARCHAR(100) NOT NULL,
                last_name VARCHAR(100) NOT NULL,
                specialty VARCHAR(100) NOT NULL,
                on_call BOOLEAN DEFAULT false
            );
            """,
            // Medications Table (Pharmacy)
            """
            CREATE TABLE medications (
                medication_id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                unit_cost DECIMAL(10, 2) NOT NULL,
                stock_quantity INT NOT NULL DEFAULT 0 CHECK(stock_quantity >= 0)
            );
            """,
            // Appointments Table (Interdependent: patients, doctors)
            """
            CREATE TABLE appointments (
                appointment_id INT AUTO_INCREMENT PRIMARY KEY,
                patient_id INT NOT NULL,
                doctor_id INT NOT NULL,
                appointment_time TIMESTAMP NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'SCHEDULED', -- SCHEDULED, COMPLETED, CANCELLED
                notes TEXT,
                consultation_fee DECIMAL(10, 2) NOT NULL,
                invoice_id INT, -- To be filled when billed
                FOREIGN KEY (patient_id) REFERENCES patients(patient_id),
                FOREIGN KEY (doctor_id) REFERENCES doctors(doctor_id)
            );
            """,
            // Prescriptions Table (Interdependent: patients, doctors, medications)
            """
            CREATE TABLE prescriptions (
                prescription_id INT AUTO_INCREMENT PRIMARY KEY,
                patient_id INT NOT NULL,
                doctor_id INT NOT NULL,
                medication_id INT NOT NULL,
                dosage VARCHAR(255) NOT NULL,
                quantity INT NOT NULL,
                date_prescribed TIMESTAMP NOT NULL,
                is_filled BOOLEAN DEFAULT false,
                invoice_id INT, -- To be filled when billed
                FOREIGN KEY (patient_id) REFERENCES patients(patient_id),
                FOREIGN KEY (doctor_id) REFERENCES doctors(doctor_id),
                FOREIGN KEY (medication_id) REFERENCES medications(medication_id)
            );
            """,
            // Invoices Table (Interdependent: patients)
            """
            CREATE TABLE invoices (
                invoice_id INT AUTO_INCREMENT PRIMARY KEY,
                patient_id INT NOT NULL,
                invoice_uuid VARCHAR(36) UNIQUE NOT NULL,
                total_amount DECIMAL(10, 2) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'UNPAID', -- UNPAID, PAID, VOID
                date_issued TIMESTAMP NOT NULL,
                date_paid TIMESTAMP,
                payment_reference VARCHAR(100),
                FOREIGN KEY (patient_id) REFERENCES patients(patient_id)
            );
            """,
            // Invoice Line Items (Highly Interdependent: invoices, appointments, prescriptions)
            // This table links all billable events to a single invoice.
            """
            CREATE TABLE invoice_line_items (
                item_id INT AUTO_INCREMENT PRIMARY KEY,
                invoice_id INT NOT NULL,
                description VARCHAR(255) NOT NULL,
                amount DECIMAL(10, 2) NOT NULL,
                item_type VARCHAR(20) NOT NULL, -- CONSULTATION, MEDICATION, PROCEDURE
                reference_id INT, -- e.g., appointment_id or prescription_id
                FOREIGN KEY (invoice_id) REFERENCES invoices(invoice_id) ON DELETE CASCADE
            );
            """,
            // Link appointments and prescriptions to their invoices AFTER invoice creation
            // We do this by adding foreign key constraints back to the items tables
            "ALTER TABLE appointments ADD FOREIGN KEY (invoice_id) REFERENCES invoices(invoice_id);",
            "ALTER TABLE prescriptions ADD FOREIGN KEY (invoice_id) REFERENCES invoices(invoice_id);"
        };

        try (Statement stmt = conn.createStatement()) {
            for (String sql : createTableSQLs) {
                stmt.execute(sql);
            }
            LOGGER.info("All tables created successfully.");
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Failed to create tables.", e);
            throw new DatabaseException("Schema creation failed: " + e.getMessage(), e);
        }
    }
}

// =================================================================================
// --- CUSTOM EXCEPTIONS ---
// =================================================================================

class DatabaseException extends Exception {
    public DatabaseException(String message) {
        super(message);
    }
    public DatabaseException(String message, Throwable cause) {
        super(message, cause);
    }
}

class PatientNotFoundException extends Exception {
    public PatientNotFoundException(String message) {
        super(message);
    }
}

class DoctorNotFoundException extends Exception {
    public DoctorNotFoundException(String message) {
        super(message);
    }
}

class AppointmentBookingException extends Exception {
    public AppointmentBookingException(String message) {
        super(message);
    }
}

class BillingException extends Exception {
    public BillingException(String message) {
        super(message);
    }
}

class PharmacyException extends Exception {
    public PharmacyException(String message) {
        super(message);
    }
}

// =================================================================================
// --- DATA MODELS (POJOs) ---
// =================================================================================

class Patient {
    int patientId;
    String firstName, lastName;
    LocalDate dateOfBirth;
    String bloodType, status;
    LocalDateTime admissionDate, dischargeDate;
    String admissionNotes;

    // Simplified constructor
    public Patient(int id, String first, String last, String status) {
        this.patientId = id;
        this.firstName = first;
        this.lastName = last;
        this.status = status;
    }
    // Full constructor could be here
}

class Doctor {
    int doctorId;
    String firstName, lastName, specialty;
    boolean onCall;
    
    // Simplified constructor
    public Doctor(int id, String first, String last, String specialty) {
        this.doctorId = id;
        this.firstName = first;
        this.lastName = last;
        this.specialty = specialty;
    }
    // Full constructor could be here
}

class Appointment {
    int appointmentId, patientId, doctorId;
    LocalDateTime appointmentTime;
    String status, notes;
    BigDecimal consultationFee;
    Integer invoiceId;
    
    // Full constructor
    public Appointment(int apptId, int patId, int docId, LocalDateTime time, String status, String notes, BigDecimal fee, Integer invId) {
        this.appointmentId = apptId;
        this.patientId = patId;
        this.doctorId = docId;
        this.appointmentTime = time;
        this.status = status;
        this.notes = notes;
        this.consultationFee = fee;
        this.invoiceId = invId;
    }
    
    @Override
    public String toString() {
        return String.format("Appointment{id=%d, patientId=%d, doctorId=%d, time=%s, fee=%.2f}", 
            appointmentId, patientId, doctorId, appointmentTime, consultationFee);
    }
}

class Medication {
    int medicationId;
    String name;
    BigDecimal unitCost;
    int stockQuantity;
    
    // Constructor
    public Medication(int id, String name, BigDecimal cost, int stock) {
        this.medicationId = id;
        this.name = name;
        this.unitCost = cost;
        this.stockQuantity = stock;
    }
    
    @Override
    public String toString() {
        return String.format("Medication{id=%d, name='%s', cost=%.2f, stock=%d}", 
            medicationId, name, unitCost, stockQuantity);
    }
}

class Prescription {
    int prescriptionId, patientId, doctorId, medicationId, quantity;
    String dosage;
    LocalDateTime datePrescribed;
    boolean isFilled;
    Integer invoiceId;
    
    // Full constructor
    public Prescription(int id, int patId, int docId, int medId, int qty, String dosage, LocalDateTime date, boolean filled, Integer invId) {
        this.prescriptionId = id;
        this.patientId = patId;
        this.doctorId = docId;
        this.medicationId = medId;
        this.quantity = qty;
        this.dosage = dosage;
        this.datePrescribed = date;
        this.isFilled = filled;
        this.invoiceId = invId;
    }

    @Override
    public String toString() {
        return String.format("Prescription{id=%d, medicationId=%d, qty=%d}", 
            prescriptionId, medicationId, quantity);
    }
}

class Invoice {
    int invoiceId, patientId;
    String invoiceUuid, status, paymentReference;
    BigDecimal totalAmount;
    LocalDateTime dateIssued, datePaid;
    List<InvoiceLineItem> lineItems = new ArrayList<>();
    
    // Simplified constructor from DB
    public Invoice(int id, int patId, String uuid, String status, BigDecimal total, LocalDateTime issued) {
        this.invoiceId = id;
        this.patientId = patId;
        this.invoiceUuid = uuid;
        this.status = status;
        this.totalAmount = total;
        this.dateIssued = issued;
    }

    @Override
    public String toString() {
        return String.format("Invoice{id=%d, uuid=%s, total=%.2f, status=%s, items=%d}", 
            invoiceId, invoiceUuid, totalAmount, status, lineItems.size());
    }
}

class InvoiceLineItem {
    int itemId, invoiceId, referenceId;
    String description, itemType;
    BigDecimal amount;
    
    // Constructor
    public InvoiceLineItem(int id, int invId, String desc, BigDecimal amt, String type, int refId) {
        this.itemId = id;
        this.invoiceId = invId;
        this.description = desc;
        this.amount = amt;
        this.itemType = type;
        this.referenceId = refId;
    }
    
    @Override
    public String toString() {
        return String.format("LineItem{desc='%s', amount=%.2f}", description, amount);
    }
}

// --- REPORTING MODELS ---

class BedOccupancyReport {
    int admittedPatients;
    int totalBeds;
    double occupancyRate;
    
    public BedOccupancyReport(int admitted, int total) {
        this.admittedPatients = admitted;
        this.totalBeds = total;
        this.occupancyRate = (total > 0) ? (double)admitted / total : 0.0;
    }
}

class BillingSummaryReport {
    BigDecimal totalBilled;
    BigDecimal totalPaid;
    BigDecimal totalOutstanding;
    
    public BillingSummaryReport(BigDecimal billed, BigDecimal paid) {
        this.totalBilled = billed != null ? billed : BigDecimal.ZERO;
        this.totalPaid = paid != null ? paid : BigDecimal.ZERO;
        this.totalOutstanding = this.totalBilled.subtract(this.totalPaid);
    }
}

class DoctorWorkload {
    int doctorId;
    String doctorName, specialty;
    int appointmentCount;
    BigDecimal totalRevenue;
    
    public DoctorWorkload(int id, String name, String spec, int count, BigDecimal revenue) {
        this.doctorId = id;
        this.doctorName = name;
        this.specialty = spec;
        this.appointmentCount = count;
        this.totalRevenue = revenue != null ? revenue : BigDecimal.ZERO;
    }
}

class MedicationStock {
    int medicationId;
    String medicationName;
    int quantity;
    
    public MedicationStock(int id, String name, int qty) {
        this.medicationId = id;
        this.medicationName = name;
        this.quantity = qty;
    }
}


// =================================================================================
// --- SERVICE CLASSES ---
// =================================================================================

/**
 * PatientService
 * Manages patient admissions, records, and discharge.
 * Interdependent on BillingService.
 */
class PatientService {
    private Connection conn;
    private BillingService billingService; // Dependency
    private static final Logger LOGGER = Logger.getLogger(PatientService.class.getName());

    public PatientService(Connection conn) {
        this.conn = conn;
    }

    // Setter for circular dependency (Patient -> Billing -> Patient)
    public void setBillingService(BillingService billingService) {
        this.billingService = billingService;
    }

    /**
     * Admits a new patient to the hospital.
     */
    public int admitPatient(String firstName, String lastName, LocalDate dob, String bloodType, String notes) throws DatabaseException {
        String sql = """
        INSERT INTO patients (first_name, last_name, date_of_birth, blood_type, status, admission_date, admission_notes)
        VALUES (?, ?, ?, ?, 'ADMITTED', ?, ?)
        """;
        try {
            return DatabaseManager.executeInsertGetId(conn, sql, 
                firstName, lastName, dob, bloodType, LocalDateTime.now(), notes);
        } catch (DatabaseException e) {
            LOGGER.log(Level.SEVERE, "Failed to admit patient " + firstName, e);
            throw e;
        }
    }

    /**
     * Discharges a patient.
     * This is a key INTERDEPENDENT method. It updates the patient's status
     * and then calls the BillingService to generate the final invoice.
     */
    public void dischargePatient(int patientId, String dischargeNotes) throws PatientNotFoundException, BillingException, DatabaseException {
        if (billingService == null) {
            throw new IllegalStateException("BillingService dependency not set.");
        }

        // First, verify patient exists and is admitted
        Patient patient = getPatientById(patientId);
        if (!"ADMITTED".equals(patient.status)) {
            throw new DatabaseException("Patient is not currently admitted.");
        }

        // Use a transaction
        try {
            conn.setAutoCommit(false);
            
            // Step 1: Update patient record
            String sqlUpdate = "UPDATE patients SET status = 'DISCHARGED', discharge_date = ? WHERE patient_id = ?";
            DatabaseManager.executeUpdate(conn, sqlUpdate, LocalDateTime.now(), patientId);
            LOGGER.info("Patient " + patientId + " status set to DISCHARGED.");
            
            // Step 2: Call BillingService to generate the final invoice
            // This is the interdependent call.
            LOGGER.info("Calling BillingService to generate final invoice for patient " + patientId);
            Invoice finalInvoice = billingService.generateFinalInvoiceForPatient(patientId);
            LOGGER.info("Generated final invoice " + finalInvoice.invoiceUuid + " for patient " + patientId);

            conn.commit();
        } catch (Exception e) {
            try {
                conn.rollback();
            } catch (SQLException ex) {
                LOGGER.log(Level.SEVERE, "Failed to rollback discharge transaction.", ex);
            }
            LOGGER.log(Level.SEVERE, "Failed to discharge patient " + patientId, e);
            if (e instanceof BillingException) throw (BillingException) e;
            if (e instanceof DatabaseException) throw (DatabaseException) e;
            throw new DatabaseException("Discharge failed.", e);
        } finally {
            try {
                conn.setAutoCommit(true);
            } catch (SQLException e) {
                LOGGER.log(Level.WARNING, "Failed to reset auto-commit.", e);
            }
        }
    }

    /**
     * Retrieves a patient by their ID.
     */
    public Patient getPatientById(int patientId) throws PatientNotFoundException, DatabaseException {
        String sql = "SELECT * FROM patients WHERE patient_id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, patientId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return new Patient(
                        rs.getInt("patient_id"),
                        rs.getString("first_name"),
                        rs.getString("last_name"),
                        rs.getString("status")
                        // ... map other fields
                    );
                } else {
                    throw new PatientNotFoundException("Patient not found with ID: " + patientId);
                }
            }
        } catch (SQLException e) {
            throw new DatabaseException("Failed to get patient: " + e.getMessage(), e);
        }
    }
}

/**
 * DoctorService
 * Manages doctor profiles and schedules.
 */
class DoctorService {
    private Connection conn;
    private static final Logger LOGGER = Logger.getLogger(DoctorService.class.getName());

    public DoctorService(Connection conn) {
        this.conn = conn;
    }

    /**
     * Adds a new doctor to the system.
     */
    public int addDoctor(String firstName, String lastName, String specialty) throws DatabaseException {
        String sql = "INSERT INTO doctors (first_name, last_name, specialty) VALUES (?, ?, ?)";
        try {
            return DatabaseManager.executeInsertGetId(conn, sql, firstName, lastName, specialty);
        } catch (DatabaseException e) {
            LOGGER.log(Level.SEVERE, "Failed to add doctor " + firstName, e);
            throw e;
        }
    }

    /**
     * Retrieves a doctor by their ID.
     */
    public Doctor getDoctorById(int doctorId) throws DoctorNotFoundException, DatabaseException {
        String sql = "SELECT * FROM doctors WHERE doctor_id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, doctorId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return new Doctor(
                        rs.getInt("doctor_id"),
                        rs.getString("first_name"),
                        rs.getString("last_name"),
                        rs.getString("specialty")
                        // ... map other fields
                    );
                } else {
                    throw new DoctorNotFoundException("Doctor not found with ID: " + doctorId);
                }
            }
        } catch (SQLException e) {
            throw new DatabaseException("Failed to get doctor: " + e.getMessage(), e);
        }
    }

    /**
     * Sets a doctor's on-call status.
     */
    public void setDoctorOnCallStatus(int doctorId, boolean onCall) throws DoctorNotFoundException, DatabaseException {
        String sql = "UPDATE doctors SET on_call = ? WHERE doctor_id = ?";
        int rows = DatabaseManager.executeUpdate(conn, sql, onCall, doctorId);
        if (rows == 0) {
            throw new DoctorNotFoundException("Doctor not found with ID: " + doctorId);
        }
    }
}

/**
 * AppointmentService
 * Manages booking and cancellation of appointments.
 * Interdependent on PatientService and DoctorService.
 */
class AppointmentService {
    private Connection conn;
    private PatientService patientService; // Dependency
    private DoctorService doctorService; // Dependency
    private static final Logger LOGGER = Logger.getLogger(AppointmentService.class.getName());

    public AppointmentService(Connection conn, PatientService patientService, DoctorService doctorService) {
        this.conn = conn;
        this.patientService = patientService; // Injected dependency
        this.doctorService = doctorService;   // Injected dependency
    }

    /**
     * Books a new appointment.
     * Interdependent: Checks for patient and doctor existence first.
     */
    public int bookAppointment(int patientId, int doctorId, LocalDateTime appointmentTime, String notes, BigDecimal fee) 
        throws PatientNotFoundException, DoctorNotFoundException, AppointmentBookingException, DatabaseException {
        
        // Interdependent checks
        patientService.getPatientById(patientId); // Throws if not found
        doctorService.getDoctorById(doctorId);   // Throws if not found

        // Check for double booking (complex SQL logic)
        String checkSql = """
        SELECT COUNT(*) FROM appointments 
        WHERE doctor_id = ? 
          AND status IN ('SCHEDULED', 'COMPLETED') 
          AND appointment_time = ?
        """;
        try (PreparedStatement pstmt = conn.prepareStatement(checkSql)) {
            pstmt.setInt(1, doctorId);
            pstmt.setTimestamp(2, Timestamp.valueOf(appointmentTime));
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next() && rs.getInt(1) > 0) {
                    throw new AppointmentBookingException("Doctor is already booked at this time.");
                }
            }
        } catch (SQLException e) {
            throw new DatabaseException("Failed to check appointment availability.", e);
        }

        // Create the appointment
        String insertSql = """
        INSERT INTO appointments (patient_id, doctor_id, appointment_time, notes, consultation_fee, status)
        VALUES (?, ?, ?, ?, ?, 'SCHEDULED')
        """;
        try {
            return DatabaseManager.executeInsertGetId(conn, insertSql, 
                patientId, doctorId, appointmentTime, notes, fee);
        } catch (DatabaseException e) {
            LOGGER.log(Level.SEVERE, "Failed to book appointment for patient " + patientId, e);
            throw e;
        }
    }

    /**
     * Cancels an existing appointment.
     */
    public void cancelAppointment(int appointmentId) throws AppointmentBookingException, DatabaseException {
        // We must check if the appointment has already been billed.
        String checkSql = "SELECT status, invoice_id FROM appointments WHERE appointment_id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(checkSql)) {
            pstmt.setInt(1, appointmentId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    if (rs.getInt("invoice_id") > 0) {
                        throw new AppointmentBookingException("Cannot cancel an appointment that has already been invoiced.");
                    }
                    if ("CANCELLED".equals(rs.getString("status"))) {
                        return; // Already cancelled
                    }
                } else {
                    throw new AppointmentBookingException("Appointment not found: " + appointmentId);
                }
            }
        } catch (SQLException e) {
            throw new DatabaseException("Failed to check appointment status.", e);
        }
        
        String updateSql = "UPDATE appointments SET status = 'CANCELLED' WHERE appointment_id = ?";
        DatabaseManager.executeUpdate(conn, updateSql, appointmentId);
    }
    
    // ... other methods like getAppointmentsForPatient, getScheduleForDoctor, etc.
}

/**
 * PharmacyService
 * Manages medication inventory and prescriptions.
 * Interdependent on PatientService and DoctorService (for validation).
 */
class PharmacyService {
    private Connection conn;
    private static final Logger LOGGER = Logger.getLogger(PharmacyService.class.getName());

    public PharmacyService(Connection conn) {
        this.conn = conn;
    }

    public int addMedication(String name, BigDecimal unitCost, int initialStock) throws DatabaseException {
        String sql = "INSERT INTO medications (name, unit_cost, stock_quantity) VALUES (?, ?, ?)";
        return DatabaseManager.executeInsertGetId(conn, sql, name, unitCost, initialStock);
    }
    
    public Medication getMedicationById(int medicationId) throws PharmacyException, DatabaseException {
        String sql = "SELECT * FROM medications WHERE medication_id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, medicationId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return new Medication(
                        rs.getInt("medication_id"),
                        rs.getString("name"),
                        rs.getBigDecimal("unit_cost"),
                        rs.getInt("stock_quantity")
                    );
                } else {
                    throw new PharmacyException("Medication not found: " + medicationId);
                }
            }
        } catch (SQLException e) {
            throw new DatabaseException("Failed to get medication.", e);
        }
    }

    public int getMedicationStock(int medicationId) throws PharmacyException, DatabaseException {
        String sql = "SELECT stock_quantity FROM medications WHERE medication_id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, medicationId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("stock_quantity");
                } else {
                    throw new PharmacyException("Medication not found: " + medicationId);
                }
            }
        } catch (SQLException e) {
            throw new DatabaseException("Failed to get medication stock.", e);
        }
    }

    /**
     * Creates a new prescription.
     * Does not fill it or decrease stock.
     */
    public int createPrescription(int patientId, int doctorId, int medicationId, String dosage, int quantity) throws DatabaseException {
        // In a real system, we'd use PatientService/DoctorService to validate IDs
        String sql = """
        INSERT INTO prescriptions (patient_id, doctor_id, medication_id, dosage, quantity, date_prescribed)
        VALUES (?, ?, ?, ?, ?, ?)
        """;
        return DatabaseManager.executeInsertGetId(conn, sql, 
            patientId, doctorId, medicationId, dosage, quantity, LocalDateTime.now());
    }

    /**
     * Fills a prescription, decreasing medication stock.
     * This is an interdependent, transactional operation.
     */
    public void fillPrescription(int prescriptionId) throws PharmacyException, DatabaseException {
        String getSql = "SELECT medication_id, quantity, is_filled FROM prescriptions WHERE prescription_id = ?";
        
        try {
            conn.setAutoCommit(false);
            
            int medicationId, quantity;
            try (PreparedStatement pstmt = conn.prepareStatement(getSql)) {
                pstmt.setInt(1, prescriptionId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (!rs.next()) {
                        throw new PharmacyException("Prescription not found: " + prescriptionId);
                    }
                    if (rs.getBoolean("is_filled")) {
                        throw new PharmacyException("Prescription already filled.");
                    }
                    medicationId = rs.getInt("medication_id");
                    quantity = rs.getInt("quantity");
                }
            }
            
            // Step 1: Update stock (interdependent SQL)
            // This SQL ensures we don't go below zero atomically.
            String stockSql = """
            UPDATE medications SET stock_quantity = stock_quantity - ? 
            WHERE medication_id = ? AND stock_quantity >= ?
            """;
            int rowsAffected = DatabaseManager.executeUpdate(conn, stockSql, quantity, medicationId, quantity);
            
            if (rowsAffected == 0) {
                // Failed to update, check why
                int currentStock = getMedicationStock(medicationId);
                throw new PharmacyException(String.format(
                    "Insufficient stock for medication %d. Required: %d, Available: %d",
                    medicationId, quantity, currentStock));
            }

            // Step 2: Mark prescription as filled
            String fillSql = "UPDATE prescriptions SET is_filled = true WHERE prescription_id = ?";
            DatabaseManager.executeUpdate(conn, fillSql, prescriptionId);

            conn.commit();
            LOGGER.info("Filled prescription " + prescriptionId);
        } catch (Exception e) {
            try {
                conn.rollback();
            } catch (SQLException ex) {
                LOGGER.log(Level.SEVERE, "Failed to rollback fill transaction.", ex);
            }
            if (e instanceof PharmacyException) throw (PharmacyException) e;
            throw new DatabaseException("Failed to fill prescription.", e);
        } finally {
            try {
                conn.setAutoCommit(true);
            } catch (SQLException e) {
                LOGGER.log(Level.WARNING, "Failed to reset auto-commit.", e);
            }
        }
    }
    
    // ... other methods like getPrescriptionsForPatient
}

/**
 * BillingService
 * Manages invoices and payments.
 * Highly interdependent on PatientService, AppointmentService, and PharmacyService.
 */
class BillingService {
    private Connection conn;
    private PatientService patientService; // Dependency
    private PharmacyService pharmacyService; // Dependency
    private static final Logger LOGGER = Logger.getLogger(BillingService.class.getName());

    public BillingService(Connection conn, PatientService patientService, PharmacyService pharmacyService) {
        this.conn = conn;
        this.patientService = patientService;
        this.pharmacyService = pharmacyService;
    }

    /**
     * Generates the final invoice for a patient.
     * This is called by PatientService.dischargePatient().
     * This method contains highly interdependent SQL queries.
     */
    public Invoice generateFinalInvoiceForPatient(int patientId) throws BillingException, DatabaseException {
        // Verify patient
        try {
            patientService.getPatientById(patientId);
        } catch (PatientNotFoundException e) {
            throw new BillingException("Cannot bill non-existent patient.", e);
        }
        
        String invoiceUuid = UUID.randomUUID().toString();
        BigDecimal totalAmount = BigDecimal.ZERO;

        try {
            conn.setAutoCommit(false);
            
            // Step 1: Create the parent invoice record to get an ID
            String invSql = "INSERT INTO invoices (patient_id, invoice_uuid, total_amount, status, date_issued) VALUES (?, ?, ?, 'UNPAID', ?)";
            int invoiceId = DatabaseManager.executeInsertGetId(conn, invSql,
                patientId, invoiceUuid, BigDecimal.ZERO, LocalDateTime.now());

            // Step 2: Find all unbilled APPOINTMENTS (interdependent query)
            String apptSql = "SELECT * FROM appointments WHERE patient_id = ? AND invoice_id IS NULL AND status = 'COMPLETED'";
            // In a real system, we'd mark appointments 'COMPLETED'
            // For this demo, we'll bill all 'SCHEDULED' ones
            apptSql = "SELECT * FROM appointments WHERE patient_id = ? AND invoice_id IS NULL";
            
            try (PreparedStatement pstmt = conn.prepareStatement(apptSql)) {
                pstmt.setInt(1, patientId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        Appointment appt = new Appointment(
                            rs.getInt("appointment_id"), patientId, rs.getInt("doctor_id"),
                            rs.getTimestamp("appointment_time").toLocalDateTime(), rs.getString("status"),
                            rs.getString("notes"), rs.getBigDecimal("consultation_fee"), null
                        );
                        
                        // Add line item
                        addLineItem(invoiceId, "Consultation: " + appt.notes, appt.consultationFee, "CONSULTATION", appt.appointmentId);
                        totalAmount = totalAmount.add(appt.consultationFee);
                        
                        // Link appointment to invoice (interdependent update)
                        DatabaseManager.executeUpdate(conn, "UPDATE appointments SET invoice_id = ? WHERE appointment_id = ?",
                            invoiceId, appt.appointmentId);
                    }
                }
            }
            
            // Step 3: Find all unbilled, FILLED PRESCRIPTIONS (interdependent query)
            String prescSql = "SELECT * FROM prescriptions WHERE patient_id = ? AND invoice_id IS NULL AND is_filled = true";
            try (PreparedStatement pstmt = conn.prepareStatement(prescSql)) {
                pstmt.setInt(1, patientId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        Prescription presc = new Prescription(
                            rs.getInt("prescription_id"), patientId, rs.getInt("doctor_id"),
                            rs.getInt("medication_id"), rs.getInt("quantity"), rs.getString("dosage"),
                            rs.getTimestamp("date_prescribed").toLocalDateTime(), true, null
                        );
                        
                        // Get medication cost (interdependent call)
                        Medication med = pharmacyService.getMedicationById(presc.medicationId);
                        BigDecimal lineCost = med.unitCost.multiply(new BigDecimal(presc.quantity));
                        
                        // Add line item
                        addLineItem(invoiceId, "Medication: " + med.name, lineCost, "MEDICATION", presc.prescriptionId);
                        totalAmount = totalAmount.add(lineCost);
                        
                        // Link prescription to invoice (interdependent update)
                        DatabaseManager.executeUpdate(conn, "UPDATE prescriptions SET invoice_id = ? WHERE prescription_id = ?",
                            invoiceId, presc.prescriptionId);
                    }
                }
            }

            // Step 4: Update the invoice with the final total
            String totalSql = "UPDATE invoices SET total_amount = ? WHERE invoice_id = ?";
            DatabaseManager.executeUpdate(conn, totalSql, totalAmount, invoiceId);

            conn.commit();
            
            // Return the full invoice object
            return getInvoiceDetails(invoiceId);

        } catch (Exception e) {
            try {
                conn.rollback();
            } catch (SQLException ex) {
                LOGGER.log(Level.SEVERE, "Failed to rollback invoice transaction.", ex);
            }
            throw new BillingException("Failed to generate final invoice: " + e.getMessage(), e);
        } finally {
            try {
                conn.setAutoCommit(true);
            } catch (SQLException e) {
                LOGGER.log(Level.WARNING, "Failed to reset auto-commit.", e);
            }
        }
    }

    /**
     * Helper to add a line item.
     */
    private void addLineItem(int invoiceId, String desc, BigDecimal amt, String type, int refId) throws DatabaseException {
        String sql = "INSERT INTO invoice_line_items (invoice_id, description, amount, item_type, reference_id) VALUES (?, ?, ?, ?, ?)";
        DatabaseManager.executeInsertGetId(conn, sql, invoiceId, desc, amt, type, refId);
    }
    
    /**
     * Marks an invoice as paid.
     */
    public void markInvoiceAsPaid(int invoiceId, String paymentReference) throws BillingException, DatabaseException {
        String sql = "UPDATE invoices SET status = 'PAID', date_paid = ?, payment_reference = ? WHERE invoice_id = ? AND status = 'UNPAID'";
        int rows = DatabaseManager.executeUpdate(conn, sql, LocalDateTime.now(), paymentReference, invoiceId);
        if (rows == 0) {
            throw new BillingException("Invoice not found, already paid, or void.");
        }
    }

    /**
     * Gets a list of outstanding (UNPAID) invoices for a patient.
     */
    public List<Invoice> getOutstandingInvoicesForPatient(int patientId) throws DatabaseException {
        List<Invoice> invoices = new ArrayList<>();
        String sql = "SELECT * FROM invoices WHERE patient_id = ? AND status = 'UNPAID'";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, patientId);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    invoices.add(new Invoice(
                        rs.getInt("invoice_id"), patientId, rs.getString("invoice_uuid"),
                        rs.getString("status"), rs.getBigDecimal("total_amount"),
                        rs.getTimestamp("date_issued").toLocalDateTime()
                    ));
                }
            }
        } catch (SQLException e) {
            throw new DatabaseException("Failed to get outstanding invoices.", e);
        }
        return invoices;
    }
    
    /**
     * Gets a single invoice and all its line items.
     * This is a complex interdependent read query.
     */
    public Invoice getInvoiceDetails(int invoiceId) throws BillingException, DatabaseException {
        String invSql = "SELECT * FROM invoices WHERE invoice_id = ?";
        String lineSql = "SELECT * FROM invoice_line_items WHERE invoice_id = ? ORDER BY item_id";
        
        Invoice invoice;
        try (PreparedStatement pstmt = conn.prepareStatement(invSql)) {
            pstmt.setInt(1, invoiceId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    invoice = new Invoice(
                        rs.getInt("invoice_id"), rs.getInt("patient_id"), rs.getString("invoice_uuid"),
                        rs.getString("status"), rs.getBigDecimal("total_amount"),
                        rs.getTimestamp("date_issued").toLocalDateTime()
                    );
                } else {
                    throw new BillingException("Invoice not found: " + invoiceId);
                }
            }
        } catch (SQLException e) {
            throw new DatabaseException("Failed to get invoice.", e);
        }
        
        // Now get line items (interdependent)
        try (PreparedStatement pstmt = conn.prepareStatement(lineSql)) {
            pstmt.setInt(1, invoiceId);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    invoice.lineItems.add(new InvoiceLineItem(
                        rs.getInt("item_id"), invoiceId, rs.getString("description"),
                        rs.getBigDecimal("amount"), rs.getString("item_type"),
                        rs.getInt("reference_id")
                    ));
                }
            }
        } catch (SQLException e) {
            throw new DatabaseException("Failed to get invoice line items.", e);
        }
        
        return invoice;
    }
}

/**
 * ReportingService
 * Generates complex, read-only reports by joining data from
 * multiple services/tables.
 */
class ReportingService {
    private Connection conn;
    private static final Logger LOGGER = Logger.getLogger(ReportingService.class.getName());

    public ReportingService(Connection conn) {
        this.conn = conn;
    }

    /**
     * Report 1: Bed Occupancy
     * Interdependent SQL: Queries patients table.
     */
    public BedOccupancyReport getBedOccupancyReport(int totalBeds) throws DatabaseException {
        String sql = "SELECT COUNT(*) FROM patients WHERE status = 'ADMITTED'";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    int admitted = rs.getInt(1);
                    return new BedOccupancyReport(admitted, totalBeds);
                }
                return new BedOccupancyReport(0, totalBeds);
            }
        } catch (SQLException e) {
            throw new DatabaseException("Failed to generate occupancy report.", e);
        }
    }

    /**
     * Report 2: Billing Summary
     * Interdependent SQL: Aggregates from invoices table.
     */
    public BillingSummaryReport getBillingSummaryByDateRange(LocalDate start, LocalDate end) throws DatabaseException {
        String sql = """
        SELECT 
            SUM(total_amount) as total_billed,
            SUM(CASE WHEN status = 'PAID' THEN total_amount ELSE 0 END) as total_paid
        FROM invoices
        WHERE date_issued >= ? AND date_issued <= ?
          AND status != 'VOID'
        """;
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setTimestamp(1, Timestamp.valueOf(start.atStartOfDay()));
            pstmt.setTimestamp(2, Timestamp.valueOf(end.plusDays(1).atStartOfDay().minusNanos(1)));
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return new BillingSummaryReport(rs.getBigDecimal("total_billed"), rs.getBigDecimal("total_paid"));
                }
                return new BillingSummaryReport(BigDecimal.ZERO, BigDecimal.ZERO);
            }
        } catch (SQLException e) {
            throw new DatabaseException("Failed to generate billing report.", e);
        }
    }
    
    /**
     * Report 3: Doctor Workload
     * Highly Interdependent SQL: Joins doctors and appointments.
     */
    public List<DoctorWorkload> getDoctorWorkloadReport(LocalDate start, LocalDate end) throws DatabaseException {
        List<DoctorWorkload> report = new ArrayList<>();
        String sql = """
        SELECT
            d.doctor_id,
            d.first_name || ' ' || d.last_name as doctor_name,
            d.specialty,
            COUNT(a.appointment_id) as appointment_count,
            SUM(a.consultation_fee) as total_revenue
        FROM doctors d
        LEFT JOIN appointments a 
            ON d.doctor_id = a.doctor_id
            AND a.status IN ('SCHEDULED', 'COMPLETED') -- Or just COMPLETED
            AND a.appointment_time BETWEEN ? AND ?
        GROUP BY d.doctor_id, doctor_name, d.specialty
        ORDER BY appointment_count DESC, total_revenue DESC
        """;
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setTimestamp(1, Timestamp.valueOf(start.atStartOfDay()));
            pstmt.setTimestamp(2, Timestamp.valueOf(end.plusDays(1).atStartOfDay().minusNanos(1)));
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    report.add(new DoctorWorkload(
                        rs.getInt("doctor_id"),
                        rs.getString("doctor_name"),
                        rs.getString("specialty"),
                        rs.getInt("appointment_count"),
                        rs.getBigDecimal("total_revenue")
                    ));
                }
            }
        } catch (SQLException e) {
            throw new DatabaseException("Failed to generate doctor workload report.", e);
        }
        return report;
    }

    /**
     * Report 4: Low Stock Medication
     * Interdependent SQL: Queries medications table.
     */
    public List<MedicationStock> getLowStockMedicationReport(int threshold) throws DatabaseException {
        List<MedicationStock> report = new ArrayList<>();
        String sql = "SELECT medication_id, name, stock_quantity FROM medications WHERE stock_quantity <= ? ORDER BY stock_quantity ASC";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, threshold);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    report.add(new MedicationStock(
                        rs.getInt("medication_id"),
                        rs.getString("name"),
                        rs.getInt("stock_quantity")
                    ));
                }
            }
        } catch (SQLException e) {
            throw new DatabaseException("Failed to generate low stock report.", e);
        }
        return report;
    }
}