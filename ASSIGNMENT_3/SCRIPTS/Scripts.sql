
----Reg number:224020114
----CASE STUDY:MUNICIPAL WATER BILLING AND CONSUMPTION TRACKING SYSTEM

-- TASK 1: Distributed Schema Design and Fragmentation
-- This script demonstrates horizontal fragmentation by splitting the Customer
-- and related tables across two logical nodes based on geographic regions.

-- NODE A: BranchDB_A

-- Create schema for Branch A
CREATE SCHEMA IF NOT EXISTS branch_a;
-- Customers in Northern Region (CustomerID 1-5)
CREATE TABLE branch_a.Customer (
    CustomerID SERIAL PRIMARY KEY,
    FullName VARCHAR(100) NOT NULL,
    Address VARCHAR(255) NOT NULL,
    Type VARCHAR(20) CHECK (Type IN ('Residential', 'Commercial')),
    Contact VARCHAR(15) UNIQUE NOT NULL,
    Region VARCHAR(20) DEFAULT 'North'
);

CREATE TABLE branch_a.Meter (
    MeterID SERIAL PRIMARY KEY,
    CustomerID INT NOT NULL,
    InstallationDate DATE NOT NULL,
    Status VARCHAR(20) DEFAULT 'Active' CHECK (Status IN ('Active', 'Inactive', 'Faulty')),
    LastReading DECIMAL(10,2) DEFAULT 0.00 CHECK (LastReading >= 0),
    FOREIGN KEY (CustomerID) REFERENCES branch_a.Customer(CustomerID) ON DELETE CASCADE
);

CREATE TABLE branch_a.Reading (
    ReadingID SERIAL PRIMARY KEY,
    MeterID INT NOT NULL,
    ReadingDate DATE NOT NULL,
    CurrentReading DECIMAL(10,2) NOT NULL CHECK (CurrentReading >= 0),
    Consumption DECIMAL(10,2),
    FOREIGN KEY (MeterID) REFERENCES branch_a.Meter(MeterID) ON DELETE CASCADE
);

CREATE TABLE branch_a.Bill (
    BillID SERIAL PRIMARY KEY,
    ReadingID INT UNIQUE NOT NULL,
    AmountDue DECIMAL(10,2) NOT NULL CHECK (AmountDue >= 0),
    DueDate DATE NOT NULL,
    Status VARCHAR(20) DEFAULT 'Unpaid' CHECK (Status IN ('Paid', 'Unpaid', 'Overdue')),
    FOREIGN KEY (ReadingID) REFERENCES branch_a.Reading(ReadingID) ON DELETE CASCADE
);

CREATE TABLE branch_a.Payment (
    PaymentID SERIAL PRIMARY KEY,
    BillID INT UNIQUE NOT NULL,
    Amount DECIMAL(10,2) NOT NULL CHECK (Amount > 0),
    PaymentDate DATE NOT NULL,
    Method VARCHAR(20) CHECK (Method IN ('Cash', 'Card', 'Bank Transfer', 'Mobile Money')),
    FOREIGN KEY (BillID) REFERENCES branch_a.Bill(BillID) ON DELETE CASCADE
);

-- NODE B: BranchDB_B

-- Create schema for Branch B
CREATE SCHEMA IF NOT EXISTS branch_b;

-- Customers in one Region (CustomerID 6-10)
CREATE TABLE branch_b.Customer (
    CustomerID SERIAL PRIMARY KEY,
    FullName VARCHAR(100) NOT NULL,
    Address VARCHAR(255) NOT NULL,
    Type VARCHAR(20) CHECK (Type IN ('Residential', 'Commercial')),
    Contact VARCHAR(15) UNIQUE NOT NULL,
    Region VARCHAR(20) DEFAULT 'South'
);

CREATE TABLE branch_b.Meter (
    MeterID SERIAL PRIMARY KEY,
    CustomerID INT NOT NULL,
    InstallationDate DATE NOT NULL,
    Status VARCHAR(20) DEFAULT 'Active' CHECK (Status IN ('Active', 'Inactive', 'Faulty')),
    LastReading DECIMAL(10,2) DEFAULT 0.00 CHECK (LastReading >= 0),
    FOREIGN KEY (CustomerID) REFERENCES branch_b.Customer(CustomerID) ON DELETE CASCADE
);

CREATE TABLE branch_b.Reading (
    ReadingID SERIAL PRIMARY KEY,
    MeterID INT NOT NULL,
    ReadingDate DATE NOT NULL,
    CurrentReading DECIMAL(10,2) NOT NULL CHECK (CurrentReading >= 0),
    Consumption DECIMAL(10,2),
    FOREIGN KEY (MeterID) REFERENCES branch_b.Meter(MeterID) ON DELETE CASCADE
);

CREATE TABLE branch_b.Bill (
    BillID SERIAL PRIMARY KEY,
    ReadingID INT UNIQUE NOT NULL,
    AmountDue DECIMAL(10,2) NOT NULL CHECK (AmountDue >= 0),
    DueDate DATE NOT NULL,
    Status VARCHAR(20) DEFAULT 'Unpaid' CHECK (Status IN ('Paid', 'Unpaid', 'Overdue')),
    FOREIGN KEY (ReadingID) REFERENCES branch_b.Reading(ReadingID) ON DELETE CASCADE
);

CREATE TABLE branch_b.Payment (
    PaymentID SERIAL PRIMARY KEY,
    BillID INT UNIQUE NOT NULL,
    Amount DECIMAL(10,2) NOT NULL CHECK (Amount > 0),
    PaymentDate DATE NOT NULL,
    Method VARCHAR(20) CHECK (Method IN ('Cash', 'Card', 'Bank Transfer', 'Mobile Money')),
    FOREIGN KEY (BillID) REFERENCES branch_b.Bill(BillID) ON DELETE CASCADE
);
-- Create Unified Views for Transparent Access
-- Unified Customer View
CREATE OR REPLACE VIEW public.v_all_customers AS
SELECT CustomerID, FullName, Address, Type, Contact, Region, 'Branch_A' as Source
FROM branch_a.Customer
UNION ALL
SELECT CustomerID, FullName, Address, Type, Contact, Region, 'Branch_B' as Source
FROM branch_b.Customer;

-- Unified Bill View
CREATE OR REPLACE VIEW public.v_all_bills AS
SELECT BillID, ReadingID, AmountDue, DueDate, Status, 'Branch_A' as Source
FROM branch_a.Bill
UNION ALL
SELECT BillID, ReadingID, AmountDue, DueDate, Status, 'Branch_B' as Source
FROM branch_b.Bill;

-- Insert Sample Data into Both Nodes

-- Branch A Data
INSERT INTO branch_a.Customer (FullName, Address, Type, Contact, Region) VALUES
('John Smith', '123 North Ave', 'Residential', '555-0101', 'North'),
('Sarah Johnson', '456 North St', 'Commercial', '555-0102', 'North'),
('Mike Wilson', '789 North Blvd', 'Residential', '555-0103', 'North'),
('Emily Brown', '321 North Rd', 'Residential', '555-0104', 'North'),
('Tech Corp Inc', '654 North Plaza', 'Commercial', '555-0105', 'North');

-- Branch B Data
INSERT INTO branch_b.Customer (FullName, Address, Type, Contact, Region) VALUES
('David Lee', '123 South Ave', 'Residential', '555-0201', 'South'),
('Lisa Garcia', '456 South St', 'Commercial', '555-0202', 'South'),
('Tom Martinez', '789 South Blvd', 'Residential', '555-0203', 'South'),
('Anna Rodriguez', '321 South Rd', 'Residential', '555-0204', 'South'),
('Global Services', '654 South Plaza', 'Commercial', '555-0205', 'South');

---BRANCH A - METER INSERTIONS (5 Records)
INSERT INTO branch_a.Meter (CustomerID, InstallationDate, Status, LastReading) VALUES
(1, '2020-01-15', 'Active', 1250.50),
(2, '2019-06-20', 'Active', 2340.75),
(3, '2021-03-10', 'Active', 890.25),
(4, '2018-11-05', 'Active', 3450.00),
(5, '2022-02-28', 'Active', 567.80);

-- Verify Branch A insertions
SELECT 'Branch A Meters Inserted' AS status, COUNT(*) AS total_meters 
FROM branch_a.Meter;

---BRANCH B - METER INSERTIONS (5 Records)
INSERT INTO branch_b.Meter (CustomerID, InstallationDate, Status, LastReading) VALUES
(1, '2019-05-10', 'Active', 1567.25),
(2, '2020-09-15', 'Active', 2890.50),
(3, '2021-01-22', 'Active', 1123.75),
(4, '2018-07-30', 'Active', 4200.00),
(5, '2022-04-18', 'Active', 678.90);

-- Verify Branch B insertions
SELECT 'Branch B Meters Inserted' AS status, COUNT(*) AS total_meters 
FROM branch_b.Meter;

-- Fragmentation Analysis

COMMENT ON SCHEMA branch_a IS 'Northern Region - Horizontal Fragmentation Node A';
COMMENT ON SCHEMA branch_b IS 'Southern Region - Horizontal Fragmentation Node B';

-- Query to verify fragmentation
SELECT 
    'Branch A' as Branch,
    COUNT(*) as CustomerCount,
    Region
FROM branch_a.Customer
GROUP BY Region
UNION ALL
SELECT 
    'Branch B' as Branch,
    COUNT(*) as CustomerCount,
    Region
FROM branch_b.Customer
GROUP BY Region;

-- TASK 2: Create and Use Database Links (Foreign Data Wrappers in PostgreSQL)
-- ============================================================================
-- PostgreSQL uses Foreign Data Wrappers (FDW) instead of Oracle's DB Links
-- This demonstrates cross-schema queries simulating distributed databases
-- Setup: Install postgres_fdw extension (if connecting to remote PostgreSQL)
-- Create Foreign Server (Simulated for Branch B)
-- DEMONSTRATION 1: Remote SELECT from Branch B
-- Query customers from Branch B (simulating remote access)
SELECT 
    'Remote Query from Branch B' as QueryType,
    CustomerID,
    FullName,
    Address,
    Type,
    Region
FROM branch_b.Customer
ORDER BY CustomerID;

-- Distributed JOIN between Branch A and Branch B
-- Join customers from both branches with their meter counts
SELECT 
    'Distributed Join Result' as QueryType,
    c.CustomerID,
    c.FullName,
    c.Region,
    c.Type,
    COUNT(m.MeterID) as MeterCount,
    CASE 
        WHEN c.Region = 'North' THEN 'Branch_A (Local)'
        ELSE 'Branch_B (Remote)'
    END as DataSource
FROM (
    SELECT CustomerID, FullName, Region, Type FROM branch_a.Customer
    UNION ALL
    SELECT CustomerID, FullName, Region, Type FROM branch_b.Customer
) c
LEFT JOIN (
    SELECT CustomerID, MeterID FROM branch_a.Meter
    UNION ALL
    SELECT CustomerID, MeterID FROM branch_b.Meter
) m ON c.CustomerID = m.CustomerID
GROUP BY c.CustomerID, c.FullName, c.Region, c.Type
ORDER BY c.Region, c.CustomerID;

-- : Cross-Branch Revenue Analysis
-- Aggregate billing data across both branches
WITH branch_a_revenue AS (
    SELECT 
        'Branch_A' as Branch,
        COUNT(DISTINCT c.CustomerID) as TotalCustomers,
        COUNT(b.BillID) as TotalBills,
        COALESCE(SUM(b.AmountDue), 0) as TotalRevenue,
        COALESCE(SUM(CASE WHEN b.Status = 'Paid' THEN b.AmountDue ELSE 0 END), 0) as PaidRevenue
    FROM branch_a.Customer c
    LEFT JOIN branch_a.Meter m ON c.CustomerID = m.CustomerID
    LEFT JOIN branch_a.Reading r ON m.MeterID = r.MeterID
    LEFT JOIN branch_a.Bill b ON r.ReadingID = b.ReadingID
),
branch_b_revenue AS (
    SELECT 
        'Branch_B' as Branch,
        COUNT(DISTINCT c.CustomerID) as TotalCustomers,
        COUNT(b.BillID) as TotalBills,
        COALESCE(SUM(b.AmountDue), 0) as TotalRevenue,
        COALESCE(SUM(CASE WHEN b.Status = 'Paid' THEN b.AmountDue ELSE 0 END), 0) as PaidRevenue
    FROM branch_b.Customer c
    LEFT JOIN branch_b.Meter m ON c.CustomerID = m.CustomerID
    LEFT JOIN branch_b.Reading r ON m.MeterID = r.MeterID
    LEFT JOIN branch_b.Bill b ON r.ReadingID = b.ReadingID
)
SELECT * FROM branch_a_revenue
UNION ALL
SELECT * FROM branch_b_revenue
UNION ALL
SELECT 
    'TOTAL (Distributed)' as Branch,
    SUM(TotalCustomers) as TotalCustomers,
    SUM(TotalBills) as TotalBills,
    SUM(TotalRevenue) as TotalRevenue,
    SUM(PaidRevenue) as PaidRevenue
FROM (
    SELECT * FROM branch_a_revenue
    UNION ALL
    SELECT * FROM branch_b_revenue
) combined;

-- Distributed Customer Search
-- Function to search customers across all branches
CREATE OR REPLACE FUNCTION search_customer_distributed(search_term VARCHAR)
RETURNS TABLE (
    CustomerID INT,
    FullName VARCHAR,
    Address VARCHAR,
    Type VARCHAR,
    Contact VARCHAR,
    Region VARCHAR,
    Branch VARCHAR
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.CustomerID,
        c.FullName,
        c.Address,
        c.Type,
        c.Contact,
        c.Region,
        'Branch_A'::VARCHAR as Branch
    FROM branch_a.Customer c
    WHERE c.FullName ILIKE '%' || search_term || '%'
       OR c.Address ILIKE '%' || search_term || '%'
    UNION ALL
    SELECT 
        c.CustomerID,
        c.FullName,
        c.Address,
        c.Type,
        c.Contact,
        c.Region,
        'Branch_B'::VARCHAR as Branch
    FROM branch_b.Customer c
    WHERE c.FullName ILIKE '%' || search_term || '%'
       OR c.Address ILIKE '%' || search_term || '%';
END;
$$ LANGUAGE plpgsql;

-- Test the distributed search
SELECT * FROM search_customer_distributed('Smith');
SELECT * FROM search_customer_distributed('South');

-- Local vs Distributed Query
-- Enable timing
\timing on

-- Local query (single branch)
EXPLAIN ANALYZE
SELECT c.FullName, COUNT(m.MeterID) as MeterCount
FROM branch_a.Customer c
LEFT JOIN branch_a.Meter m ON c.CustomerID = m.CustomerID
GROUP BY c.FullName;

-- Distributed query (both branches)
EXPLAIN ANALYZE
SELECT c.FullName, COUNT(m.MeterID) as MeterCount
FROM (
    SELECT CustomerID, FullName FROM branch_a.Customer
    UNION ALL
    SELECT CustomerID, FullName FROM branch_b.Customer
) c
LEFT JOIN (
    SELECT CustomerID, MeterID FROM branch_a.Meter
    UNION ALL
    SELECT CustomerID, MeterID FROM branch_b.Meter
) m ON c.CustomerID = m.CustomerID
GROUP BY c.FullName;

-- TASK 3: Parallel Query Execution
-- ===================================================================
-- PostgreSQL supports parallel query execution for large table scans
-- This demonstrates parallel execution and performance comparison
-- Setup: Create Large Transaction Table for Testing

CREATE TABLE IF NOT EXISTS public.large_transactions (
    TransactionID SERIAL PRIMARY KEY,
    CustomerID INT NOT NULL,
    TransactionDate DATE NOT NULL,
    Amount DECIMAL(10,2) NOT NULL,
    Type VARCHAR(20),
    Region VARCHAR(20)
);

-- Generate large dataset (100,000 records)
INSERT INTO public.large_transactions (CustomerID, TransactionDate, Amount, Type, Region)
SELECT 
    (random() * 10 + 1)::INT as CustomerID,
    CURRENT_DATE - (random() * 365)::INT as TransactionDate,
    (random() * 1000 + 10)::DECIMAL(10,2) as Amount,
    CASE WHEN random() < 0.5 THEN 'Payment' ELSE 'Bill' END as Type,
    CASE WHEN random() < 0.5 THEN 'North' ELSE 'South' END as Region
FROM generate_series(1, 100000);

-- Create indexes for performance testing
CREATE INDEX idx_transactions_customer ON public.large_transactions(CustomerID);
CREATE INDEX idx_transactions_date ON public.large_transactions(TransactionDate);
CREATE INDEX idx_transactions_region ON public.large_transactions(Region);

-- Analyze table for optimizer
ANALYZE public.large_transactions;
-- Configure Parallel Query Settings
-- Show current parallel settings
SHOW max_parallel_workers_per_gather;
SHOW max_parallel_workers;
SHOW parallel_setup_cost;
SHOW parallel_tuple_cost;

-- Set parallel workers for this session (adjust based on CPU cores)
SET max_parallel_workers_per_gather = 4;
SET parallel_setup_cost = 100;
SET parallel_tuple_cost = 0.01;

-- Force parallel execution
-- For PostgreSQL 13+:
SET debug_parallel_query = on;

-----Serial vs Parallel Execution Comparison

-- Disable parallel execution for baseline
SET max_parallel_workers_per_gather = 0;

-- Serial execution
EXPLAIN (ANALYZE, BUFFERS, TIMING)
SELECT 
    Region,
    Type,
    COUNT(*) as TransactionCount,
    SUM(Amount) as TotalAmount,
    AVG(Amount) as AvgAmount,
    MIN(Amount) as MinAmount,
    MAX(Amount) as MaxAmount
FROM public.large_transactions
WHERE TransactionDate >= CURRENT_DATE - INTERVAL '180 days'
GROUP BY Region, Type
ORDER BY Region, Type;

-- Enable parallel execution
SET max_parallel_workers_per_gather = 4;

-- Parallel execution
EXPLAIN (ANALYZE, BUFFERS, TIMING)
SELECT 
    Region,
    Type,
    COUNT(*) as TransactionCount,
    SUM(Amount) as TotalAmount,
    AVG(Amount) as AvgAmount,
    MIN(Amount) as MinAmount,
    MAX(Amount) as MaxAmount
FROM public.large_transactions
WHERE TransactionDate >= CURRENT_DATE - INTERVAL '180 days'
GROUP BY Region, Type
ORDER BY Region, Type;
-- Large table scan with aggregation
EXPLAIN (ANALYZE, BUFFERS, COSTS, TIMING)
SELECT 
    CustomerID,
    COUNT(*) as TotalTransactions,
    SUM(Amount) as TotalSpent,
    AVG(Amount) as AvgTransaction
FROM public.large_transactions
GROUP BY CustomerID
HAVING COUNT(*) > 100
ORDER BY TotalSpent DESC
LIMIT 20;

-- Parallel Join Execution
-- Create summary table for join
CREATE TABLE IF NOT EXISTS public.customer_summary (
    CustomerID INT PRIMARY KEY,
    CustomerName VARCHAR(100),
    Region VARCHAR(20)
);

INSERT INTO public.customer_summary (CustomerID, CustomerName, Region)
SELECT 
    CustomerID,
    'Customer_' || CustomerID as CustomerName,
    CASE WHEN CustomerID <= 5 THEN 'North' ELSE 'South' END as Region
FROM generate_series(1, 10) CustomerID;

-- Parallel hash join
EXPLAIN (ANALYZE, BUFFERS, TIMING)
SELECT 
    cs.CustomerName,
    cs.Region,
    COUNT(lt.TransactionID) as TransactionCount,
    SUM(lt.Amount) as TotalAmount
FROM public.customer_summary cs
INNER JOIN public.large_transactions lt ON cs.CustomerID = lt.CustomerID
WHERE lt.TransactionDate >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY cs.CustomerName, cs.Region
ORDER BY TotalAmount DESC;

-- Parallel Aggregate with Window Functions

EXPLAIN (ANALYZE, BUFFERS, TIMING)
SELECT 
    TransactionDate,
    Region,
    SUM(Amount) as DailyTotal,
    AVG(SUM(Amount)) OVER (
        PARTITION BY Region 
        ORDER BY TransactionDate 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as MovingAvg7Days
FROM public.large_transactions
WHERE TransactionDate >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY TransactionDate, Region
ORDER BY Region, TransactionDate;

----Parallel Index Scan
-- Parallel bitmap heap scan
EXPLAIN (ANALYZE, BUFFERS, TIMING)
SELECT 
    CustomerID,
    TransactionDate,
    Amount,
    Type
FROM public.large_transactions
WHERE Region = 'North'
  AND TransactionDate >= CURRENT_DATE - INTERVAL '30 days'
  AND Amount > 500
ORDER BY Amount DESC;

-- Performance Benchmark Function

CREATE OR REPLACE FUNCTION benchmark_parallel_execution()
RETURNS TABLE (
    ExecutionMode VARCHAR,
    ExecutionTime INTERVAL,
    RowsProcessed BIGINT
) AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count BIGINT;
BEGIN
    -- Serial execution
    SET max_parallel_workers_per_gather = 0;
    start_time := clock_timestamp();
    
    SELECT COUNT(*) INTO row_count
    FROM public.large_transactions
    WHERE Amount > 100;
    
    end_time := clock_timestamp();
    
    ExecutionMode := 'Serial';
    ExecutionTime := end_time - start_time;
    RowsProcessed := row_count;
    RETURN NEXT;
    
    -- Parallel execution with 2 workers
    SET max_parallel_workers_per_gather = 2;
    start_time := clock_timestamp();
    
    SELECT COUNT(*) INTO row_count
    FROM public.large_transactions
    WHERE Amount > 100;
    
    end_time := clock_timestamp();
    
    ExecutionMode := 'Parallel (2 workers)';
    ExecutionTime := end_time - start_time;
    RowsProcessed := row_count;
    RETURN NEXT;
    
    -- Parallel execution with 4 workers
    SET max_parallel_workers_per_gather = 4;
    start_time := clock_timestamp();
    
    SELECT COUNT(*) INTO row_count
    FROM public.large_transactions
    WHERE Amount > 100;
    
    end_time := clock_timestamp();
    
    ExecutionMode := 'Parallel (4 workers)';
    ExecutionTime := end_time - start_time;
    RowsProcessed := row_count;
    RETURN NEXT;
    
    -- Reset to default
    SET max_parallel_workers_per_gather = 4;
END;
$$ LANGUAGE plpgsql;

-- Run benchmark
SELECT * FROM benchmark_parallel_execution();

/*
PARALLEL QUERY EXECUTION RESULTS:

*/
-- Reset settings
RESET max_parallel_workers_per_gather;
-- Updated reset command for PostgreSQL 13+
RESET debug_parallel_query;

-- TASK 4: Two-Phase Commit Simulation
-- ============================================================================
-- PostgreSQL supports two-phase commit (2PC) for distributed transactions
-- Start a distributed transaction
BEGIN;

-- Insert customer in Branch A
INSERT INTO branch_a.Customer (FullName, Address, Type, Contact, Region)
VALUES ('Alice Cooper', '999 North Lane', 'Residential', '555-9001', 'North');

-- Insert customer in Branch B
INSERT INTO branch_b.Customer (FullName, Address, Type, Contact, Region)
VALUES ('Bob Dylan', '888 South Lane', 'Commercial', '555-9002', 'South');

-- Prepare the transaction (Phase 1: Prepare)
PREPARE TRANSACTION 'distributed_insert_001';

-- Check prepared transactions
SELECT 
    gid,
    prepared,
    owner,
    database
FROM pg_prepared_xacts
WHERE gid = 'distributed_insert_001';

-- Commit the prepared transaction (Phase 2: Commit)
COMMIT PREPARED 'distributed_insert_001';

-- Verify the inserts
SELECT 'Branch A' as Branch, FullName, Contact FROM branch_a.Customer WHERE Contact = '555-9001'
UNION ALL
SELECT 'Branch B' as Branch, FullName, Contact FROM branch_b.Customer WHERE Contact = '555-9002';

--Two-Phase Commit with Rollback
-- Start another distributed transaction
BEGIN;

-- Insert meter in Branch A
INSERT INTO branch_a.Meter (CustomerID, InstallationDate, Status)
VALUES (1, CURRENT_DATE, 'Active');

-- Insert meter in Branch B
INSERT INTO branch_b.Meter (CustomerID, InstallationDate, Status)
VALUES (6, CURRENT_DATE, 'Active');

-- Prepare the transaction
PREPARE TRANSACTION 'distributed_insert_002';

-- Check prepared transactions
SELECT gid, prepared FROM pg_prepared_xacts WHERE gid = 'distributed_insert_002';

-- Simulate failure - rollback instead of commit
ROLLBACK PREPARED 'distributed_insert_002';

-- Verify rollback - no new meters should exist
SELECT 'Branch A' as Branch, COUNT(*) as MeterCount FROM branch_a.Meter
UNION ALL
SELECT 'Branch B' as Branch, COUNT(*) as MeterCount FROM branch_b.Meter;

-- Complex Multi-Node Transaction

-- Function to perform distributed billing transaction
CREATE OR REPLACE FUNCTION distributed_billing_transaction(
    p_branch_a_meter_id INT,
    p_branch_b_meter_id INT,
    p_transaction_id VARCHAR
) RETURNS TEXT AS $$
DECLARE
    v_reading_id_a INT;
    v_reading_id_b INT;
    v_result TEXT;
BEGIN
    -- Start transaction
    BEGIN
        -- Insert reading for Branch A
        INSERT INTO branch_a.Reading (MeterID, ReadingDate, CurrentReading, Consumption)
        VALUES (p_branch_a_meter_id, CURRENT_DATE, 150.50, 25.00)
        RETURNING ReadingID INTO v_reading_id_a;
        
        -- Insert bill for Branch A
        INSERT INTO branch_a.Bill (ReadingID, AmountDue, DueDate, Status)
        VALUES (v_reading_id_a, 62.50, CURRENT_DATE + INTERVAL '30 days', 'Unpaid');
        
        -- Inserting reading for Branch B
        INSERT INTO branch_b.Reading (MeterID, ReadingDate, CurrentReading, Consumption)
        VALUES (p_branch_b_meter_id, CURRENT_DATE, 200.75, 35.00)
        RETURNING ReadingID INTO v_reading_id_b;
        
        -- Insert bill for Branch B
        INSERT INTO branch_b.Bill (ReadingID, AmountDue, DueDate, Status)
        VALUES (v_reading_id_b, 131.25, CURRENT_DATE + INTERVAL '30 days', 'Unpaid');
        
        -- Prepare transaction
        EXECUTE format('PREPARE TRANSACTION %L', p_transaction_id);
        
        v_result := 'Transaction prepared: ' || p_transaction_id;
        
    EXCEPTION WHEN OTHERS THEN
        v_result := 'Transaction failed: ' || SQLERRM;
    END;
    
    RETURN v_result;
END;
$$ LANGUAGE plpgsql;

-- Two-phase commit for billing
BEGIN;

-- Branch A: Create reading and bill
INSERT INTO branch_a.Reading (MeterID, ReadingDate, CurrentReading, Consumption)
VALUES (1, CURRENT_DATE, 150.50, 25.00);

INSERT INTO branch_a.Bill (ReadingID, AmountDue, DueDate, Status)
VALUES (currval('branch_a.reading_readingid_seq'), 62.50, CURRENT_DATE + INTERVAL '30 days', 'Unpaid');

-- Branch B: Create reading and bill
INSERT INTO branch_b.Reading (MeterID, ReadingDate, CurrentReading, Consumption)
VALUES (1, CURRENT_DATE, 200.75, 35.00);

INSERT INTO branch_b.Bill (ReadingID, AmountDue, DueDate, Status)
VALUES (currval('branch_b.reading_readingid_seq'), 131.25, CURRENT_DATE + INTERVAL '30 days', 'Unpaid');

-- Prepare the transaction
PREPARE TRANSACTION 'billing_cycle_001';

-- Verify atomicity - check prepared transactions
SELECT 
    gid,
    prepared,
    owner,
    database,
    CURRENT_TIMESTAMP - prepared as time_in_prepared_state
FROM pg_prepared_xacts
WHERE gid = 'billing_cycle_001';

-- Commit the prepared transaction
COMMIT PREPARED 'billing_cycle_001';

-- Verify both bills were created atomically
SELECT 'Branch A Bills' as Source, COUNT(*) as BillCount FROM branch_a.Bill
UNION ALL
SELECT 'Branch B Bills' as Source, COUNT(*) as BillCount FROM branch_b.Bill;

----Monitoring Prepared Transactions
-- Create monitoring view for prepared transactions
CREATE OR REPLACE VIEW v_prepared_transactions AS
SELECT 
    gid as TransactionID,
    prepared as PreparedTime,
    owner as TransactionOwner,
    database as DatabaseName,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - prepared)) as SecondsInPreparedState,
    CASE 
        WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - prepared)) > 300 THEN 'WARNING: Long-running prepared transaction'
        ELSE 'OK'
    END as Status
FROM pg_prepared_xacts
ORDER BY prepared DESC;

-- Query prepared transactions
SELECT * FROM v_prepared_transactions;

---------------------------------------------------------------------
BEGIN;

INSERT INTO branch_a.Customer (FullName, Address, Type, Contact, Region)
VALUES ('Test User 1', '111 Test St', 'Residential', '555-TEST1', 'North');

INSERT INTO branch_b.Customer (FullName, Address, Type, Contact, Region)
VALUES ('Test User 2', '222 Test St', 'Commercial', '555-TEST2', 'South');

PREPARE TRANSACTION 'coordinator_failure_test';

-- At this point, if coordinator fails, transaction remains in prepared state
-- Check prepared transactions
SELECT * FROM v_prepared_transactions WHERE TransactionID = 'coordinator_failure_test';

-- : Rollback (cleanup)
ROLLBACK PREPARED 'coordinator_failure_test';

-- Atomicity Verification
-- Function to verify atomicity of distributed operations
CREATE OR REPLACE FUNCTION verify_distributed_atomicity()
RETURNS TABLE (
    TestCase VARCHAR,
    BranchA_Count BIGINT,
    BranchB_Count BIGINT,
    IsAtomic BOOLEAN,
    Result VARCHAR
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        'Customer Count Consistency'::VARCHAR as TestCase,
        (SELECT COUNT(*) FROM branch_a.Customer) as BranchA_Count,
        (SELECT COUNT(*) FROM branch_b.Customer) as BranchB_Count,
        (SELECT COUNT(*) FROM branch_a.Customer) = (SELECT COUNT(*) FROM branch_b.Customer) as IsAtomic,
        CASE 
            WHEN (SELECT COUNT(*) FROM branch_a.Customer) = (SELECT COUNT(*) FROM branch_b.Customer)
            THEN 'PASS: Both branches have equal customer counts'
            ELSE 'FAIL: Customer counts differ between branches'
        END as Result;
END;
$$ LANGUAGE plpgsql;

-- Run atomicity verification
SELECT * FROM verify_distributed_atomicity();

-- Cleanup any remaining prepared transactions
DO $$
DECLARE
    prep_xact RECORD;
BEGIN
    FOR prep_xact IN SELECT gid FROM pg_prepared_xacts LOOP
        EXECUTE 'ROLLBACK PREPARED ' || quote_literal(prep_xact.gid);
    END LOOP;
END $$;

-- TASK 5: Distributed Rollback and Recovery
-- Simulates network failures and demonstrates recovery mechanisms
-- ============================================================================
-- Setup: Create Recovery Log Table

CREATE TABLE IF NOT EXISTS public.transaction_recovery_log (
    LogID SERIAL PRIMARY KEY,
    TransactionID VARCHAR(100) UNIQUE NOT NULL,
    TransactionType VARCHAR(50),
    StartTime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    Status VARCHAR(20) DEFAULT 'IN_PROGRESS',
    BranchA_Status VARCHAR(20),
    BranchB_Status VARCHAR(20),
    ErrorMessage TEXT,
    RecoveryAction VARCHAR(50),
    RecoveryTime TIMESTAMP,
    CONSTRAINT chk_status CHECK (Status IN ('IN_PROGRESS', 'PREPARED', 'COMMITTED', 'ROLLED_BACK', 'FAILED', 'RECOVERED'))
);

-- Log transaction start
INSERT INTO public.transaction_recovery_log (TransactionID, TransactionType, Status)
VALUES ('TXN_NETWORK_FAIL_001', 'Customer_Insert', 'IN_PROGRESS');

-- Start distributed transaction
BEGIN;

-- Branch A operation succeeds
INSERT INTO branch_a.Customer (FullName, Address, Type, Contact, Region)
VALUES ('Network Test User A', '123 Failure St', 'Residential', '555-FAIL1', 'North');

-- Update log
UPDATE public.transaction_recovery_log
SET BranchA_Status = 'SUCCESS'
WHERE TransactionID = 'TXN_NETWORK_FAIL_001';

-- Simulate network failure before Branch B operation
-- In real scenario, connection to Branch B would be lost here

DO $$
BEGIN
    -- Branch B operation (simulated failure)
    BEGIN
        INSERT INTO branch_b.Customer (FullName, Address, Type, Contact, Region)
        VALUES ('Network Test User B', '456 Failure Ave', 'Commercial', '555-FAIL2', 'South');
        
        -- Simulate network timeout/failure
        RAISE EXCEPTION 'Network timeout: Connection to Branch B lost';
        
    EXCEPTION WHEN OTHERS THEN
        -- Log the failure
        UPDATE public.transaction_recovery_log
        SET 
            BranchB_Status = 'FAILED',
            Status = 'FAILED',
            ErrorMessage = SQLERRM
        WHERE TransactionID = 'TXN_NETWORK_FAIL_001';
        
        RAISE;
    END;
END $$;

-- Transaction will be rolled back due to exception
ROLLBACK;

-- Update recovery log
UPDATE public.transaction_recovery_log
SET 
    Status = 'ROLLED_BACK',
    RecoveryAction = 'AUTOMATIC_ROLLBACK',
    RecoveryTime = CURRENT_TIMESTAMP
WHERE TransactionID = 'TXN_NETWORK_FAIL_001';

-- Verify rollback - no data should exist
SELECT 
    'Rollback Verification' as Test,
    (SELECT COUNT(*) FROM branch_a.Customer WHERE Contact = '555-FAIL1') as BranchA_Count,
    (SELECT COUNT(*) FROM branch_b.Customer WHERE Contact = '555-FAIL2') as BranchB_Count,
    CASE 
        WHEN (SELECT COUNT(*) FROM branch_a.Customer WHERE Contact = '555-FAIL1') = 0
         AND (SELECT COUNT(*) FROM branch_b.Customer WHERE Contact = '555-FAIL2') = 0
        THEN 'PASS: Complete rollback successful'
        ELSE 'FAIL: Partial data remains'
    END as Result;

-- Start transaction
BEGIN;

INSERT INTO branch_a.Customer (FullName, Address, Type, Contact, Region)
VALUES ('Recovery Test A', '789 Recovery Rd', 'Residential', '555-REC1', 'North');

INSERT INTO branch_b.Customer (FullName, Address, Type, Contact, Region)
VALUES ('Recovery Test B', '321 Recovery Ln', 'Commercial', '555-REC2', 'South');

-- Log as prepared
INSERT INTO public.transaction_recovery_log (TransactionID, TransactionType, Status)
VALUES ('TXN_RECOVERY_001', 'Customer_Insert', 'PREPARED');

-- Prepare transaction (simulating coordinator about to crash)
PREPARE TRANSACTION 'TXN_RECOVERY_001';

-- Simulate coordinator crash - transaction remains in prepared state
-- Check unresolved transactions
SELECT 
    'Unresolved Transaction Check' as Status,
    gid as TransactionID,
    prepared as PreparedTime,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - prepared)) as SecondsInPreparedState
FROM pg_prepared_xacts
WHERE gid = 'TXN_RECOVERY_001';

-- Step 1: Identify stuck prepared transactions
CREATE OR REPLACE VIEW v_stuck_prepared_transactions AS
SELECT 
    gid as TransactionID,
    prepared as PreparedTime,
    owner,
    database,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - prepared)) as SecondsStuck,
    CASE 
        WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - prepared)) > 300 THEN 'CRITICAL'
        WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - prepared)) > 60 THEN 'WARNING'
        ELSE 'OK'
    END as Severity
FROM pg_prepared_xacts
WHERE EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - prepared)) > 10
ORDER BY prepared ASC;

-- Check for stuck transactions
SELECT * FROM v_stuck_prepared_transactions;

-- Step 2: Investigate transaction state
SELECT 
    trl.TransactionID,
    trl.TransactionType,
    trl.Status,
    trl.BranchA_Status,
    trl.BranchB_Status,
    trl.ErrorMessage,
    pxt.prepared as PreparedTime
FROM public.transaction_recovery_log trl
LEFT JOIN pg_prepared_xacts pxt ON trl.TransactionID = pxt.gid
WHERE trl.TransactionID = 'TXN_RECOVERY_001';

-- Option A: Commit the prepared transaction (if both branches OK)
COMMIT PREPARED 'TXN_RECOVERY_001';

-- Update recovery log
UPDATE public.transaction_recovery_log
SET 
    Status = 'RECOVERED',
    RecoveryAction = 'MANUAL_COMMIT',
    RecoveryTime = CURRENT_TIMESTAMP
WHERE TransactionID = 'TXN_RECOVERY_001';

-- Verify recovery
SELECT 
    'Recovery Verification' as Test,
    (SELECT COUNT(*) FROM branch_a.Customer WHERE Contact = '555-REC1') as BranchA_Count,
    (SELECT COUNT(*) FROM branch_b.Customer WHERE Contact = '555-REC2') as BranchB_Count,
    CASE 
        WHEN (SELECT COUNT(*) FROM branch_a.Customer WHERE Contact = '555-REC1') = 1
         AND (SELECT COUNT(*) FROM branch_b.Customer WHERE Contact = '555-REC2') = 1
        THEN 'PASS: Recovery successful, data committed'
        ELSE 'FAIL: Data inconsistency detected'
    END as Result;

--Automatic Recovery Function

CREATE OR REPLACE FUNCTION recover_stuck_transactions(
    p_timeout_seconds INT DEFAULT 300
) RETURNS TABLE (
    TransactionID VARCHAR,
    Action VARCHAR,
    Result VARCHAR
) AS $$
DECLARE
    stuck_txn RECORD;
    recovery_action VARCHAR;
BEGIN
    -- Find stuck prepared transactions
    FOR stuck_txn IN 
        SELECT gid, prepared
        FROM pg_prepared_xacts
        WHERE EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - prepared)) > p_timeout_seconds
    LOOP
        -- Check transaction log for decision
        SELECT 
            CASE 
                WHEN BranchA_Status = 'SUCCESS' AND BranchB_Status = 'SUCCESS' THEN 'COMMIT'
                WHEN BranchA_Status = 'FAILED' OR BranchB_Status = 'FAILED' THEN 'ROLLBACK'
                ELSE 'ROLLBACK' -- Default to rollback if uncertain
            END
        INTO recovery_action
        FROM public.transaction_recovery_log
        WHERE TransactionID = stuck_txn.gid;
        
        -- If no log entry, default to rollback
        IF recovery_action IS NULL THEN
            recovery_action := 'ROLLBACK';
        END IF;
        
        -- Execute recovery action
        BEGIN
            IF recovery_action = 'COMMIT' THEN
                EXECUTE 'COMMIT PREPARED ' || quote_literal(stuck_txn.gid);
                TransactionID := stuck_txn.gid;
                Action := 'COMMIT';
                Result := 'SUCCESS';
            ELSE
                EXECUTE 'ROLLBACK PREPARED ' || quote_literal(stuck_txn.gid);
                TransactionID := stuck_txn.gid;
                Action := 'ROLLBACK';
                Result := 'SUCCESS';
            END IF;
            
            -- Update recovery log
            UPDATE public.transaction_recovery_log
            SET 
                Status = 'RECOVERED',
                RecoveryAction = 'AUTOMATIC_' || recovery_action,
                RecoveryTime = CURRENT_TIMESTAMP
            WHERE TransactionID = stuck_txn.gid;
            
            RETURN NEXT;
            
        EXCEPTION WHEN OTHERS THEN
            TransactionID := stuck_txn.gid;
            Action := recovery_action;
            Result := 'FAILED: ' || SQLERRM;
            RETURN NEXT;
        END;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Test automatic recovery (no stuck transactions expected now)
SELECT * FROM recover_stuck_transactions(10);

---Rollback Force Simulation

-- Create a prepared transaction to force rollback
BEGIN;

INSERT INTO branch_a.Meter (CustomerID, InstallationDate, Status)
VALUES (1, CURRENT_DATE, 'Active');

INSERT INTO branch_b.Meter (CustomerID, InstallationDate, Status)
VALUES (6, CURRENT_DATE, 'Active');

PREPARE TRANSACTION 'TXN_FORCE_ROLLBACK';

-- Verify prepared state
SELECT * FROM pg_prepared_xacts WHERE gid = 'TXN_FORCE_ROLLBACK';

-- Force rollback (simulating recovery after failure)
ROLLBACK PREPARED 'TXN_FORCE_ROLLBACK';

-- Verify rollback completed
SELECT 
    'Force Rollback Verification' as Test,
    CASE 
        WHEN NOT EXISTS (SELECT 1 FROM pg_prepared_xacts WHERE gid = 'TXN_FORCE_ROLLBACK')
        THEN 'PASS: Transaction successfully rolled back'
        ELSE 'FAIL: Transaction still in prepared state'
    END as Result;

-----Recovery Monitoring Dashboard

CREATE OR REPLACE VIEW v_recovery_dashboard AS
SELECT 
    -- Current prepared transactions
    (SELECT COUNT(*) FROM pg_prepared_xacts) as CurrentPreparedTransactions,
    
    -- Stuck transactions (>5 minutes)
    (SELECT COUNT(*) FROM pg_prepared_xacts 
     WHERE EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - prepared)) > 300) as StuckTransactions,
    
    -- Total recovery attempts
    (SELECT COUNT(*) FROM public.transaction_recovery_log 
     WHERE RecoveryAction IS NOT NULL) as TotalRecoveryAttempts,
    
    -- Successful recoveries
    (SELECT COUNT(*) FROM public.transaction_recovery_log 
     WHERE Status = 'RECOVERED') as SuccessfulRecoveries,
    
    -- Failed transactions
    (SELECT COUNT(*) FROM public.transaction_recovery_log 
     WHERE Status = 'FAILED') as FailedTransactions,
    
    -- Average recovery time
    (SELECT AVG(EXTRACT(EPOCH FROM (RecoveryTime - StartTime))) 
     FROM public.transaction_recovery_log 
     WHERE RecoveryTime IS NOT NULL) as AvgRecoveryTimeSeconds;

-- View recovery dashboard
SELECT * FROM v_recovery_dashboard;

-- Detailed recovery log
SELECT 
    TransactionID,
    TransactionType,
    Status,
    BranchA_Status,
    BranchB_Status,
    RecoveryAction,
    EXTRACT(EPOCH FROM (RecoveryTime - StartTime)) as RecoveryTimeSeconds,
    ErrorMessage
FROM public.transaction_recovery_log
ORDER BY StartTime DESC
LIMIT 10;

-- TASK 6: Distributed Concurrency Control
-- Demonstrates lock conflicts and concurrency control in distributed database
---------========================================================
-- Setup: Create Lock Monitoring Tables
CREATE TABLE IF NOT EXISTS public.lock_test_log (
    LogID SERIAL PRIMARY KEY,
    SessionID INT,
    SessionName VARCHAR(50),
    Operation VARCHAR(100),
    TargetTable VARCHAR(50),
    TargetRecord VARCHAR(100),
    LockType VARCHAR(50),
    LockAcquired BOOLEAN,
    WaitTime INTERVAL,
    Timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-----Basic Lock Conflict Simulation
-- Start transaction in Session 1
BEGIN;

-- Update customer (acquires row-level lock)
UPDATE branch_a.Customer
SET Address = '999 Updated Address'
WHERE CustomerID = 1;

-- Log the operation
INSERT INTO public.lock_test_log (SessionID, SessionName, Operation, TargetTable, TargetRecord, LockType, LockAcquired)
VALUES (pg_backend_pid(), 'Session_1', 'UPDATE Customer', 'branch_a.Customer', 'CustomerID=1', 'ROW EXCLUSIVE', TRUE);

-- Check current locks held by this session
SELECT 
    pid,
    locktype,
    relation::regclass as table_name,
    mode,
    granted,
    pg_blocking_pids(pid) as blocking_pids
FROM pg_locks
WHERE pid = pg_backend_pid()
  AND relation IS NOT NULL;
-- View all current locks
CREATE OR REPLACE VIEW v_current_locks AS
SELECT 
    l.pid as ProcessID,
    a.usename as Username,
    a.application_name as Application,
    l.locktype as LockType,
    l.relation::regclass as TableName,
    l.mode as LockMode,
    l.granted as LockGranted,
    a.state as SessionState,
    a.query as CurrentQuery,
    pg_blocking_pids(l.pid) as BlockingPIDs,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - a.query_start)) as QueryDurationSeconds
FROM pg_locks l
LEFT JOIN pg_stat_activity a ON l.pid = a.pid
WHERE l.relation IS NOT NULL
ORDER BY l.granted, a.query_start;

-- View current locks
SELECT * FROM v_current_locks;

-- View blocking relationships
CREATE OR REPLACE VIEW v_blocking_locks AS
SELECT 
    blocked.pid as BlockedPID,
    blocked.usename as BlockedUser,
    blocked.query as BlockedQuery,
    blocking.pid as BlockingPID,
    blocking.usename as BlockingUser,
    blocking.query as BlockingQuery,
    blocked_locks.mode as BlockedLockMode,
    blocking_locks.mode as BlockingLockMode,
    blocked_locks.relation::regclass as TableName,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - blocked.query_start)) as BlockedDurationSeconds
FROM pg_stat_activity blocked
JOIN pg_locks blocked_locks ON blocked.pid = blocked_locks.pid
JOIN pg_locks blocking_locks ON blocked_locks.relation = blocking_locks.relation
    AND blocked_locks.pid != blocking_locks.pid
    AND NOT blocked_locks.granted
    AND blocking_locks.granted
JOIN pg_stat_activity blocking ON blocking_locks.pid = blocking.pid
WHERE blocked.pid != pg_backend_pid()
ORDER BY blocked.query_start;

-- View blocking locks
SELECT * FROM v_blocking_locks;
COMMIT;

----Simulate distributed transaction locking records in both branches

BEGIN;

-- Lock customer in Branch A
UPDATE branch_a.Customer
SET Address = 'Distributed Lock Test A'
WHERE CustomerID = 1;

-- Lock customer in Branch B
UPDATE branch_b.Customer
SET Address = 'Distributed Lock Test B'
WHERE CustomerID = 6;

-- Check locks across both branches
SELECT 
    'Branch A Locks' as Source,
    l.pid,
    l.locktype,
    l.relation::regclass as table_name,
    l.mode,
    l.granted
FROM pg_locks l
WHERE l.pid = pg_backend_pid()
  AND l.relation::regclass::text LIKE 'branch_a.%'
UNION ALL
SELECT 
    'Branch B Locks' as Source,
    l.pid,
    l.locktype,
    l.relation::regclass as table_name,
    l.mode,
    l.granted
FROM pg_locks l
WHERE l.pid = pg_backend_pid()
  AND l.relation::regclass::text LIKE 'branch_b.%';

COMMIT;
------Lock Timeout Configuration
------------------------------------------------
SET lock_timeout = '5s';

-- Demonstrate timeout
BEGIN;

-- This will timeout if lock cannot be acquired within 5 seconds
-- (Requires another session holding the lock)
UPDATE branch_a.Customer
SET Address = 'Timeout Test'
WHERE CustomerID = 1;

COMMIT;

-- Reset timeout
RESET lock_timeout;

-- Session 1: Acquire advisory lock
SELECT pg_advisory_lock(1001);

-- Log advisory lock
INSERT INTO public.lock_test_log (SessionID, SessionName, Operation, TargetTable, LockType, LockAcquired)
VALUES (pg_backend_pid(), 'Session_1', 'Advisory Lock', 'Custom Resource 1001', 'ADVISORY', TRUE);
---------------------------------------------------------------

-- Check advisory locks
SELECT 
    locktype,
    objid as LockID,
    mode,
    granted,
    pid
FROM pg_locks
WHERE locktype = 'advisory'
  AND pid = pg_backend_pid();

------------------------------------
SELECT pg_advisory_unlock(1001);

-----------------------------------
-- Create function to monitor lock escalation
CREATE OR REPLACE FUNCTION monitor_lock_escalation()
RETURNS TABLE (
    LockType VARCHAR,
    LockCount BIGINT,
    GrantedCount BIGINT,
    WaitingCount BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        l.locktype::VARCHAR as LockType,
        COUNT(*) as LockCount,
        SUM(CASE WHEN l.granted THEN 1 ELSE 0 END) as GrantedCount,
        SUM(CASE WHEN NOT l.granted THEN 1 ELSE 0 END) as WaitingCount
    FROM pg_locks l
    GROUP BY l.locktype
    ORDER BY COUNT(*) DESC;
END;
$$ LANGUAGE plpgsql;

-- Monitor current lock distribution
SELECT * FROM monitor_lock_escalation();

-- Test different isolation levels

-- READ COMMITTED (default)
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
SELECT * FROM branch_a.Customer WHERE CustomerID = 1;
-- Acquires shared lock, released after SELECT
COMMIT;

-- REPEATABLE READ
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SELECT * FROM branch_a.Customer WHERE CustomerID = 1;
-- Acquires shared lock, held until transaction end
COMMIT;

-- SERIALIZABLE
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SELECT * FROM branch_a.Customer WHERE CustomerID = 1;
-- Strictest isolation, prevents phantom reads
COMMIT;
------------------------------------------------------
CREATE OR REPLACE VIEW v_lock_wait_statistics AS
SELECT 
    schemaname,
    relname as table_name,
    n_tup_ins as inserts,
    n_tup_upd as updates,
    n_tup_del as deletes,
    n_live_tup as live_tuples,
    n_dead_tup as dead_tuples,
    last_vacuum,
    last_autovacuum
FROM pg_stat_user_tables
WHERE schemaname IN ('branch_a', 'branch_b', 'public')
ORDER BY n_tup_upd DESC;

-- View lock wait statistics
SELECT * FROM v_lock_wait_statistics;
TRUNCATE public.lock_test_log;

--------- TASK 7: Parallel Data Loading / ETL Simulation
---Demonstrates parallel DML and ETL operations with performance comparison
---==================================================================
-- Staging table for raw data import
CREATE TABLE IF NOT EXISTS public.staging_water_usage (
    StagingID SERIAL PRIMARY KEY,
    CustomerRef VARCHAR(50),
    MeterNumber VARCHAR(50),
    ReadingDate DATE,
    ReadingValue DECIMAL(10,2),
    Region VARCHAR(20),
    CustomerType VARCHAR(20),
    LoadTimestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Target table for transformed data
CREATE TABLE IF NOT EXISTS public.fact_water_consumption (
    FactID SERIAL PRIMARY KEY,
    CustomerID INT,
    MeterID INT,
    ReadingDate DATE,
    CurrentReading DECIMAL(10,2),
    PreviousReading DECIMAL(10,2),
    Consumption DECIMAL(10,2),
    ConsumptionCategory VARCHAR(20),
    Region VARCHAR(20),
    CustomerType VARCHAR(20),
    BillingAmount DECIMAL(10,2),
    LoadDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- ETL performance log
CREATE TABLE IF NOT EXISTS public.etl_performance_log (
    LogID SERIAL PRIMARY KEY,
    ETLProcess VARCHAR(100),
    ExecutionMode VARCHAR(50),
    RowsProcessed BIGINT,
    StartTime TIMESTAMP,
    EndTime TIMESTAMP,
    DurationSeconds DECIMAL(10,3),
    ThroughputRowsPerSecond DECIMAL(10,2)
);
--Staging records
INSERT INTO public.staging_water_usage (CustomerRef, MeterNumber, ReadingDate, ReadingValue, Region, CustomerType)
SELECT 
    'CUST-' || LPAD((random() * 10000)::INT::TEXT, 5, '0') as CustomerRef,
    'MTR-' || LPAD((random() * 50000)::INT::TEXT, 6, '0') as MeterNumber,
    CURRENT_DATE - (random() * 730)::INT as ReadingDate,
    (random() * 1000 + 50)::DECIMAL(10,2) as ReadingValue,
    CASE WHEN random() < 0.5 THEN 'North' ELSE 'South' END as Region,
    CASE WHEN random() < 0.7 THEN 'Residential' ELSE 'Commercial' END as CustomerType
FROM generate_series(1, 500000);

-- Create indexes on staging table
CREATE INDEX idx_staging_customer_ref ON public.staging_water_usage(CustomerRef);
CREATE INDEX idx_staging_meter ON public.staging_water_usage(MeterNumber);
CREATE INDEX idx_staging_date ON public.staging_water_usage(ReadingDate);

-- Analyze for optimizer
ANALYZE public.staging_water_usage;

-----------------------------------------------
-- Disable parallel execution
SET max_parallel_workers_per_gather = 0;
SET max_parallel_maintenance_workers = 0;

-- Record start time
DO $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_rows_processed BIGINT;
BEGIN
    v_start_time := clock_timestamp();
    
    -- Serial ETL: Transform and load data
    INSERT INTO public.fact_water_consumption 
    (CustomerID, MeterID, ReadingDate, CurrentReading, PreviousReading, Consumption, ConsumptionCategory, Region, CustomerType, BillingAmount)
    SELECT 
        (SUBSTRING(s.CustomerRef FROM 6))::INT as CustomerID,
        (SUBSTRING(s.MeterNumber FROM 5))::INT as MeterID,
        s.ReadingDate,
        s.ReadingValue as CurrentReading,
        LAG(s.ReadingValue, 1, 0) OVER (PARTITION BY s.MeterNumber ORDER BY s.ReadingDate) as PreviousReading,
        s.ReadingValue - LAG(s.ReadingValue, 1, 0) OVER (PARTITION BY s.MeterNumber ORDER BY s.ReadingDate) as Consumption,
        CASE 
            WHEN s.ReadingValue - LAG(s.ReadingValue, 1, 0) OVER (PARTITION BY s.MeterNumber ORDER BY s.ReadingDate) < 20 THEN 'Low'
            WHEN s.ReadingValue - LAG(s.ReadingValue, 1, 0) OVER (PARTITION BY s.MeterNumber ORDER BY s.ReadingDate) < 50 THEN 'Medium'
            ELSE 'High'
        END as ConsumptionCategory,
        s.Region,
        s.CustomerType,
        CASE 
            WHEN s.CustomerType = 'Residential' THEN (s.ReadingValue - LAG(s.ReadingValue, 1, 0) OVER (PARTITION BY s.MeterNumber ORDER BY s.ReadingDate)) * 2.50
            ELSE (s.ReadingValue - LAG(s.ReadingValue, 1, 0) OVER (PARTITION BY s.MeterNumber ORDER BY s.ReadingDate)) * 3.75
        END as BillingAmount
    FROM public.staging_water_usage s
    WHERE s.ReadingDate >= CURRENT_DATE - INTERVAL '365 days';
    
    GET DIAGNOSTICS v_rows_processed = ROW_COUNT;
    v_end_time := clock_timestamp();
    
    -- Log performance
    INSERT INTO public.etl_performance_log (ETLProcess, ExecutionMode, RowsProcessed, StartTime, EndTime, DurationSeconds, ThroughputRowsPerSecond)
    VALUES (
        'Water Consumption ETL',
        'Serial',
        v_rows_processed,
        v_start_time,
        v_end_time,
        EXTRACT(EPOCH FROM (v_end_time - v_start_time)),
        v_rows_processed / EXTRACT(EPOCH FROM (v_end_time - v_start_time))
    );
END $$;

-- Clear target table for parallel test
TRUNCATE public.fact_water_consumption;
----------------------------------------------------------------------------
-- Enable parallel execution
SET max_parallel_workers_per_gather = 4;
SET max_parallel_maintenance_workers = 4;

-- Record start time
DO $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_rows_processed BIGINT;
BEGIN
    v_start_time := clock_timestamp();
    
    -- Parallel ETL: Transform and load data
    INSERT INTO public.fact_water_consumption 
    (CustomerID, MeterID, ReadingDate, CurrentReading, PreviousReading, Consumption, ConsumptionCategory, Region, CustomerType, BillingAmount)
    SELECT 
        (SUBSTRING(s.CustomerRef FROM 6))::INT as CustomerID,
        (SUBSTRING(s.MeterNumber FROM 5))::INT as MeterID,
        s.ReadingDate,
        s.ReadingValue as CurrentReading,
        LAG(s.ReadingValue, 1, 0) OVER (PARTITION BY s.MeterNumber ORDER BY s.ReadingDate) as PreviousReading,
        s.ReadingValue - LAG(s.ReadingValue, 1, 0) OVER (PARTITION BY s.MeterNumber ORDER BY s.ReadingDate) as Consumption,
        CASE 
            WHEN s.ReadingValue - LAG(s.ReadingValue, 1, 0) OVER (PARTITION BY s.MeterNumber ORDER BY s.ReadingDate) < 20 THEN 'Low'
            WHEN s.ReadingValue - LAG(s.ReadingValue, 1, 0) OVER (PARTITION BY s.MeterNumber ORDER BY s.ReadingDate) < 50 THEN 'Medium'
            ELSE 'High'
        END as ConsumptionCategory,
        s.Region,
        s.CustomerType,
        CASE 
            WHEN s.CustomerType = 'Residential' THEN (s.ReadingValue - LAG(s.ReadingValue, 1, 0) OVER (PARTITION BY s.MeterNumber ORDER BY s.ReadingDate)) * 2.50
            ELSE (s.ReadingValue - LAG(s.ReadingValue, 1, 0) OVER (PARTITION BY s.MeterNumber ORDER BY s.ReadingDate)) * 3.75
        END as BillingAmount
    FROM public.staging_water_usage s
    WHERE s.ReadingDate >= CURRENT_DATE - INTERVAL '365 days';
    
    GET DIAGNOSTICS v_rows_processed = ROW_COUNT;
    v_end_time := clock_timestamp();
    
    -- Log performance
    INSERT INTO public.etl_performance_log (ETLProcess, ExecutionMode, RowsProcessed, StartTime, EndTime, DurationSeconds, ThroughputRowsPerSecond)
    VALUES (
        'Water Consumption ETL',
        'Parallel (4 workers)',
        v_rows_processed,
        v_start_time,
        v_end_time,
        EXTRACT(EPOCH FROM (v_end_time - v_start_time)),
        v_rows_processed / EXTRACT(EPOCH FROM (v_end_time - v_start_time))
    );
END $$;
--------------------------------------------------------------------
-- Serial aggregation
SET max_parallel_workers_per_gather = 0;

DO $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
BEGIN
    v_start_time := clock_timestamp();
    
    -- Perform aggregation
    PERFORM 
        Region,
        CustomerType,
        ConsumptionCategory,
        COUNT(*) as RecordCount,
        SUM(Consumption) as TotalConsumption,
        AVG(Consumption) as AvgConsumption,
        SUM(BillingAmount) as TotalBilling
    FROM public.fact_water_consumption
    GROUP BY Region, CustomerType, ConsumptionCategory;
    
    v_end_time := clock_timestamp();
    
    INSERT INTO public.etl_performance_log (ETLProcess, ExecutionMode, RowsProcessed, StartTime, EndTime, DurationSeconds)
    VALUES (
        'Consumption Aggregation',
        'Serial',
        (SELECT COUNT(*) FROM public.fact_water_consumption),
        v_start_time,
        v_end_time,
        EXTRACT(EPOCH FROM (v_end_time - v_start_time))
    );
END $$;

-- Parallel aggregation
SET max_parallel_workers_per_gather = 4;

DO $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
BEGIN
    v_start_time := clock_timestamp();
    
    -- Perform aggregation
    PERFORM 
        Region,
        CustomerType,
        ConsumptionCategory,
        COUNT(*) as RecordCount,
        SUM(Consumption) as TotalConsumption,
        AVG(Consumption) as AvgConsumption,
        SUM(BillingAmount) as TotalBilling
    FROM public.fact_water_consumption
    GROUP BY Region, CustomerType, ConsumptionCategory;
    
    v_end_time := clock_timestamp();
    
    INSERT INTO public.etl_performance_log (ETLProcess, ExecutionMode, RowsProcessed, StartTime, EndTime, DurationSeconds)
    VALUES (
        'Consumption Aggregation',
        'Parallel (4 workers)',
        (SELECT COUNT(*) FROM public.fact_water_consumption),
        v_start_time,
        v_end_time,
        EXTRACT(EPOCH FROM (v_end_time - v_start_time))
    );
END $$;
-----------------------------------------------------------------
-- Compare serial vs parallel performance
SELECT 
    ETLProcess,
    ExecutionMode,
    RowsProcessed,
    DurationSeconds,
    ThroughputRowsPerSecond,
    CASE 
        WHEN LAG(DurationSeconds) OVER (PARTITION BY ETLProcess ORDER BY LogID) IS NOT NULL
        THEN ROUND(((LAG(DurationSeconds) OVER (PARTITION BY ETLProcess ORDER BY LogID) - DurationSeconds) / 
                    LAG(DurationSeconds) OVER (PARTITION BY ETLProcess ORDER BY LogID) * 100)::NUMERIC, 2)
        ELSE NULL
    END as PerformanceImprovementPercent
FROM public.etl_performance_log
ORDER BY ETLProcess, LogID;
-----------------------------------------------------------------
-- Drop existing indexes
DROP INDEX IF EXISTS idx_fact_customer;
DROP INDEX IF EXISTS idx_fact_date;
DROP INDEX IF EXISTS idx_fact_region;

-- Serial index creation
SET max_parallel_maintenance_workers = 0;

DO $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
BEGIN
    v_start_time := clock_timestamp();
    
    CREATE INDEX idx_fact_customer_serial ON public.fact_water_consumption(CustomerID);
    
    v_end_time := clock_timestamp();
    
    INSERT INTO public.etl_performance_log (ETLProcess, ExecutionMode, StartTime, EndTime, DurationSeconds)
    VALUES (
        'Index Creation',
        'Serial',
        v_start_time,
        v_end_time,
        EXTRACT(EPOCH FROM (v_end_time - v_start_time))
    );
END $$;

DROP INDEX idx_fact_customer_serial;

-- Parallel index creation
SET max_parallel_maintenance_workers = 4;

DO $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
BEGIN
    v_start_time := clock_timestamp();
    
    CREATE INDEX idx_fact_customer_parallel ON public.fact_water_consumption(CustomerID);
    
    v_end_time := clock_timestamp();
    
    INSERT INTO public.etl_performance_log (ETLProcess, ExecutionMode, StartTime, EndTime, DurationSeconds)
    VALUES (
        'Index Creation',
        'Parallel (4 workers)',
        v_start_time,
        v_end_time,
        EXTRACT(EPOCH FROM (v_end_time - v_start_time))
    );
END $$;
-------------------------------------------------------------
-- View final performance comparison
SELECT 
    ETLProcess,
    ExecutionMode,
    ROUND(DurationSeconds, 3) as DurationSeconds,
    ROUND(ThroughputRowsPerSecond, 2) as RowsPerSecond
FROM public.etl_performance_log
ORDER BY ETLProcess, LogID;

-- Reset settings
RESET max_parallel_workers_per_gather;
RESET max_parallel_maintenance_workers;
-------------------------------------------------------------
-- TASK 8: Three-Tier Client-Server Architecture Design
--This file documents the three-tier architecture for the Water Billing System
----==================================================================
-- Create API endpoint documentation table
CREATE TABLE IF NOT EXISTS public.api_endpoints (
    EndpointID SERIAL PRIMARY KEY,
    HTTPMethod VARCHAR(10),
    EndpointPath VARCHAR(255),
    Description TEXT,
    RequestBody JSONB,
    ResponseBody JSONB,
    AuthRequired BOOLEAN DEFAULT TRUE,
    Tier VARCHAR(20) DEFAULT 'Presentation'
);
-----------------------------------------------------------
-- Document API endpoints
INSERT INTO public.api_endpoints (HTTPMethod, EndpointPath, Description, RequestBody, ResponseBody, AuthRequired) VALUES
('GET', '/api/customers', 'List all customers', NULL, '{"customers": [{"id": 1, "name": "John Doe"}]}', TRUE),
('POST', '/api/customers', 'Create new customer', '{"name": "John Doe", "address": "123 Main St", "type": "Residential"}', '{"id": 1, "message": "Customer created"}', TRUE),
('GET', '/api/customers/:id', 'Get customer details', NULL, '{"id": 1, "name": "John Doe", "meters": [...]}', TRUE),
('PUT', '/api/customers/:id', 'Update customer', '{"address": "456 New St"}', '{"message": "Customer updated"}', TRUE),
('GET', '/api/meters/:id/readings', 'Get meter readings', NULL, '{"readings": [{"date": "2024-01-01", "value": 150.5}]}', TRUE),
('POST', '/api/readings', 'Submit new reading', '{"meterId": 1, "value": 175.5}', '{"id": 1, "billGenerated": true}', TRUE),
('GET', '/api/bills', 'List customer bills', NULL, '{"bills": [{"id": 1, "amount": 62.50, "status": "Unpaid"}]}', TRUE),
('POST', '/api/payments', 'Process payment', '{"billId": 1, "amount": 62.50, "method": "Card"}', '{"paymentId": 1, "status": "Success"}', TRUE),
('GET', '/api/reports/consumption', 'Consumption report', NULL, '{"data": [...], "charts": [...]}', TRUE),
('GET', '/api/admin/dashboard', 'Admin dashboard data', NULL, '{"totalCustomers": 100, "totalRevenue": 50000}', TRUE);
---------------------------------------------------------------------------------------
-- Customer Service: Register new customer
CREATE OR REPLACE FUNCTION app_register_customer(
    p_full_name VARCHAR,
    p_address VARCHAR,
    p_type VARCHAR,
    p_contact VARCHAR,
    p_region VARCHAR
) RETURNS JSONB AS $$
DECLARE
    v_customer_id INT;
    v_result JSONB;
BEGIN
    -- Determine which branch based on region
    IF p_region = 'North' THEN
        INSERT INTO branch_a.Customer (FullName, Address, Type, Contact, Region)
        VALUES (p_full_name, p_address, p_type, p_contact, p_region)
        RETURNING CustomerID INTO v_customer_id;
    ELSE
        INSERT INTO branch_b.Customer (FullName, Address, Type, Contact, Region)
        VALUES (p_full_name, p_address, p_type, p_contact, p_region)
        RETURNING CustomerID INTO v_customer_id;
    END IF;
    
    v_result := jsonb_build_object(
        'success', true,
        'customerId', v_customer_id,
        'message', 'Customer registered successfully',
        'branch', CASE WHEN p_region = 'North' THEN 'Branch_A' ELSE 'Branch_B' END
    );
    
    RETURN v_result;
EXCEPTION WHEN OTHERS THEN
    RETURN jsonb_build_object(
        'success', false,
        'error', SQLERRM
    );
END;
$$ LANGUAGE plpgsql;

-- Billing Service: Calculate and generate bill
CREATE OR REPLACE FUNCTION app_generate_bill(
    p_reading_id INT,
    p_region VARCHAR
) RETURNS JSONB AS $$
DECLARE
    v_consumption DECIMAL(10,2);
    v_customer_type VARCHAR(20);
    v_amount_due DECIMAL(10,2);
    v_bill_id INT;
    v_result JSONB;
BEGIN
    -- Get consumption and customer type
    IF p_region = 'North' THEN
        SELECT r.Consumption, c.Type
        INTO v_consumption, v_customer_type
        FROM branch_a.Reading r
        JOIN branch_a.Meter m ON r.MeterID = m.MeterID
        JOIN branch_a.Customer c ON m.CustomerID = c.CustomerID
        WHERE r.ReadingID = p_reading_id;

----------------------------------------------------------------
 -- Calculate amount
        v_amount_due := CASE 
            WHEN v_customer_type = 'Residential' THEN v_consumption * 2.50
            ELSE v_consumption * 3.75
        END;
        
        -- Create bill
        INSERT INTO branch_a.Bill (ReadingID, AmountDue, DueDate, Status)
        VALUES (p_reading_id, v_amount_due, CURRENT_DATE + INTERVAL '30 days', 'Unpaid')
        RETURNING BillID INTO v_bill_id;
    ELSE
        SELECT r.Consumption, c.Type
        INTO v_consumption, v_customer_type
        FROM branch_b.Reading r
        JOIN branch_b.Meter m ON r.MeterID = m.MeterID
        JOIN branch_b.Customer c ON m.CustomerID = c.CustomerID
        WHERE r.ReadingID = p_reading_id;
        
        v_amount_due := CASE 
            WHEN v_customer_type = 'Residential' THEN v_consumption * 2.50
            ELSE v_consumption * 3.75
        END;
        
        INSERT INTO branch_b.Bill (ReadingID, AmountDue, DueDate, Status)
        VALUES (p_reading_id, v_amount_due, CURRENT_DATE + INTERVAL '30 days', 'Unpaid')
        RETURNING BillID INTO v_bill_id;
    END IF;
    
    v_result := jsonb_build_object(
        'success', true,
        'billId', v_bill_id,
        'amountDue', v_amount_due,
        'consumption', v_consumption,
        'customerType', v_customer_type,
        'dueDate', CURRENT_DATE + INTERVAL '30 days'
    );
    
    RETURN v_result;
EXCEPTION WHEN OTHERS THEN
    RETURN jsonb_build_object(
        'success', false,
        'error', SQLERRM
    );
END;
$$ LANGUAGE plpgsql;
------------------------------------------------------------
-- Meter Service: Submit reading
CREATE OR REPLACE FUNCTION app_submit_reading(
    p_meter_id INT,
    p_reading_value DECIMAL,
    p_region VARCHAR
) RETURNS JSONB AS $$
DECLARE
    v_reading_id INT;
    v_last_reading DECIMAL(10,2);
    v_consumption DECIMAL(10,2);
    v_bill_result JSONB;
    v_result JSONB;
BEGIN
    -- Get last reading
    IF p_region = 'North' THEN
        SELECT LastReading INTO v_last_reading
        FROM branch_a.Meter
        WHERE MeterID = p_meter_id;
        
        -- Calculate consumption
        v_consumption := p_reading_value - v_last_reading;
        
        -- Insert reading
        INSERT INTO branch_a.Reading (MeterID, ReadingDate, CurrentReading, Consumption)
        VALUES (p_meter_id, CURRENT_DATE, p_reading_value, v_consumption)
        RETURNING ReadingID INTO v_reading_id;
        
        -- Update meter
        UPDATE branch_a.Meter
        SET LastReading = p_reading_value
        WHERE MeterID = p_meter_id;
    ELSE
        SELECT LastReading INTO v_last_reading
        FROM branch_b.Meter
        WHERE MeterID = p_meter_id;
        
        v_consumption := p_reading_value - v_last_reading;
        
        INSERT INTO branch_b.Reading (MeterID, ReadingDate, CurrentReading, Consumption)
        VALUES (p_meter_id, CURRENT_DATE, p_reading_value, v_consumption)
        RETURNING ReadingID INTO v_reading_id;
        
        UPDATE branch_b.Meter
        SET LastReading = p_reading_value
        WHERE MeterID = p_meter_id;
    END IF;
    ------------------------------------------------------------
	-- Generate bill automatically
    v_bill_result := app_generate_bill(v_reading_id, p_region);
    
    v_result := jsonb_build_object(
        'success', true,
        'readingId', v_reading_id,
        'consumption', v_consumption,
        'bill', v_bill_result
    );
    
    RETURN v_result;
EXCEPTION WHEN OTHERS THEN
    RETURN jsonb_build_object(
        'success', false,
        'error', SQLERRM
    );
END;
$$ LANGUAGE plpgsql;
--------------------------------------------------------------
-- Tier 1 (Presentation) → Tier 2 (Application) → Tier 3 (Database)

SELECT app_register_customer(
    'Alice Johnson',
    '789 Water St',
    'Residential',
    '555-FLOW1',
    'North'
) as registration_result;

-- Example 2: Reading Submission and Billing Flow
-- Tier 1 → Tier 2 → Tier 3 (with automatic bill generation)

SELECT app_submit_reading(
    1,  -- meter_id
    200.50,  -- reading_value
    'North'  -- region
) as reading_submission_result;
----------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.architecture_documentation (
    DocID SERIAL PRIMARY KEY,
    Component VARCHAR(100),
    Tier VARCHAR(20),
    Technology VARCHAR(100),
    Purpose TEXT,
    Interactions TEXT
);
-----------------------------------------------------------
--Tier 1 (Presentation):
--Frontend: React.js / Next.js
-- Mobile: React Native
--Admin: Next.js Dashboard

--Tier 2 (Application):
--- API Server: Node.js / Express or Python / FastAPI
-- Business Logic: PostgreSQL Functions (PL/pgSQL)
----Connection Pool: PgBouncer
----API Gateway: Kong / AWS API Gateway

--Tier 3 (Data):
--- Database: PostgreSQL 16+
-- Replication: Streaming Replication
--- -Backup: pg_dump / WAL archiving
- ---Monitoring: pg_stat_statements

-------------------------------------------------------------
INSERT INTO public.architecture_documentation (Component, Tier, Technology, Purpose, Interactions) VALUES
('Web Application', 'Presentation', 'React.js / Next.js', 'Customer-facing web interface', 'Calls REST API endpoints'),
('Mobile App', 'Presentation', 'React Native', 'Mobile interface for customers', 'Calls REST API endpoints'),
('Admin Dashboard', 'Presentation', 'Next.js', 'Administrative interface', 'Calls REST API with elevated permissions'),
('API Gateway', 'Application', 'Kong / Express', 'Request routing and authentication', 'Routes to appropriate services'),
('Customer Service', 'Application', 'Node.js / PL/pgSQL', 'Customer management logic', 'Interacts with Customer tables'),
('Billing Service', 'Application', 'Node.js / PL/pgSQL', 'Billing calculation and generation', 'Interacts with Bill and Payment tables'),
('Meter Service', 'Application', 'Node.js / PL/pgSQL', 'Meter reading management', 'Interacts with Meter and Reading tables'),
('Connection Pool', 'Application', 'PgBouncer', 'Database connection management', 'Manages connections to all database nodes'),
('Branch A Database', 'Data', 'PostgreSQL 16', 'North region data storage', 'Stores customers, meters, readings, bills for North'),
('Branch B Database', 'Data', 'PostgreSQL 16', 'South region data storage', 'Stores customers, meters, readings, bills for South'),
('Central Database', 'Data', 'PostgreSQL 16', 'Aggregated reporting data', 'Stores reports, analytics, audit logs'),
('Database Links', 'Data', 'postgres_fdw', 'Cross-branch queries', 'Enables distributed queries across branches');
-- View architecture documentation
SELECT * FROM public.architecture_documentation ORDER BY Tier, Component;

-- TASK 9: Distributed Query Optimization
---Demonstrates query optimization techniques for distributed databases
--==================================================================
-----------Setup: Enable Query Analysis Tools

-- Load pg_stat_statements extension for query performance tracking
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Reset statistics
SELECT pg_stat_statements_reset();
-- Query : Unoptimized distributed join
EXPLAIN (ANALYZE, BUFFERS, COSTS, TIMING, VERBOSE)
SELECT 
    c.FullName,
    c.Region,
    c.Type,
    COUNT(r.ReadingID) as TotalReadings,
    SUM(r.Consumption) as TotalConsumption,
    SUM(b.AmountDue) as TotalBilled
FROM (
    SELECT * FROM branch_a.Customer
    UNION ALL
    SELECT * FROM branch_b.Customer
) c
LEFT JOIN (
    SELECT * FROM branch_a.Meter
    UNION ALL
    SELECT * FROM branch_b.Meter
) m ON c.CustomerID = m.CustomerID
LEFT JOIN (
    SELECT * FROM branch_a.Reading
    UNION ALL
    SELECT * FROM branch_b.Reading
) r ON m.MeterID = r.MeterID
LEFT JOIN (
    SELECT * FROM branch_a.Bill
    UNION ALL
    SELECT * FROM branch_b.Bill
) b ON r.ReadingID = b.ReadingID
GROUP BY c.CustomerID, c.FullName, c.Region, c.Type
ORDER BY TotalConsumption DESC;
-- Query: Optimized with materialized CTEs to reduce data movement
EXPLAIN (ANALYZE, BUFFERS, COSTS, TIMING, VERBOSE)
WITH customer_data AS MATERIALIZED (
    SELECT CustomerID, FullName, Region, Type FROM branch_a.Customer
    UNION ALL
    SELECT CustomerID, FullName, Region, Type FROM branch_b.Customer
),
meter_data AS MATERIALIZED (
    SELECT CustomerID, MeterID FROM branch_a.Meter
    UNION ALL
    SELECT CustomerID, MeterID FROM branch_b.Meter
),
reading_summary AS MATERIALIZED (
    SELECT 
        MeterID,
        COUNT(*) as ReadingCount,
        SUM(Consumption) as TotalConsumption
    FROM (
        SELECT MeterID, Consumption FROM branch_a.Reading
        UNION ALL
        SELECT MeterID, Consumption FROM branch_b.Reading
    ) readings
    GROUP BY MeterID
),
billing_summary AS MATERIALIZED (
    SELECT 
        ReadingID,
        SUM(AmountDue) as TotalBilled
    FROM (
        SELECT ReadingID, AmountDue FROM branch_a.Bill
        UNION ALL
        SELECT ReadingID, AmountDue FROM branch_b.Bill
    ) bills
    GROUP BY ReadingID
)
SELECT 
    c.FullName,
    c.Region,
    c.Type,
    COALESCE(rs.ReadingCount, 0) as TotalReadings,
    COALESCE(rs.TotalConsumption, 0) as TotalConsumption,
    COALESCE(bs.TotalBilled, 0) as TotalBilled
FROM customer_data c
LEFT JOIN meter_data m ON c.CustomerID = m.CustomerID
LEFT JOIN reading_summary rs ON m.MeterID = rs.MeterID
LEFT JOIN billing_summary bs ON rs.MeterID = bs.MeterID
ORDER BY rs.TotalConsumption DESC NULLS LAST;

-- Query: Region-specific query (avoids cross-branch join)
EXPLAIN (ANALYZE, BUFFERS, COSTS, TIMING)
SELECT 
    c.FullName,
    c.Region,
    COUNT(r.ReadingID) as TotalReadings,
    SUM(r.Consumption) as TotalConsumption
FROM branch_a.Customer c
LEFT JOIN branch_a.Meter m ON c.CustomerID = m.CustomerID
LEFT JOIN branch_a.Reading r ON m.MeterID = r.MeterID
WHERE c.Region = 'North'
GROUP BY c.CustomerID, c.FullName, c.Region
ORDER BY TotalConsumption DESC;
--------------------------------------------------------------------
-- Create indexes for optimization
CREATE INDEX IF NOT EXISTS idx_branch_a_meter_customer ON branch_a.Meter(CustomerID);
CREATE INDEX IF NOT EXISTS idx_branch_b_meter_customer ON branch_b.Meter(CustomerID);
CREATE INDEX IF NOT EXISTS idx_branch_a_reading_meter ON branch_a.Reading(MeterID);
CREATE INDEX IF NOT EXISTS idx_branch_b_reading_meter ON branch_b.Reading(MeterID);
CREATE INDEX IF NOT EXISTS idx_branch_a_bill_reading ON branch_a.Bill(ReadingID);
CREATE INDEX IF NOT EXISTS idx_branch_b_bill_reading ON branch_b.Bill(ReadingID);

-- Analyze tables to update statistics
ANALYZE branch_a.Customer;
ANALYZE branch_a.Meter;
ANALYZE branch_a.Reading;
ANALYZE branch_a.Bill;
ANALYZE branch_b.Customer;
ANALYZE branch_b.Meter;
ANALYZE branch_b.Reading;
ANALYZE branch_b.Bill;

-- Re-run optimized query with indexes
EXPLAIN (ANALYZE, BUFFERS, COSTS, TIMING)
SELECT 
    c.FullName,
    c.Region,
    COUNT(r.ReadingID) as TotalReadings,
    SUM(r.Consumption) as TotalConsumption
FROM branch_a.Customer c
LEFT JOIN branch_a.Meter m ON c.CustomerID = m.CustomerID
LEFT JOIN branch_a.Reading r ON m.MeterID = r.MeterID
GROUP BY c.CustomerID, c.FullName, c.Region
ORDER BY TotalConsumption DESC;
-----------------------------------------------
---Query: Push aggregation down to each branch before union
EXPLAIN (ANALYZE, BUFFERS, COSTS, TIMING, VERBOSE)
WITH branch_a_summary AS (
    SELECT 
        c.CustomerID,
        c.FullName,
        c.Region,
        c.Type,
        COUNT(r.ReadingID) as TotalReadings,
        SUM(r.Consumption) as TotalConsumption,
        SUM(b.AmountDue) as TotalBilled
    FROM branch_a.Customer c
    LEFT JOIN branch_a.Meter m ON c.CustomerID = m.CustomerID
    LEFT JOIN branch_a.Reading r ON m.MeterID = r.MeterID
    LEFT JOIN branch_a.Bill b ON r.ReadingID = b.ReadingID
    GROUP BY c.CustomerID, c.FullName, c.Region, c.Type
),
branch_b_summary AS (
    SELECT 
        c.CustomerID,
        c.FullName,
        c.Region,
        c.Type,
        COUNT(r.ReadingID) as TotalReadings,
        SUM(r.Consumption) as TotalConsumption,
        SUM(b.AmountDue) as TotalBilled
    FROM branch_b.Customer c
    LEFT JOIN branch_b.Meter m ON c.CustomerID = m.CustomerID
    LEFT JOIN branch_b.Reading r ON m.MeterID = r.MeterID
    LEFT JOIN branch_b.Bill b ON r.ReadingID = b.ReadingID
    GROUP BY c.CustomerID, c.FullName, c.Region, c.Type
)
SELECT * FROM branch_a_summary
UNION ALL
SELECT * FROM branch_b_summary
ORDER BY TotalConsumption DESC;

----Query Cost Comparison

-- Create function to compare query costs
CREATE OR REPLACE FUNCTION compare_query_costs()
RETURNS TABLE (
    QueryType VARCHAR,
    TotalCost NUMERIC,
    ExecutionTime NUMERIC,
    RowsReturned BIGINT,
    BuffersHit BIGINT,
    BuffersRead BIGINT
) AS $$
BEGIN
    -- This is a simplified example
    -- In practice, you would parse EXPLAIN output
    
    RETURN QUERY
    SELECT 
        'Unoptimized Distributed Join'::VARCHAR,
        1500.00::NUMERIC,
        250.00::NUMERIC,
        10::BIGINT,
        1000::BIGINT,
        500::BIGINT
    UNION ALL
    SELECT 
        'Optimized with CTEs'::VARCHAR,
        800.00::NUMERIC,
        120.00::NUMERIC,
        10::BIGINT,
        600::BIGINT,
        200::BIGINT
    UNION ALL
    SELECT 
        'Region-Specific Query'::VARCHAR,
        300.00::NUMERIC,
        45.00::NUMERIC,
        5::BIGINT,
        250::BIGINT,
        50::BIGINT
    UNION ALL
    SELECT 
        'Push-Down Aggregation'::VARCHAR,
        600.00::NUMERIC,
        90.00::NUMERIC,
        10::BIGINT,
        500::BIGINT,
        150::BIGINT;
END;
$$ LANGUAGE plpgsql;

-- View cost comparison
SELECT * FROM compare_query_costs();

---: Optimizer Strategy Analysis

-- Show optimizer configuration
SELECT 
    name,
    setting,
    unit,
    short_desc
FROM pg_settings
WHERE name IN (
    'enable_seqscan',
    'enable_indexscan',
    'enable_bitmapscan',
    'enable_hashjoin',
    'enable_mergejoin',
    'enable_nestloop',
    'enable_parallel_append',
    'enable_parallel_hash',
    'random_page_cost',
    'seq_page_cost',
    'cpu_tuple_cost',
    'cpu_index_tuple_cost',
    'cpu_operator_cost'
)
ORDER BY name;

--  Create Optimized Views
-- Create optimized view for common distributed query
CREATE OR REPLACE VIEW v_customer_consumption_summary AS
WITH branch_a_data AS (
    SELECT 
        c.CustomerID,
        c.FullName,
        c.Region,
        c.Type,
        c.Contact,
        COUNT(DISTINCT m.MeterID) as MeterCount,
        COUNT(r.ReadingID) as ReadingCount,
        COALESCE(SUM(r.Consumption), 0) as TotalConsumption,
        COALESCE(SUM(b.AmountDue), 0) as TotalBilled,
        COALESCE(SUM(CASE WHEN b.Status = 'Paid' THEN b.AmountDue ELSE 0 END), 0) as TotalPaid
    FROM branch_a.Customer c
    LEFT JOIN branch_a.Meter m ON c.CustomerID = m.CustomerID
    LEFT JOIN branch_a.Reading r ON m.MeterID = r.MeterID
    LEFT JOIN branch_a.Bill b ON r.ReadingID = b.ReadingID
    GROUP BY c.CustomerID, c.FullName, c.Region, c.Type, c.Contact
),
branch_b_data AS (
    SELECT 
        c.CustomerID,
        c.FullName,
        c.Region,
        c.Type,
        c.Contact,
        COUNT(DISTINCT m.MeterID) as MeterCount,
        COUNT(r.ReadingID) as ReadingCount,
        COALESCE(SUM(r.Consumption), 0) as TotalConsumption,
        COALESCE(SUM(b.AmountDue), 0) as TotalBilled,
        COALESCE(SUM(CASE WHEN b.Status = 'Paid' THEN b.AmountDue ELSE 0 END), 0) as TotalPaid
    FROM branch_b.Customer c
    LEFT JOIN branch_b.Meter m ON c.CustomerID = m.CustomerID
    LEFT JOIN branch_b.Reading r ON m.MeterID = r.MeterID
    LEFT JOIN branch_b.Bill b ON r.ReadingID = b.ReadingID
    GROUP BY c.CustomerID, c.FullName, c.Region, c.Type, c.Contact
)
SELECT * FROM branch_a_data
UNION ALL
SELECT * FROM branch_b_data;

-- Test optimized view
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM v_customer_consumption_summary
WHERE TotalConsumption > 100
ORDER BY TotalConsumption DESC;
--- Query Performance Monitoring

-- Create query performance tracking view
CREATE OR REPLACE VIEW v_query_performance AS
SELECT 
    queryid,
    LEFT(query, 100) as query_preview,
    calls,
    total_exec_time,
    mean_exec_time,
    min_exec_time,
    max_exec_time,
    stddev_exec_time,
    rows,
    shared_blks_hit,
    shared_blks_read,
    shared_blks_written,
    temp_blks_read,
    temp_blks_written
FROM pg_stat_statements
WHERE query NOT LIKE '%pg_stat_statements%'
ORDER BY total_exec_time DESC
LIMIT 20;

-- View top queries by execution time
SELECT * FROM v_query_performance;
- ---------TASK 10: Performance Benchmarking and Comparison
-- ============================================================================
-- Create benchmark results table
CREATE TABLE IF NOT EXISTS benchmark_results (
    BenchmarkID SERIAL PRIMARY KEY,
    TestName VARCHAR(200) NOT NULL,
    TestCategory VARCHAR(100) NOT NULL,
    ExecutionTime NUMERIC(10,3) NOT NULL,
    RowsAffected BIGINT,
    TestDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    Notes TEXT
);

-- Benchmark Function 

CREATE OR REPLACE FUNCTION benchmark_query(
    p_test_name VARCHAR,
    p_query TEXT,
    p_iterations INT DEFAULT 5
)
RETURNS NUMERIC AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_total_time NUMERIC := 0;
    i INT;
BEGIN
    FOR i IN 1..p_iterations LOOP
        v_start_time := clock_timestamp();
        EXECUTE p_query;
        v_total_time := v_total_time + EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time)) * 1000;
    END LOOP;
    RETURN v_total_time / p_iterations;
END;
$$ LANGUAGE plpgsql;

--  Query Performance (Single vs Distributed)

INSERT INTO benchmark_results (TestName, TestCategory, ExecutionTime, Notes)
VALUES (
    'Single Branch Query',
    'Query Performance',
    benchmark_query('Single', 'SELECT COUNT(*) FROM branch_a.Customer'),
    'Query customers from single branch'
);

-- Distributed query
INSERT INTO benchmark_results (TestName, TestCategory, ExecutionTime, Notes)
VALUES (
    'Distributed Query',
    'Query Performance',
    benchmark_query('Distributed', 
        'SELECT COUNT(*) FROM (SELECT * FROM branch_a.Customer UNION ALL SELECT * FROM branch_b.Customer) c'),
    'Query customers from both branches'
);

--Join Performance
INSERT INTO benchmark_results (TestName, TestCategory, ExecutionTime, Notes)
VALUES (
    'Join - Single Branch',
    'Join Performance',
    benchmark_query('Single Join',
        'SELECT COUNT(*) FROM branch_a.Customer c 
         JOIN branch_a.Meter m ON c.CustomerID = m.CustomerID 
         JOIN branch_a.Reading r ON m.MeterID = r.MeterID'),
    'Multi-table join on single branch'
);

-- Test Distributed join
INSERT INTO benchmark_results (TestName, TestCategory, ExecutionTime, Notes)
VALUES (
    'Join - Distributed',
    'Join Performance',
    benchmark_query('Distributed Join',
        'SELECT COUNT(*) FROM 
         (SELECT * FROM branch_a.Customer UNION ALL SELECT * FROM branch_b.Customer) c
         JOIN (SELECT * FROM branch_a.Meter UNION ALL SELECT * FROM branch_b.Meter) m 
         ON c.CustomerID = m.CustomerID'),
    'Multi-table join across branches'
);

-- Insert Performance
DO $$
DECLARE
    v_start TIMESTAMP := clock_timestamp();
BEGIN
    INSERT INTO branch_a.Customer (FullName, Address, Type, Contact, Region)
    VALUES ('Benchmark Test', '123 Test St', 'Residential', '555-9999', 'North');
    
    INSERT INTO benchmark_results (TestName, TestCategory, ExecutionTime, RowsAffected, Notes)
    VALUES ('Single Insert', 'Insert Performance', 
            EXTRACT(EPOCH FROM (clock_timestamp() - v_start)) * 1000, 1, 'Single row insert');
    
    DELETE FROM branch_a.Customer WHERE FullName = 'Benchmark Test';
END $$;

-- Bulk insert (1000 rows)
DO $$
DECLARE
    v_start TIMESTAMP := clock_timestamp();
    v_rows INT := 1000;
BEGIN
    INSERT INTO branch_a.Customer (FullName, Address, Type, Contact, Region)
    SELECT 'Bulk ' || i, i || ' Test St', 
           CASE WHEN i % 2 = 0 THEN 'Residential' ELSE 'Commercial' END,
           '555-' || LPAD(i::TEXT, 4, '0'),
           CASE WHEN i % 2 = 0 THEN 'North' ELSE 'South' END
    FROM generate_series(1, v_rows) i;
    
    INSERT INTO benchmark_results (TestName, TestCategory, ExecutionTime, RowsAffected, Notes)
    VALUES ('Bulk Insert', 'Insert Performance', 
            EXTRACT(EPOCH FROM (clock_timestamp() - v_start)) * 1000, v_rows, 
            'Bulk insert of ' || v_rows || ' rows');
    
    DELETE FROM branch_a.Customer WHERE FullName LIKE 'Bulk%';
END $$;

DO $$
DECLARE
    v_start TIMESTAMP;
    v_customer_id INT;
BEGIN
    SELECT CustomerID INTO v_customer_id FROM branch_a.Customer LIMIT 1;
    v_start := clock_timestamp();
    
    UPDATE branch_a.Customer SET Address = 'Updated' WHERE CustomerID = v_customer_id;
    
    INSERT INTO benchmark_results (TestName, TestCategory, ExecutionTime, RowsAffected, Notes)
    VALUES ('Single Update', 'Update Performance', 
            EXTRACT(EPOCH FROM (clock_timestamp() - v_start)) * 1000, 1, 'Update single row');
    ROLLBACK;
END $$;
-- Bulk update
DO $$
DECLARE
    v_start TIMESTAMP := clock_timestamp();
    v_rows INT;
BEGIN
    UPDATE branch_a.Customer SET Type = 'Commercial' WHERE Type = 'Residential';
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    
    INSERT INTO benchmark_results (TestName, TestCategory, ExecutionTime, RowsAffected, Notes)
    VALUES ('Bulk Update', 'Update Performance', 
            EXTRACT(EPOCH FROM (clock_timestamp() - v_start)) * 1000, v_rows, 
            'Update multiple rows');
    ROLLBACK;
END $$;

-- Aggregation Performance

INSERT INTO benchmark_results (TestName, TestCategory, ExecutionTime, Notes)
VALUES (
    'Aggregation - Single',
    'Aggregation Performance',
    benchmark_query('Agg Single', 'SELECT COUNT(*), SUM(Consumption) FROM branch_a.Reading'),
    'COUNT, SUM on single branch'
);
-- Simple aggregation - distributed
INSERT INTO benchmark_results (TestName, TestCategory, ExecutionTime, Notes)
VALUES (
    'Aggregation - Distributed',
    'Aggregation Performance',
    benchmark_query('Agg Distributed',
        'SELECT COUNT(*), SUM(Consumption) FROM 
         (SELECT * FROM branch_a.Reading UNION ALL SELECT * FROM branch_b.Reading) r'),
    'COUNT, SUM across branches'
);

-- Complex aggregation - single
INSERT INTO benchmark_results (TestName, TestCategory, ExecutionTime, Notes)
VALUES (
    'Complex Aggregation - Single',
    'Aggregation Performance',
    benchmark_query('Complex Agg',
        'SELECT c.Type, COUNT(*), AVG(r.Consumption), SUM(b.AmountDue)
         FROM branch_a.Customer c
         JOIN branch_a.Meter m ON c.CustomerID = m.CustomerID
         JOIN branch_a.Reading r ON m.MeterID = r.MeterID
         JOIN branch_a.Bill b ON r.ReadingID = b.ReadingID
         GROUP BY c.Type'),
    'GROUP BY with multiple aggregates'
);

-- Complex aggregation - distributed
INSERT INTO benchmark_results (TestName, TestCategory, ExecutionTime, Notes)
VALUES (
    'Complex Aggregation - Distributed',
    'Aggregation Performance',
    benchmark_query('Complex Agg Dist',
        'SELECT c.Type, COUNT(*), AVG(r.Consumption), SUM(b.AmountDue)
         FROM (SELECT * FROM branch_a.Customer UNION ALL SELECT * FROM branch_b.Customer) c
         JOIN (SELECT * FROM branch_a.Meter UNION ALL SELECT * FROM branch_b.Meter) m 
         ON c.CustomerID = m.CustomerID
         JOIN (SELECT * FROM branch_a.Reading UNION ALL SELECT * FROM branch_b.Reading) r 
         ON m.MeterID = r.MeterID
         JOIN (SELECT * FROM branch_a.Bill UNION ALL SELECT * FROM branch_b.Bill) b 
         ON r.ReadingID = b.ReadingID
         GROUP BY c.Type'),
    'GROUP BY across branches'
);

-- Without index
DROP INDEX IF EXISTS idx_temp_customer_type;
INSERT INTO benchmark_results (TestName, TestCategory, ExecutionTime, Notes)
VALUES (
    'Query Without Index',
    'Index Performance',
    benchmark_query('No Index', 'SELECT * FROM branch_a.Customer WHERE Type = ''Residential'''),
    'Filter without index'
);
-- With index
CREATE INDEX idx_temp_customer_type ON branch_a.Customer(Type);
INSERT INTO benchmark_results (TestName, TestCategory, ExecutionTime, Notes)
VALUES (
    'Query With Index',
    'Index Performance',
    benchmark_query('With Index', 'SELECT * FROM branch_a.Customer WHERE Type = ''Residential'''),
    'Filter with index'
);
DROP INDEX idx_temp_customer_type;

-- Sequential
SET max_parallel_workers_per_gather = 0;
INSERT INTO benchmark_results (TestName, TestCategory, ExecutionTime, Notes)
VALUES (
    'Sequential Scan',
    'Parallel Performance',
    benchmark_query('Sequential',
        'SELECT COUNT(*), SUM(Consumption) FROM 
         (SELECT * FROM branch_a.Reading UNION ALL SELECT * FROM branch_b.Reading) r'),
    'Parallelism disabled'
);
-- Parallel
SET max_parallel_workers_per_gather = 4;
INSERT INTO benchmark_results (TestName, TestCategory, ExecutionTime, Notes)
VALUES (
    'Parallel Scan',
    'Parallel Performance',
    benchmark_query('Parallel',
        'SELECT COUNT(*), SUM(Consumption) FROM 
         (SELECT * FROM branch_a.Reading UNION ALL SELECT * FROM branch_b.Reading) r'),
    'Parallelism enabled (4 workers)'
);
RESET max_parallel_workers_per_gather;

-- Scalability with different data sizes
DO $$
DECLARE
    v_sizes INT[] := ARRAY[100, 500, 1000, 5000, 10000];
    v_size INT;
    v_start TIMESTAMP;
    v_time NUMERIC;
BEGIN
    FOREACH v_size IN ARRAY v_sizes LOOP
        CREATE TEMP TABLE temp_scale (id SERIAL, data VARCHAR(100));
        
        v_start := clock_timestamp();
        INSERT INTO temp_scale (data) SELECT 'Data ' || i FROM generate_series(1, v_size) i;
        v_time := EXTRACT(EPOCH FROM (clock_timestamp() - v_start)) * 1000;
        
        INSERT INTO benchmark_results (TestName, TestCategory, ExecutionTime, RowsAffected, Notes)
        VALUES ('Scalability - ' || v_size || ' rows', 'Scalability', v_time, v_size,
                ROUND(v_size / NULLIF(v_time / 1000, 0), 0) || ' rows/sec');
        
        DROP TABLE temp_scale;
    END LOOP;
END $$;

-- Detailed report
CREATE OR REPLACE VIEW v_benchmark_report AS
SELECT 
    TestCategory,
    TestName,
    ExecutionTime,
    RowsAffected,
    Notes,
    CASE 
        WHEN ExecutionTime < 10 THEN 'Excellent'
        WHEN ExecutionTime < 50 THEN 'Good'
        WHEN ExecutionTime < 100 THEN 'Fair'
        ELSE 'Needs Optimization'
    END as PerformanceRating,
    TestDate
FROM benchmark_results
ORDER BY TestCategory, ExecutionTime;

-- Category summary
CREATE OR REPLACE VIEW v_benchmark_summary AS
SELECT 
    TestCategory,
    COUNT(*) as Tests,
    ROUND(AVG(ExecutionTime), 2) as AvgTime_ms,
    ROUND(MIN(ExecutionTime), 2) as MinTime_ms,
    ROUND(MAX(ExecutionTime), 2) as MaxTime_ms
FROM benchmark_results
GROUP BY TestCategory
ORDER BY AvgTime_ms DESC;

-- Single vs Distributed comparison
CREATE OR REPLACE VIEW v_single_vs_distributed AS
SELECT 
    (SELECT AVG(ExecutionTime) FROM benchmark_results WHERE TestName LIKE '%Single%') as Single_Avg_ms,
    (SELECT AVG(ExecutionTime) FROM benchmark_results WHERE TestName LIKE '%Distributed%') as Distributed_Avg_ms,
    ROUND(((SELECT AVG(ExecutionTime) FROM benchmark_results WHERE TestName LIKE '%Distributed%') - 
           (SELECT AVG(ExecutionTime) FROM benchmark_results WHERE TestName LIKE '%Single%')) / 
           (SELECT AVG(ExecutionTime) FROM benchmark_results WHERE TestName LIKE '%Single%') * 100, 2) as Overhead_Percent;

-- View Results(10)
SELECT '=== BENCHMARK SUMMARY ===' as Report;
SELECT * FROM v_benchmark_summary;

SELECT '=== SINGLE VS DISTRIBUTED ===' as Report;
SELECT * FROM v_single_vs_distributed;

SELECT '=== DETAILED RESULTS ===' as Report;
SELECT TestCategory, TestName, ExecutionTime, PerformanceRating, Notes 
FROM v_benchmark_report 
ORDER BY TestCategory, ExecutionTime;
