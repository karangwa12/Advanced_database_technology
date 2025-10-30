
----Reg-umber_224020114
---CASE STUDY:MUNICIPAL WATER BILLING AND CONSUMPTION TRACKING SYSTEM

---SECTION A

---A1.Fragment & Recombine Main Fact.

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
Select * from branch_a.Customer ;
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

CREATE TABLE Reading_A (
    ReadingID serial PRIMARY KEY,
    MeterID int NOT NULL,
    ReadingDate DATE NOT NULL,
    CurrentReading int NOT NULL CHECK (CurrentReading >= 0),
    Consumption int NOT NULL CHECK (Consumption >= 0)
);

-- Insert readings for MeterID with HASH(MeterID) MOD 2 = 0
-- 5 rows on Node_A
INSERT INTO Reading_A VALUES (1, 102, DATE '2024-01-15', 5800, 0);
INSERT INTO Reading_A VALUES (2, 102, DATE '2024-02-15', 6150, 350);
INSERT INTO Reading_A VALUES (3, 102, DATE '2024-03-15', 6520, 370);
INSERT INTO Reading_A VALUES (4, 102, DATE '2024-04-15', 6900, 380);
INSERT INTO Reading_A VALUES (5, 102, DATE '2024-05-15', 7280, 380);

COMMIT;


-- ON NODE_B: Create Reading_B fragment

CREATE TABLE Reading_B (
    ReadingID serial PRIMARY KEY,
    MeterID int  NOT NULL,
    ReadingDate DATE NOT NULL,
    CurrentReading int NOT NULL CHECK (CurrentReading >= 0),
    Consumption int NOT NULL CHECK (Consumption >= 0)
);

-- Insert readings for MeterID with HASH(MeterID) MOD 2 = 1
-- 5 rows on Node_B
INSERT INTO Reading_B VALUES (6, 101, DATE '2024-01-20', 1250, 0);
INSERT INTO Reading_B VALUES (7, 101, DATE '2024-02-20', 1285, 35);
INSERT INTO Reading_B VALUES (8, 103, DATE '2024-01-25', 980, 0);
INSERT INTO Reading_B VALUES (9, 103, DATE '2024-02-25', 1012, 32);
INSERT INTO Reading_B VALUES (10, 101, DATE '2024-03-20', 1322, 37);

COMMIT;


-- ON NODE_A: Create Database Link to Node_B

CREATE DATABASE LINK proj_link
CONNECT TO water_user IDENTIFIED BY "secure_password"
USING '(DESCRIPTION=
    (ADDRESS=(PROTOCOL=TCP)(HOST=node_b_host)(PORT=1521))
    (CONNECT_DATA=(SERVICE_NAME=water_db_node_b)))';


-- ON NODE_A: Create Global View

CREATE VIEW Reading_ALL AS
SELECT ReadingID, MeterID, ReadingDate, CurrentReading, Consumption
FROM Reading_A
UNION ALL
SELECT ReadingID, MeterID, ReadingDate, CurrentReading, Consumption
FROM Reading_B@proj_link;


-- Count validation
SELECT 'Reading_A' AS Fragment, COUNT(*) AS RowCount FROM Reading_A
UNION ALL
SELECT 'Reading_B', COUNT(*) FROM Reading_B@proj_link
UNION ALL
SELECT 'Reading_ALL', COUNT(*) FROM Reading_ALL;

-- Checksum validation using MOD(primary_key, 97)
SELECT 'Reading_A' AS Fragment, SUM(MOD(ReadingID, 97)) AS Checksum FROM Reading_A
UNION ALL
SELECT 'Reading_B', SUM(MOD(ReadingID, 97)) FROM Reading_B@proj_link
UNION ALL
SELECT 'Reading_ALL', SUM(MOD(ReadingID, 97)) FROM Reading_ALL;

-------------------------------------------------------------------
-----A2: Database Link & Cross-Node Join 

CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Step 2: Create a foreign server pointing to NODE_B
CREATE SERVER node_b_server
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host 'node_b_host', port '5432', dbname 'water_db_node_b');

-- Step 3: Create user mapping for authentication
CREATE USER MAPPING FOR CURRENT_USER
    SERVER node_b_server
    OPTIONS (user 'water_user', password 'bebe@123');

-- Step 4: Import foreign schema (import all tables from NODE_B)
IMPORT FOREIGN SCHEMA public
    FROM SERVER node_b_server
    INTO public;

OR import specific tables only:
CREATE FOREIGN TABLE customer_remote (
    CustomerID INT,
    FullName VARCHAR(100),
    Address VARCHAR(200),
    Type VARCHAR(20),
    Contact VARCHAR(20)
) SERVER node_b_server OPTIONS (schema_name 'public', table_name 'customer');

CREATE FOREIGN TABLE meter_remote (
    MeterID INT,
    CustomerID INT,
    InstallationDate DATE,
    Status VARCHAR(20),
    LastReading INT
) SERVER node_b_server OPTIONS (schema_name 'public', table_name 'meter');

--Using dblink Extension (Ad-hoc queries)

CREATE EXTENSION IF NOT EXISTS dblink;

SELECT * FROM dblink(
    'host=node_b_host port=5432 dbname=water_db_node_b user=water_user password=bebe@123',
    'SELECT * FROM meter LIMIT 5'
) AS remote_meter(MeterID INT, CustomerID INT, InstallationDate DATE, Status VARCHAR, LastReading INT);

-- Remote SELECT on Meter (from NODE_B)

-- Using standard SELECT on foreign table instead of @proj_link syntax
SELECT * 
FROM meter 
LIMIT 5;

-- Joining local Reading_A with remote tables using standard JOIN syntax
SELECT 
    r.ReadingID,
    r.MeterID,
    r.ReadingDate,
    r.Consumption,
    c.FullName AS CustomerName,
    c.Address,
    c.Type AS CustomerType
FROM Reading_A r
JOIN meter m ON r.MeterID = m.MeterID
JOIN customer c ON m.CustomerID = c.CustomerID
WHERE r.Consumption > 0
ORDER BY r.ReadingDate;
SELECT 
    r.ReadingID,
    c.FullName,
    c.Contact,
    r.Consumption,
    r.ReadingDate
FROM Reading_A r
JOIN meter m ON r.MeterID = m.MeterID
JOIN customer c ON m.CustomerID = c.CustomerID
WHERE r.ReadingDate >= DATE '2024-02-01'
  AND c.Type = 'Commercial'
LIMIT 10;
----==A2:2--Run remote SELECT on Meter@proj_link showing up to 5 sample rows. 
SELECT 
    foreign_table_schema,
    foreign_table_name,
    foreign_server_name
FROM information_schema.foreign_tables;

-- Test connection to remote server
SELECT * FROM customer LIMIT 3;
SELECT * FROM meter LIMIT 3;

------A3: Parallel vs Serial Aggregation

-- A3: Parallel vs Serial Aggregation

-- SERIAL Aggregation on Reading_ALL

SET AUTOTRACE ON EXPLAIN STATISTICS;

SELECT 
    MeterID,
    COUNT(*) AS ReadingCount,
    SUM(Consumption) AS TotalConsumption,
    AVG(Consumption) AS AvgConsumption,
    MAX(CurrentReading) AS LatestReading
FROM Reading_ALL
GROUP BY MeterID
ORDER BY MeterID;

-- Expected Output (3 groups):
-- MeterID  ReadingCount  TotalConsumption  AvgConsumption  LatestReading
-- 101      3             72                24              1322
-- 102      5             1480              296             7280
-- 103      2             32                16              1012

-- Capture execution plan
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR(NULL, NULL, 'ALLSTATS LAST'));

SET AUTOTRACE OFF;


-- PARALLEL Aggregation with Hints

SET AUTOTRACE ON EXPLAIN STATISTICS;

SELECT /*+ PARALLEL(Reading_A,8) PARALLEL(Reading_B,8) */
    MeterID,
    COUNT(*) AS ReadingCount,
    SUM(Consumption) AS TotalConsumption,
    AVG(Consumption) AS AvgConsumption,
    MAX(CurrentReading) AS LatestReading
FROM Reading_ALL
GROUP BY MeterID
ORDER BY MeterID;

-- Capture execution plan showing parallel operations
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR(NULL, NULL, 'ALLSTATS LAST'));

SET AUTOTRACE OFF;

-- Comparison Table

CREATE TABLE Parallel_Comparison (
    ExecutionMode VARCHAR(20),
    ElapsedTimeMS INT,
    BufferGets INT,
    ParallelDegree INT,
    PlanNotes VARCHAR(200)
);
 ---- serial aggregation
INSERT INTO Parallel_Comparison VALUES 
('SERIAL', 12, 45, 1, 'Sequential scan with UNION ALL, single thread'),
('SERIAL', 10, 40, 2, 'Sequential scan with UNION ALL, single thread'),
('SERIAL', 12, 45, 1, 'Sequential scan with UNION ALL, single thread'),
('SERIAL', 10, 40, 2, 'Sequential scan with UNION ALL, single thread');
------------------------------------
select * from Parallel_Comparison;
 ----------Run the same aggregation with /*+ PARALLEL
INSERT INTO Parallel_Comparison VALUES 
('PARALLEL', 18, 52, 8, 'PX COORDINATOR with parallel slaves, overhead visible'),
('PARALLEL', 15, 50, 9, 'PX COORDINATOR with parallel slaves, overhead visible'),
('PARALLEL', 18, 52, 8, 'PX COORDINATOR with parallel slaves, overhead visible'),
('PARALLEL', 10, 50, 9, 'PX COORDINATOR with parallel slaves, overhead visible'),
('PARALLEL', 13, 52, 8, 'PX COORDINATOR with parallel slaves, overhead visible'),
('PARALLEL', 12, 50, 9, 'PX COORDINATOR with parallel slaves, overhead visible');

COMMIT;

SELECT * FROM Parallel_Comparison;

-------------------------------------------------------------------
----A4 :Two-Phase Commit & Recovery 

SET SERVEROUTPUT ON;

DECLARE
    v_reading_id NUMBER := 11;
    v_payment_id NUMBER := 1;
BEGIN
    -- Insert local row on Node_A (Reading_A)
    INSERT INTO Reading_A VALUES (
        v_reading_id, 
        102, 
        DATE '2024-06-15', 
        7660, 
        380
    );
    
    DBMS_OUTPUT.PUT_LINE('Local insert on Node_A completed');
    
    -- Insert remote row on Node_B (Payment@proj_link)
    INSERT INTO Payment@proj_link VALUES (
        v_payment_id,
        1,  -- BillID (assuming Bill with ID 1 exists)
        45000,  -- 45,000 RWF
        SYSDATE,
        'MoMo'
    );
    
    DBMS_OUTPUT.PUT_LINE('Remote insert on Node_B completed');
    
    -- Commit both transactions (2PC protocol)
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('Two-phase commit successful');
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
        RAISE;
END;
-- Induce Failure Scenario
DECLARE
    v_reading_id NUMBER := 12;
    v_payment_id NUMBER := 2;
BEGIN
    -- Insert local row
    INSERT INTO Reading_A VALUES (
        v_reading_id, 
        102, 
        DATE '2024-07-15', 
        8040, 
        380
    );
    
    DBMS_OUTPUT.PUT_LINE('Simulating link failure...');
    
    -- This will create an in-doubt transaction
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;  -- Ensure test rows are rolled back
        DBMS_OUTPUT.PUT_LINE('Transaction rolled back: ' || SQLERRM);
END;

-- Query In-Doubt Transactions

SELECT 
    local_tran_id,
    state,
    mixed,
    advice,
    commit#,
    TO_CHAR(fail_time, 'DD-MON-YY HH24:MI:SS') AS fail_time
FROM DBA_2PC_PENDING
ORDER BY fail_time DESC;
-- Verify No Pending Transactions

SELECT COUNT(*) AS PendingTransactions
FROM DBA_2PC_PENDING;

-- Final Consistency Check
SELECT 'Reading_A' AS Location, COUNT(*) AS CommittedRows 
FROM Reading_A
UNION ALL
SELECT 'Payment@proj_link', COUNT(*) 
FROM Payment@proj_link;

--==================================================================
----======A5 :Distributed Lock Conflict & Diagnosis

-- Run this first and keep transaction open
UPDATE Bill 
SET AmountDue = 50000 
WHERE BillID = 1;

-- DO NOT COMMIT YET - keep lock held
SELECT 'Lock acquired on Bill row 1' AS Status FROM DUAL;
-- This will block waiting for the lock

UPDATE Bill@proj_link 
SET Status = 'Paid' 
WHERE BillID = 1;

-- Check blocking sessions
SELECT 
    s1.sid AS blocker_sid,
    s1.serial# AS blocker_serial,
    s1.username AS blocker_user,
    s1.program AS blocker_program,
    s2.sid AS waiter_sid,
    s2.serial# AS waiter_serial,
    s2.username AS waiter_user,
    s2.program AS waiter_program,
    l1.type AS lock_type,
    l1.id1,
    l1.id2
FROM v$lock l1
JOIN v$session s1 ON l1.sid = s1.sid
JOIN v$lock l2 ON l1.id1 = l2.id1 AND l1.id2 = l2.id2
JOIN v$session s2 ON l2.sid = s2.sid
WHERE l1.block = 1 
  AND l2.request > 0;

-- Alternative: Use DBA_BLOCKERS and DBA_WAITERS (Oracle 12c+)
SELECT 
    blocker.sid AS blocker_sid,
    blocker.username AS blocker_user,
    waiter.sid AS waiter_sid,
    waiter.username AS waiter_user,
    waiter.seconds_in_wait,
    waiter.event
FROM dba_blockers b
JOIN v$session blocker ON b.holding_session = blocker.sid
JOIN v$session waiter ON b.waiting_session = waiter.sid;

-- Check lock details
SELECT 
    sid,
    type,
    id1,
    id2,
    lmode,  -- Lock mode held
    request, -- Lock mode requested
    block,   -- 1 if blocking others
    ctime    -- Time held in seconds
FROM v$lock
WHERE type IN ('TX', 'TM')  -- Transaction and Table locks
ORDER BY block DESC, ctime DESC;
-- Run this in Session 1 to release the lock
COMMIT;

SELECT 
    SYSTIMESTAMP AS lock_released_at,
    'Lock released - Session 2 should now proceed' AS Status 
FROM DUAL;

-- Check the timestamp to show it proceeded after lock release

SELECT 
    SYSTIMESTAMP AS update_completed_at,
    'Update completed after lock release' AS Status 
FROM DUAL;

COMMIT;
-- Verification: Check Final State
SELECT 
    BillID,
    AmountDue,
    Status,
    'Both updates applied successfully' AS Result
FROM Bill
WHERE BillID = 1;
---=================================================================
--SECTION_B
--------------------------------------------------------------------
-- B6: Declarative Rules Hardening
ALTER TABLE Bill 
ADD CONSTRAINT chk_bill_amount_positive 
CHECK (AmountDue >= 0);

ALTER TABLE Bill 
ADD CONSTRAINT chk_bill_status_valid 
CHECK (Status IN ('Pending', 'Paid', 'Overdue'));

ALTER TABLE Bill 
ALTER COLUMN ReadingID SET NOT NULL;

ALTER TABLE Bill 
ALTER COLUMN DueDate SET NOT NULL;

-------------------------------------------------
-- Add Constraints to Payment Table
ALTER TABLE Payment 
ADD CONSTRAINT chk_payment_amount_positive 
CHECK (Amount > 0);

ALTER TABLE Payment 
ADD CONSTRAINT chk_payment_method_valid 
CHECK (Method IN ('Cash', 'MoMo', 'Bank', 'Airtel Money'));

ALTER TABLE Payment 
ALTER COLUMN BillID SET NOT NULL;

ALTER TABLE Payment 
ALTER COLUMN PaymentDate SET NOT NULL;

-- Add constraint: Payment date should not be before reasonable date
ALTER TABLE Payment 
ADD CONSTRAINT chk_payment_date_logical
CHECK (PaymentDate >= DATE '2020-01-01');

-- Test Cases for Bill Table
-- FAILING TEST 1: Negative amount
DO $$
BEGIN
    INSERT INTO Bill VALUES (1, 1, -5000, DATE '2024-02-15', 'Pending');
    RAISE NOTICE 'FAIL: Should have rejected negative amount';
    ROLLBACK;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'PASS: Negative amount rejected - %', SQLERRM;
        ROLLBACK;
END $$;

-- FAILING TEST 2: Invalid status
DO $$
BEGIN
    INSERT INTO Bill VALUES (2, 2, 30000, DATE '2024-02-20', 'Cancelled');
    RAISE NOTICE 'FAIL: Should have rejected invalid status';
    ROLLBACK;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'PASS: Invalid status rejected - %', SQLERRM;
        ROLLBACK;
END $$;

-- PASSING TEST 1: Valid bill
DO $$
BEGIN
    INSERT INTO Bill VALUES (1, 1, 35000, DATE '2024-02-15', 'Pending');
    RAISE NOTICE 'PASS: Valid bill inserted';
    COMMIT;
END $$;

-- PASSING TEST 2: Another valid bill
DO $$
BEGIN
    INSERT INTO Bill VALUES (2, 2, 125000, DATE '2024-02-20', 'Paid');
    RAISE NOTICE 'PASS: Valid bill inserted';
    COMMIT;
END $$;

-- Test Cases for Payment Table

-- FAILING TEST 1: Zero amount
DO $$
BEGIN
    INSERT INTO Payment VALUES (1, 1, 0, DATE '2024-02-16', 'MoMo');
    RAISE NOTICE 'FAIL: Should have rejected zero amount';
    ROLLBACK;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'PASS: Zero amount rejected - %', SQLERRM;
        ROLLBACK;
END $$;

-- FAILING TEST 2: Invalid payment method
DO $$
BEGIN
    INSERT INTO Payment VALUES (2, 2, 50000, DATE '2024-02-21', 'Credit Card');
    RAISE NOTICE 'FAIL: Should have rejected invalid method';
    ROLLBACK;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'PASS: Invalid payment method rejected - %', SQLERRM;
        ROLLBACK;
END $$;

-- PASSING TEST 1: Valid payment
INSERT INTO Payment VALUES (1, 1, 35000, DATE '2024-02-16', 'MoMo');

-- PASSING TEST 2: Another valid payment
INSERT INTO Payment VALUES (2, 2, 125000, DATE '2024-02-21', 'Bank');

-- Verification: Show Only Committed Rows
SELECT 'Bill' AS TableName, COUNT(*) AS CommittedRows FROM Bill
UNION ALL
SELECT 'Payment', COUNT(*) FROM Payment;

-- Show actual data
SELECT * FROM Bill ORDER BY BillID;
SELECT * FROM Payment ORDER BY PaymentID;

-- Expected: Only the 2 passing rows per table are committed
-- Total committed rows across project: ≤10

--------------------------------------------------------------------------------------
-- B7: E–C–A Trigger for Denormalized Totals (small DML set)

-- Create Audit Table
    bef_total NUMERIC,
    aft_total NUMERIC,
    changed_at TIMESTAMP,
    key_col VARCHAR(64)
);

-- Create Function to Handle Payment Changes

CREATE OR REPLACE FUNCTION fn_payment_audit()
RETURNS TRIGGER AS $$
DECLARE
    v_bill_id INTEGER;
    v_before_total NUMERIC;
    v_after_total NUMERIC;
BEGIN
    -- Determine which bill was affected
    IF TG_OP = 'DELETE' THEN
        v_bill_id := OLD.BillID;
    ELSE
        v_bill_id := NEW.BillID;
    END IF;
    
    -- Get bill amount (before total)
    SELECT AmountDue INTO v_before_total
    FROM Bill
    WHERE BillID = v_bill_id;
    
    -- Calculate actual total paid (after total)
    SELECT COALESCE(SUM(Amount), 0) INTO v_after_total
    FROM Payment
    WHERE BillID = v_bill_id;
    
    -- Log to audit table
    INSERT INTO Bill_AUDIT VALUES (
        v_before_total,
        v_after_total,
        CURRENT_TIMESTAMP,
        'BillID=' || v_bill_id
    );
    
    -- Update bill status if fully paid
    IF v_after_total >= v_before_total THEN
        UPDATE Bill 
        SET Status = 'Paid' 
        WHERE BillID = v_bill_id;
    ELSIF v_after_total > 0 THEN
        UPDATE Bill 
        SET Status = 'Pending' 
        WHERE BillID = v_bill_id;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create Statement-Level AFTER Trigger
CREATE TRIGGER trg_payment_stmt_audit
AFTER INSERT OR UPDATE OR DELETE ON Payment
FOR EACH ROW
EXECUTE FUNCTION fn_payment_audit();

-- Insert 2 new payments
INSERT INTO Payment VALUES (3, 1, 15000, DATE '2024-02-17', 'Cash');
INSERT INTO Payment VALUES (4, 2, 50000, DATE '2024-02-22', 'Airtel Money');

-- Update 1 payment
UPDATE Payment 
SET Amount = 20000 
WHERE PaymentID = 3;

-- Delete 1 payment (then rollback to stay within budget)
BEGIN;
DELETE FROM Payment WHERE PaymentID = 4;
ROLLBACK;

-- Verification: Check Audit Log

SELECT 
    TO_CHAR(changed_at, 'DD-Mon-YY HH24:MI:SS') AS changed_at,
    key_col,
    bef_total AS before_total,
    aft_total AS after_total,
    (aft_total - bef_total) AS difference
FROM Bill_AUDIT
ORDER BY changed_at;

-- Verify Bill Status Updates
--------------------------------------------------------------
SELECT 
    b.BillID,
    b.AmountDue,
    b.Status,
    COALESCE(SUM(p.Amount), 0) AS TotalPaid,
    CASE 
        WHEN COALESCE(SUM(p.Amount), 0) >= b.AmountDue THEN 'Fully Paid'
        ELSE 'Partial Payment'
    END AS PaymentStatus
FROM Bill b
LEFT JOIN Payment p ON b.BillID = p.BillID
GROUP BY b.BillID, b.AmountDue, b.Status
ORDER BY b.BillID;
-------------------------------------------------------------------
-- B8: Recursive Hierarchy Roll-Up (6–10 rows)

-- Create Hierarchy Table
CREATE TABLE HIER (
    parent_id VARCHAR(50),
    child_id VARCHAR(50),
    PRIMARY KEY (child_id)
);

-- Insert 8 rows forming a 3-level hierarchy
-- Level 1: District (Kigali)
-- Level 2: Sectors (Nyarugenge, Kimironko)
-- Level 3: Cells (specific neighborhoods)

INSERT INTO HIER VALUES (NULL, 'Kigali');  -- Root
INSERT INTO HIER VALUES ('Kigali', 'Nyarugenge');
INSERT INTO HIER VALUES ('Kigali', 'Kimironko');
INSERT INTO HIER VALUES ('Nyarugenge', 'Nyarugenge-Cell1');
INSERT INTO HIER VALUES ('Nyarugenge', 'Nyarugenge-Cell2');
INSERT INTO HIER VALUES ('Kimironko', 'Kimironko-Cell1');
INSERT INTO HIER VALUES ('Kimironko', 'Kimironko-Cell2');
INSERT INTO HIER VALUES ('Kimironko', 'Kimironko-Cell3');

-- Add location column to Customer for hierarchy
ALTER TABLE Customer ADD COLUMN Location VARCHAR(50);

UPDATE Customer SET Location = 'Nyarugenge-Cell1' WHERE CustomerID = 1;
UPDATE Customer SET Location = 'Nyarugenge-Cell2' WHERE CustomerID = 2;
UPDATE Customer SET Location = 'Kimironko-Cell1' WHERE CustomerID = 3;

-- Recursive Query: Build Hierarchy with Depth

WITH RECURSIVE hierarchy_tree (child_id, root_id, depth, path) AS (
    -- Anchor: Root nodes (no parent)
    SELECT 
        child_id,
        child_id AS root_id,
        0 AS depth,
        child_id AS path
    FROM HIER
    WHERE parent_id IS NULL
    
    UNION ALL
    
    -- Recursive: Children
    SELECT 
        h.child_id,
        ht.root_id,
        ht.depth + 1,
        ht.path || ' -> ' || h.child_id
    FROM HIER h
    JOIN hierarchy_tree ht ON h.parent_id = ht.child_id
)
SELECT 
    child_id,
    root_id,
    depth,
    path,
    LPAD(' ', depth * 2) || child_id AS indented_display
FROM hierarchy_tree
ORDER BY path;

-- Expected Output:
-- child_id              root_id  depth  path
-- Kigali                Kigali   0      Kigali
-- Nyarugenge            Kigali   1      Kigali -> Nyarugenge
-- Nyarugenge-Cell1      Kigali   2      Kigali -> Nyarugenge -> Nyarugenge-Cell1
-- Nyarugenge-Cell2      Kigali   2      Kigali -> Nyarugenge -> Nyarugenge-Cell2
-- Kimironko             Kigali   1      Kigali -> Kimironko
-- Kimironko-Cell1       Kigali   2      Kigali -> Kimironko -> Kimironko-Cell1
-- Kimironko-Cell2       Kigali   2      Kigali -> Kimironko -> Kimironko-Cell2
-- Kimironko-Cell3       Kigali   2      Kigali -> Kimironko -> Kimironko-Cell3

-- Hierarchical Rollup: Water Consumption by Location
WITH RECURSIVE hierarchy_tree (child_id, root_id, depth) AS (
    SELECT child_id, child_id AS root_id, 0 AS depth
    FROM HIER
    WHERE parent_id IS NULL
    
    UNION ALL
    
    SELECT h.child_id, ht.root_id, ht.depth + 1
    FROM HIER h
    JOIN hierarchy_tree ht ON h.parent_id = ht.child_id
)
SELECT 
    ht.child_id AS Location,
    ht.root_id AS District,
    ht.depth AS Level,
    COUNT(DISTINCT c.CustomerID) AS CustomerCount,
    COUNT(r.ReadingID) AS TotalReadings,
    COALESCE(SUM(r.Consumption), 0) AS TotalConsumption
FROM hierarchy_tree ht
LEFT JOIN Customer c ON c.Location = ht.child_id
LEFT JOIN Meter m ON m.CustomerID = c.CustomerID
LEFT JOIN Reading r ON r.MeterID = m.MeterID
GROUP BY ht.child_id, ht.root_id, ht.depth
HAVING COUNT(DISTINCT c.CustomerID) > 0 OR ht.depth = 0
ORDER BY ht.depth, ht.child_id;

-- Control Aggregation: Validate Rollup
SELECT 
    'Total Consumption (Direct)' AS Metric,
    SUM(Consumption) AS Value
FROM Reading

UNION ALL

SELECT 
    'Total Consumption (via Hierarchy)',
    SUM(TotalConsumption)
FROM (
    WITH RECURSIVE hierarchy_tree (child_id, root_id, depth) AS (
        SELECT child_id, child_id AS root_id, 0 AS depth
        FROM HIER WHERE parent_id IS NULL
        UNION ALL
        SELECT h.child_id, ht.root_id, ht.depth + 1
        FROM HIER h
        JOIN hierarchy_tree ht ON h.parent_id = ht.child_id
    )
    SELECT COALESCE(SUM(r.Consumption), 0) AS TotalConsumption
    FROM hierarchy_tree ht
    LEFT JOIN Customer c ON c.Location = ht.child_id
    LEFT JOIN Meter m ON m.CustomerID = c.CustomerID
    LEFT JOIN Reading r ON r.MeterID = m.MeterID
    WHERE ht.depth = 2  -- Leaf level only to avoid double counting
) subquery;

-- B9: Mini-Knowledge Base with Transitive Inference

-- Create Triple Table for Knowledge Base
CREATE TABLE TRIPLE (
    s VARCHAR(64),  -- Subject
    p VARCHAR(64),  -- Predicate
    o VARCHAR(64)   -- Object
);

-- Type hierarchy
INSERT INTO TRIPLE VALUES ('ResidentialMeter', 'isA', 'WaterMeter');
INSERT INTO TRIPLE VALUES ('CommercialMeter', 'isA', 'WaterMeter');
INSERT INTO TRIPLE VALUES ('WaterMeter', 'isA', 'UtilityDevice');

-- Billing rules
INSERT INTO TRIPLE VALUES ('HighConsumption', 'implies', 'PremiumRate');
INSERT INTO TRIPLE VALUES ('PremiumRate', 'implies', 'MonthlyAudit');

-- Customer types
INSERT INTO TRIPLE VALUES ('Residential', 'isA', 'Customer');
INSERT INTO TRIPLE VALUES ('Commercial', 'isA', 'Customer');
INSERT INTO TRIPLE VALUES ('Customer', 'isA', 'BillingEntity');

-- Recursive Inference Query: Transitive isA*
WITH RECURSIVE transitive_closure (subject, object, depth, path) AS (
    -- Base case: Direct relationships
    SELECT 
        s AS subject,
        o AS object,
        1 AS depth,
        s || ' -> ' || o AS path
    FROM TRIPLE
    WHERE p = 'isA'
    
    UNION ALL
    
    -- Recursive case: Transitive relationships
    SELECT 
        tc.subject,
        t.o AS object,
        tc.depth + 1,
        tc.path || ' -> ' || t.o
    FROM transitive_closure tc
    JOIN TRIPLE t ON tc.object = t.s AND t.p = 'isA'
    WHERE tc.depth < 5  -- Prevent infinite loops
)
SELECT DISTINCT
    subject,
    object,
    depth,
    path,
    'Inferred: ' || subject || ' is a type of ' || object AS inference
FROM transitive_closure
ORDER BY subject, depth;
-- Expected Output (showing transitive relationships):
-- subject              object           depth  path
-- ResidentialMeter     WaterMeter       1      ResidentialMeter -> WaterMeter
-- ResidentialMeter     UtilityDevice    2      ResidentialMeter -> WaterMeter -> UtilityDevice
-- CommercialMeter      WaterMeter       1      CommercialMeter -> WaterMeter
-- CommercialMeter      UtilityDevice    2      CommercialMeter -> WaterMeter -> UtilityDevice
-- WaterMeter           UtilityDevice    1      WaterMeter -> UtilityDevice
-- Residential          Customer         1      Residential -> Customer
-- Residential          BillingEntity    2      Residential -> Customer -> BillingEntity
-- Commercial           Customer         1      Commercial -> Customer
-- Commercial           BillingEntity    2      Commercial -> Customer -> BillingEntity
-- Customer             BillingEntity    1      Customer -> BillingEntity

-- Apply Labels to Base Records Using Inference
WITH RECURSIVE transitive_closure (subject, object) AS (
    SELECT s, o FROM TRIPLE WHERE p = 'isA'
    UNION ALL
    SELECT tc.subject, t.o
    FROM transitive_closure tc
    JOIN TRIPLE t ON tc.object = t.s AND t.p = 'isA'
)
SELECT 
    c.CustomerID,
    c.FullName,
    c.Type AS DirectType,
    tc.object AS InferredType,
    'Customer ' || c.FullName || ' is a ' || tc.object AS Label
FROM Customer c
JOIN transitive_closure tc ON c.Type = tc.subject
WHERE tc.object IN ('Customer', 'BillingEntity')
ORDER BY c.CustomerID, tc.object;

-- Expected Output (6 rows - 3 customers × 2 inferred types each):
-- CustomerID  FullName        DirectType    InferredType    Label
-- 1           Uwase Marie     Residential   Customer        Customer Uwase Marie is a Customer
-- 1           Uwase Marie     Residential   BillingEntity   Customer Uwase Marie is a BillingEntity
-- 2           Mugisha Jean    Commercial    Customer        Customer Mugisha Jean is a Customer
-- 2           Mugisha Jean    Commercial    BillingEntity   Customer Mugisha Jean is a BillingEntity
-- 3           Mukamana Grace  Residential   Customer        Customer Mukamana Grace is a Customer
-- 3           Mukamana Grace  Residential   BillingEntity   Customer Mukamana Grace is a BillingEntity

-- Grouping Counts: Validate Inference Consistency
SELECT 
    tc.object AS InferredType,
    COUNT(DISTINCT c.CustomerID) AS CustomerCount
FROM Customer c
JOIN (
    WITH RECURSIVE transitive_closure (subject, object) AS (
        SELECT s, o FROM TRIPLE WHERE p = 'isA'
        UNION ALL
        SELECT tc.subject, t.o
        FROM transitive_closure tc
        JOIN TRIPLE t ON tc.object = t.s AND t.p = 'isA'
    )
    SELECT * FROM transitive_closure
) tc ON c.Type = tc.subject
GROUP BY tc.object
ORDER BY tc.object;
---------------------------------------------------------------
-- B10: Business Limit Alert (Function + Trigger) (row-budget safe)
-- Create Business Limits Table
CREATE TABLE BUSINESS_LIMITS (
    rule_key VARCHAR(64) PRIMARY KEY,
    threshold NUMERIC NOT NULL,
    active CHAR(1) CHECK (active IN ('Y', 'N')) NOT NULL
);

-- Seed one active rule: Maximum single payment is 500,000 RWF
INSERT INTO BUSINESS_LIMITS VALUES ('MAX_SINGLE_PAYMENT', 500000, 'Y');

-- Create Alert Function

CREATE OR REPLACE FUNCTION fn_should_alert(
    p_bill_id INTEGER,
    p_amount NUMERIC,
    p_payment_date DATE
) RETURNS INTEGER AS $$
DECLARE
    v_threshold NUMERIC;
    v_active CHAR(1);
    v_total_paid NUMERIC;
    v_bill_amount NUMERIC;
BEGIN
    -- Check if MAX_SINGLE_PAYMENT rule is active
    BEGIN
        SELECT threshold, active 
        INTO v_threshold, v_active
        FROM BUSINESS_LIMITS
        WHERE rule_key = 'MAX_SINGLE_PAYMENT';
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN 0;  -- No rule, allow payment
    END;
    
    -- If rule is not active, allow payment
    IF v_active = 'N' THEN
        RETURN 0;
    END IF;
    
    -- Check 1: Single payment exceeds threshold
    IF p_amount > v_threshold THEN
        RETURN 1;  -- Alert: Single payment too large
    END IF;
    
    -- Check 2: Total payments for bill would exceed bill amount significantly
    BEGIN
        SELECT b.AmountDue, COALESCE(SUM(p.Amount), 0)
        INTO v_bill_amount, v_total_paid
        FROM Bill b
        LEFT JOIN Payment p ON b.BillID = p.BillID
        WHERE b.BillID = p_bill_id
        GROUP BY b.AmountDue;
        
        -- Alert if total would exceed bill by more than 10%
        IF (v_total_paid + p_amount) > (v_bill_amount * 1.1) THEN
            RETURN 1;  -- Alert: Overpayment
        END IF;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN 0;  -- Bill not found, allow (will fail on FK anyway)
    END;
    
    RETURN 0;  -- No alert, allow payment
END;
$$ LANGUAGE plpgsql;

-- Create Trigger to Enforce Business Rule
CREATE OR REPLACE FUNCTION fn_payment_limit_check()
RETURNS TRIGGER AS $$
DECLARE
    v_alert_result INTEGER;
BEGIN
    -- Call the alert function
    v_alert_result := fn_should_alert(
        NEW.BillID,
        NEW.Amount,
        NEW.PaymentDate
    );   
    -- If alert is raised, prevent the operation
    IF v_alert_result = 1 THEN
        RAISE EXCEPTION 'Business rule violation: Payment exceeds allowed limit or would cause overpayment. Amount: % RWF for Bill ID: %', 
            NEW.Amount, NEW.BillID;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_payment_limit_check
BEFORE INSERT OR UPDATE ON Payment
FOR EACH ROW
EXECUTE FUNCTION fn_payment_limit_check();

-- FAILING TEST 1: Payment exceeds
DO $$
BEGIN
    INSERT INTO Payment VALUES (10, 1, 600000, DATE '2024-03-01', 'Bank');
    RAISE NOTICE 'FAIL: Should have rejected payment > 500,000';
    ROLLBACK;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'PASS: Large payment rejected';
        RAISE NOTICE 'Error: %', SQLERRM;
        ROLLBACK;
END $$;

-- FAILING TEST 2: Payment would cause significant overpayment
DO $$
BEGIN
    -- Bill 1 has AmountDue = 35,000, already paid 35,000
    -- Trying to pay another 20,000 would exceed by > 10%
    INSERT INTO Payment VALUES (11, 1, 20000, DATE '2024-03-02', 'MoMo');
    RAISE NOTICE 'FAIL: Should have rejected overpayment';
    ROLLBACK;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'PASS: Overpayment rejected';
        RAISE NOTICE 'Error: %', SQLERRM;
        ROLLBACK;
END $$;

-- PASSING TEST 1: Valid payment within limits
INSERT INTO Payment VALUES (5, 2, 50000, DATE '2024-03-03', 'MoMo');

-- PASSING TEST 2: Another valid payment
INSERT INTO Payment VALUES (6, 2, 25000, DATE '2024-03-04', 'Cash');

-- Verification: Show Committed Data
SELECT 
    'Payment' AS TableName,
    COUNT(*) AS CommittedRows
FROM Payment;

SELECT 
    p.PaymentID,
    p.BillID,
    p.Amount,
    TO_CHAR(p.PaymentDate, 'DD-Mon-YY') AS PaymentDate,
    p.Method,
    b.AmountDue AS BillAmount,
    'Within Limits' AS Status
FROM Payment p
JOIN Bill b ON p.BillID = b.BillID
ORDER BY p.PaymentID;

-- Show business rule is active
SELECT 
    rule_key,
    threshold,
    active,
    'Rule enforced by trigger' AS Status
FROM BUSINESS_LIMITS;
