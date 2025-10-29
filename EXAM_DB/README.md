
# Case study:Municipal Water Billing and Consumption Tracking System.
## PostgreSQL Implementation

This database system manages water utility operations,including customer management, meter readings, billing, payments, and maintenance tracking.
For both residential and business clients, it facilitates payment administration, water usage tracking, and automated billing.
## System Overview
### Core Tables
1. **Customer** - Water service customers (residential and commercial)
2. **Meter** - Water meters installed at customer locations
3. **Reading** - Meter readings and consumption data (horizontally fragmented)
4. **Bill** - Generated bills based on consumption
5. **Payment** - Payment records (Cash, MoMo, Bank, Airtel Money)
6. **Maintenance** - Meter maintenance and repair records

### Step 1: Create Databases

# Create two databases (simulating Node A and Node B)
postgres createdb water_db_node_a
postgres createdb water_db_node_b

# Create user
postgres psql -c "CREATE USER water_user WITH PASSWORD 'bebe@123';"

# Grant privileges
postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE water_db_node_a TO water_user;"
postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE water_db_node_b TO water_user;"
\`\`\`

### Step 2: Enable Extensions

\`\`\`
# Enable postgres_fdw on both databases
postgres psql water_db_node_a -c "CREATE EXTENSION postgres_fdw;"
postgres psql water_db_node_b -c "CREATE EXTENSION postgres_fdw;"
\`\`\`

### Step 3: Configure Connection
A database connection between my application and the PostgreSQL database.
For example:Host: localhost 
(localhost = my computer)
Port: 5432  (5432 is PostgreSQL default)
Database: water_billing_db  
Username: water_user    --my username
Password: bebe@123      -- mypassword

Edit `postgresql.conf`:
\`\`\`conf
listen_addresses = '*'
port = 5432
\`\`\`

Edit `pg_hba.conf`:
\`\`\`conf
host    all    all    0.0.0.0/0    md5
\`\`\`

Restart PostgreSQL:
\`\`\`
systemctl restart postgresql
\`\`\`

---

## Database Schema

### Entity Relationship Diagram

\`\`\`
Customer (1) ──── (N) Meter (1) ──── (N) Reading
                    │                      │
                    │                      │
                    │                      │
                (N) │                  (1) │
                    │                      │
              Maintenance              Bill (1) ──── (N) Payment
\`\`\`

### Table Structures

\`\`\`sql
-- Customer Table
CREATE TABLE Customer (
    CustomerID SERIAL PRIMARY KEY,
    FullName VARCHAR(100) NOT NULL,
    Address VARCHAR(200) NOT NULL,
    Type VARCHAR(20) CHECK (Type IN ('Residential', 'Commercial')),
    Contact VARCHAR(20)
);

-- Meter Table
CREATE TABLE Meter (
    MeterID SERIAL PRIMARY KEY,
    CustomerID INT REFERENCES Customer(CustomerID),
    InstallationDate DATE NOT NULL,
    Status VARCHAR(20) CHECK (Status IN ('Active', 'Inactive', 'Faulty')),
    LastReading NUMERIC(10,2) DEFAULT 0
);

-- Reading Table (Fragmented)
CREATE TABLE Reading (
    ReadingID SERIAL PRIMARY KEY,
    MeterID INT REFERENCES Meter(MeterID),
    ReadingDate DATE NOT NULL,
    CurrentReading NUMERIC(10,2) NOT NULL,
    Consumption NUMERIC(10,2) NOT NULL
);

-- Bill Table
CREATE TABLE Bill (
    BillID SERIAL PRIMARY KEY,
    ReadingID INT REFERENCES Reading(ReadingID),
    AmountDue NUMERIC(10,2) NOT NULL CHECK (AmountDue > 0),
    DueDate DATE NOT NULL,
    Status VARCHAR(20) CHECK (Status IN ('Pending', 'Paid', 'Overdue'))
);

-- Payment Table
CREATE TABLE Payment (
    PaymentID SERIAL PRIMARY KEY,
    BillID INT REFERENCES Bill(BillID),
    Amount NUMERIC(10,2) NOT NULL CHECK (Amount > 0),
    PaymentDate DATE NOT NULL,
    Method VARCHAR(30) CHECK (Method IN ('Cash', 'MoMo', 'Bank Transfer', 'Airtel Money'))
);

-- Maintenance Table
CREATE TABLE Maintenance (
    MaintID SERIAL PRIMARY KEY,
    MeterID INT REFERENCES Meter(MeterID),
    Issue TEXT NOT NULL,
    Technician VARCHAR(100),
    DateFixed DATE,
    Cost NUMERIC(10,2)
);
\`\`\`

---

## Task Implementations

### A1: Fragment & Recombine (Horizontal Fragmentation)

**Objective**: Implement horizontal fragmentation with UNION ALL view

**Implementation**:
\`\`\`sql
-- Node A: Create fragment for Commercial meters
CREATE TABLE Reading_A (
    ReadingID INT PRIMARY KEY,
    MeterID INT,
    ReadingDate DATE NOT NULL,
    CurrentReading NUMERIC(10,2) NOT NULL,
    Consumption NUMERIC(10,2) NOT NULL
);

-- Node B: Create fragment for Residential meters
CREATE TABLE Reading_B (
    ReadingID INT PRIMARY KEY,
    MeterID INT,
    ReadingDate DATE NOT NULL,
    CurrentReading NUMERIC(10,2) NOT NULL,
    Consumption NUMERIC(10,2) NOT NULL
);

-- Fragmentation Rule: HASH(MeterID) MOD 2
-- MeterID 102 (Commercial) → Node A (5 rows)
-- MeterID 101, 103 (Residential) → Node B (5 rows)

-- Global View on Node A
CREATE VIEW Reading_ALL AS
SELECT * FROM Reading_A
UNION ALL
SELECT * FROM reading_b_remote;  -- via FDW
\`\`\`

**Validation**:
\`\`\`sql
-- Row count check
SELECT COUNT(*) FROM Reading_ALL;  -- Expected: 10

-- Checksum validation
SELECT SUM(MOD(ReadingID, 97)) FROM Reading_ALL;  -- Expected: 460
\`\`\`

---

### A2: Database Link & Cross-Node Join

**Objective**: Create foreign data wrapper and perform distributed joins

**Implementation**:
\`\`\`sql
-- Create foreign server (Node A connecting to Node B)
CREATE SERVER node_b_server
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'localhost', port '5433', dbname 'water_db_node_b');

-- Create user mapping
CREATE USER MAPPING FOR water_user
SERVER node_b_server
OPTIONS (user 'water_user', password 'bebe@123');

-- Import foreign schema
IMPORT FOREIGN SCHEMA public
LIMIT TO (Meter, Customer)
FROM SERVER node_b_server
INTO public;

-- Distributed join query
SELECT 
    c.FullName,
    c.Address,
    m.Status AS MeterStatus,
    r.Consumption,
    r.ReadingDate
FROM Reading_A r
JOIN meter_remote m ON r.MeterID = m.MeterID
JOIN customer_remote c ON m.CustomerID = c.CustomerID
WHERE r.Consumption > 15
LIMIT 5;
\`\`\`

---

### A3: Parallel vs Serial Aggregation

**Objective**: Compare serial and parallel query execution

**Implementation**:
\`\`\`sql
-- Serial aggregation
SELECT 
    MeterID,
    COUNT(*) AS reading_count,
    SUM(Consumption) AS total_consumption,
    AVG(Consumption) AS avg_consumption
FROM Reading_ALL
GROUP BY MeterID
ORDER BY MeterID;

-- Parallel aggregation (PostgreSQL)
SET max_parallel_workers_per_gather = 4;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;

EXPLAIN (ANALYZE, BUFFERS)
SELECT 
    MeterID,
    COUNT(*) AS reading_count,
    SUM(Consumption) AS total_consumption,
    AVG(Consumption) AS avg_consumption
FROM Reading_ALL
GROUP BY MeterID
ORDER BY MeterID;
\`\`\`

---

### A4: Two-Phase Commit & Recovery

**Objective**: Demonstrate distributed transaction with 2PC

**Implementation**:
\`\`\`sql
-- Two-phase commit transaction
BEGIN;

-- Local insert on Node A
INSERT INTO Reading_A VALUES 
(11, 102, '2025-01-26', 1250.00, 25.00);

-- Remote insert on Node B (via FDW)
INSERT INTO payment_remote VALUES 
(7, 2, 45000, '2025-01-26', 'MoMo');

COMMIT;  -- 2PC automatically handled by postgres_fdw

-- Verify consistency
SELECT COUNT(*) FROM Reading_A WHERE ReadingID = 11;
SELECT COUNT(*) FROM payment_remote WHERE PaymentID = 7;
\`\`\`

**Failure Simulation**:
\`\`\`sql
-- Simulate failure (disable network between nodes)
BEGIN;
INSERT INTO Reading_A VALUES (12, 102, '2025-01-27', 1275.00, 25.00);
-- Network failure occurs here
INSERT INTO payment_remote VALUES (8, 2, 50000, '2025-01-27', 'Bank Transfer');
-- Transaction will fail and rollback
ROLLBACK;
\`\`\`

---

### A5: Distributed Lock Conflict & Diagnosis

**Objective**: Demonstrate and diagnose distributed locking

**Implementation**:
\`\`\`sql
-- Session 1 (Node A): Acquire lock
BEGIN;
UPDATE Bill SET AmountDue = 51000 WHERE BillID = 1;
-- Keep transaction open

-- Session 2 (Node B): Attempt to update same row
BEGIN;
UPDATE bill_remote SET AmountDue = 52000 WHERE BillID = 1;
-- This will block waiting for Session 1

-- Diagnose locks (Session 3)
SELECT 
    pid,
    usename,
    application_name,
    state,
    wait_event_type,
    wait_event,
    query
FROM pg_stat_activity
WHERE state = 'active' AND wait_event IS NOT NULL;

-- Release lock (Session 1)
COMMIT;

-- Session 2 now proceeds
COMMIT;
\`\`\`

---

### B6: Declarative Rules Hardening

**Objective**: Add and test CHECK constraints

**Implementation**:
\`\`\`sql
-- Add constraints to Bill
ALTER TABLE Bill
ADD CONSTRAINT chk_bill_amount_positive CHECK (AmountDue > 0),
ADD CONSTRAINT chk_bill_status CHECK (Status IN ('Pending', 'Paid', 'Overdue')),
ADD CONSTRAINT chk_bill_duedate_future CHECK (DueDate >= CURRENT_DATE - INTERVAL '1 year');

-- Add constraints to Payment
ALTER TABLE Payment
ADD CONSTRAINT chk_payment_amount_positive CHECK (Amount > 0),
ADD CONSTRAINT chk_payment_method CHECK (Method IN ('Cash', 'MoMo', 'Bank Transfer', 'Airtel Money')),
ADD CONSTRAINT chk_payment_date_valid CHECK (PaymentDate <= CURRENT_DATE);

-- Test failing cases (will be rolled back)
DO $$
BEGIN
    -- Failing test 1: Negative amount
    INSERT INTO Bill VALUES (999, 1, -5000, '2025-02-01', 'Pending');
EXCEPTION WHEN check_violation THEN
    RAISE NOTICE 'Test 1 Failed (Expected): Negative amount rejected';
END $$;

-- Test passing cases (will be committed)
INSERT INTO Bill VALUES (3, 3, 35000, '2025-02-15', 'Pending');
\`\`\`

---

### B7: E-C-A Trigger for Denormalized Totals

**Objective**: Create audit trigger for Payment changes

**Implementation**:
\`\`\`sql
-- Create audit table
CREATE TABLE Bill_AUDIT (
    audit_id SERIAL PRIMARY KEY,
    bill_id INT,
    bef_total NUMERIC(10,2),
    aft_total NUMERIC(10,2),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    operation VARCHAR(10)
);

-- Create trigger function
CREATE OR REPLACE FUNCTION audit_bill_totals()
RETURNS TRIGGER AS $$
DECLARE
    v_bill_id INT;
    v_bef_total NUMERIC(10,2);
    v_aft_total NUMERIC(10,2);
BEGIN
    -- Get affected bill
    IF TG_OP = 'DELETE' THEN
        v_bill_id := OLD.BillID;
    ELSE
        v_bill_id := NEW.BillID;
    END IF;
    
    -- Calculate before total
    SELECT COALESCE(SUM(Amount), 0) INTO v_bef_total
    FROM Payment WHERE BillID = v_bill_id;
    
    -- Adjust for current operation
    IF TG_OP = 'INSERT' THEN
        v_bef_total := v_bef_total - NEW.Amount;
    ELSIF TG_OP = 'DELETE' THEN
        v_bef_total := v_bef_total + OLD.Amount;
    END IF;
    
    -- Calculate after total
    SELECT COALESCE(SUM(Amount), 0) INTO v_aft_total
    FROM Payment WHERE BillID = v_bill_id;
    
    -- Insert audit record
    INSERT INTO Bill_AUDIT (bill_id, bef_total, aft_total, operation)
    VALUES (v_bill_id, v_bef_total, v_aft_total, TG_OP);
    
    -- Update bill status
    UPDATE Bill SET Status = 
        CASE 
            WHEN v_aft_total >= AmountDue THEN 'Paid'
            ELSE 'Pending'
        END
    WHERE BillID = v_bill_id;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger
CREATE TRIGGER trg_audit_payment
AFTER INSERT OR UPDATE OR DELETE ON Payment
FOR EACH ROW EXECUTE FUNCTION audit_bill_totals();
\`\`\`

---
### B8: Recursive Hierarchy Roll-Up

**Objective**: Implement recursive query for location hierarchy

**Implementation**:
\`\`\`sql
-- Create hierarchy table
CREATE TABLE HIER (
    parent_id VARCHAR(64),
    child_id VARCHAR(64),
    PRIMARY KEY (parent_id, child_id)
);

-- Insert 3-level hierarchy (District → Sector → Cell)
INSERT INTO HIER VALUES
('Rwanda', 'Kigali'),
('Kigali', 'Gasabo'),
('Kigali', 'Kicukiro'),
('Gasabo', 'Remera'),
('Gasabo', 'Kimironko'),
('Kicukiro', 'Gikondo'),
('Kicukiro', 'Niboye'),
('Remera', 'KG_001');

-- Recursive query with rollup
WITH RECURSIVE hierarchy_tree AS (
    -- Base case: leaf nodes
    SELECT 
        child_id,
        child_id AS root_id,
        0 AS depth
    FROM HIER
    WHERE child_id NOT IN (SELECT parent_id FROM HIER)
    
    UNION ALL
    
    -- Recursive case: traverse up
    SELECT 
        h.parent_id,
        ht.root_id,
        ht.depth + 1
    FROM HIER h
    JOIN hierarchy_tree ht ON h.child_id = ht.child_id
)
SELECT 
    ht.child_id AS location,
    ht.root_id AS leaf_location,
    ht.depth,
    COUNT(c.CustomerID) AS customer_count,
    SUM(r.Consumption) AS total_consumption
FROM hierarchy_tree ht
LEFT JOIN Customer c ON c.Address LIKE '%' || ht.child_id || '%'
LEFT JOIN Meter m ON c.CustomerID = m.CustomerID
LEFT JOIN Reading r ON m.MeterID = r.MeterID
GROUP BY ht.child_id, ht.root_id, ht.depth
ORDER BY ht.depth, ht.child_id;
\`\`\`

---

### B9: Mini-Knowledge Base with Transitive Inference

**Objective**: Implement transitive closure for type hierarchy

**Implementation**:
\`\`\`sql
-- Create triple table
CREATE TABLE TRIPLE (
    s VARCHAR(64),
    p VARCHAR(64),
    o VARCHAR(64),
    PRIMARY KEY (s, p, o)
);

-- Insert domain facts
INSERT INTO TRIPLE VALUES
('Residential', 'isA', 'Customer'),
('Commercial', 'isA', 'Customer'),
('HighConsumer', 'isA', 'Commercial'),
('LowConsumer', 'isA', 'Residential'),
('Customer', 'requires', 'Meter'),
('Meter', 'generates', 'Reading'),
('Reading', 'creates', 'Bill'),
('Bill', 'receives', 'Payment');

-- Recursive inference query
WITH RECURSIVE inference AS (
    -- Base facts
    SELECT s, p, o, 0 AS depth
    FROM TRIPLE
    WHERE p = 'isA'
    
    UNION
    
    -- Transitive closure
    SELECT t.s, 'isA' AS p, i.o, i.depth + 1
    FROM TRIPLE t
    JOIN inference i ON t.o = i.s
    WHERE t.p = 'isA' AND i.depth < 5
)
SELECT DISTINCT 
    c.CustomerID,
    c.FullName,
    c.Type AS declared_type,
    i.o AS inferred_type,
    i.depth AS inference_depth
FROM Customer c
LEFT JOIN inference i ON c.Type = i.s
ORDER BY c.CustomerID, i.depth;
\`\`\`

---

### B10: Business Limit Alert (Function + Trigger)

**Objective**: Enforce business rules with triggers

**Implementation**:
\`\`\`sql
-- Create business limits table
CREATE TABLE BUSINESS_LIMITS (
    rule_key VARCHAR(64) PRIMARY KEY,
    threshold NUMERIC(10,2) NOT NULL,
    active CHAR(1) CHECK (active IN ('Y', 'N')) DEFAULT 'Y'
);

-- Insert active rule
INSERT INTO BUSINESS_LIMITS VALUES 
('MAX_SINGLE_PAYMENT', 500000, 'Y');

-- Create alert function
CREATE OR REPLACE FUNCTION fn_should_alert(
    p_amount NUMERIC,
    p_bill_id INT
) RETURNS INT AS $$
DECLARE
    v_threshold NUMERIC(10,2);
    v_active CHAR(1);
BEGIN
    -- Get active limit
    SELECT threshold, active INTO v_threshold, v_active
    FROM BUSINESS_LIMITS
    WHERE rule_key = 'MAX_SINGLE_PAYMENT';
    
    -- Check if rule is active and violated
    IF v_active = 'Y' AND p_amount > v_threshold THEN
        RETURN 1;  -- Alert
    END IF;
    
    RETURN 0;  -- No alert
END;
$$ LANGUAGE plpgsql;

-- Create enforcement trigger
CREATE OR REPLACE FUNCTION enforce_payment_limit()
RETURNS TRIGGER AS $$
BEGIN
    IF fn_should_alert(NEW.Amount, NEW.BillID) = 1 THEN
        RAISE EXCEPTION 'Payment amount % exceeds maximum allowed limit of 500,000 RWF', NEW.Amount;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_check_payment_limit
BEFORE INSERT OR UPDATE ON Payment
FOR EACH ROW EXECUTE FUNCTION enforce_payment_limit();

-- Test failing case
DO $$
BEGIN
    INSERT INTO Payment VALUES (999, 1, 600000, '2025-01-22', 'Bank Transfer');
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Test Failed (Expected): Payment exceeds limit';
END $$;

-- Test passing case
INSERT INTO Payment VALUES (7, 2, 45000, '2025-01-22', 'MoMo');
\`\`\`

---

## Execution Guide

### Recommended Execution Order

\`\`\`
# 1. Create base tables and initial data
psql -U water_user -d water_db_node_a -f scripts/00-create-base-tables.sql

# 2. Distributed database tasks
psql -U water_user -d water_db_node_a -f scripts/A1-fragment-recombine.sql
psql -U water_user -d water_db_node_a -f scripts/A2-database-link-join.sql
psql -U water_user -d water_db_node_a -f scripts/A3-parallel-vs-serial.sql
psql -U water_user -d water_db_node_a -f scripts/A4-two-phase-commit.sql
psql -U water_user -d water_db_node_a -f scripts/A5-distributed-lock-conflict.sql

# 3. Advanced SQL tasks
psql -U water_user -d water_db_node_a -f scripts/B6-declarative-rules.sql
psql -U water_user -d water_db_node_a -f scripts/B7-eca-trigger-audit.sql
psql -U water_user -d water_db_node_a -f scripts/B8-recursive-hierarchy.sql
psql -U water_user -d water_db_node_a -f scripts/B9-knowledge-base-inference.sql
psql -U water_user -d water_db_node_a -f scripts/B10-business-limit-alert.sql

# 4. Verify final state
psql -U water_user -d water_db_node_a -f scripts/99-verification-summary.sql
\`\`\`

### Interactive Execution

\`\`\`
# Connect to database
psql -U water_user -d water_db_node_a

# Run individual queries
\i scripts/A1-fragment-recombine.sql

# Check results
SELECT * FROM Reading_ALL;

# Exit
\q
\`\`\`

---

## Validation & Testing

### Row Budget Verification

\`\`\`sql
-- Check total committed rows (should be ≤10)
SELECT 
    'Customer' AS table_name, COUNT(*) AS row_count FROM Customer
UNION ALL
SELECT 'Meter', COUNT(*) FROM Meter
UNION ALL
SELECT 'Reading_A', COUNT(*) FROM Reading_A
UNION ALL
SELECT 'Reading_B', COUNT(*) FROM reading_b_remote
UNION ALL
SELECT 'Bill', COUNT(*) FROM Bill
UNION ALL
SELECT 'Payment', COUNT(*) FROM Payment;

\`\`\`

### Data Integrity Checks

\`\`\`sql
-- Check referential integrity
SELECT m.MeterID, m.CustomerID
FROM Meter m
LEFT JOIN Customer c ON m.CustomerID = c.CustomerID
WHERE c.CustomerID IS NULL;  -- Should return 0 rows

-- Check constraint violations
SELECT * FROM Bill WHERE AmountDue <= 0;  -- Should return 0 rows
SELECT * FROM Payment WHERE Amount <= 0;  -- Should return 0 rows

-- Check fragmentation consistency
SELECT COUNT(*) FROM Reading_ALL;  -- Should equal sum of fragments
\`\`\`

### Performance Validation

\`\`\`sql
-- Check query performance
EXPLAIN ANALYZE
SELECT c.FullName, SUM(r.Consumption) AS total
FROM Customer c
JOIN Meter m ON c.CustomerID = m.CustomerID
JOIN Reading_ALL r ON m.MeterID = r.MeterID
GROUP BY c.FullName;
\`\`\`

---

## Troubleshooting

### Common Issues

#### 1. Connection Refused

**Problem**: Cannot connect to PostgreSQL
\`\`\`
psql: error: connection to server failed: Connection refused
\`\`\`

**Solution**:
\`\`\`
# Check if PostgreSQL is running
sudo systemctl status postgresql

# Start if not running
sudo systemctl start postgresql

# Check port
sudo netstat -plnt | grep postgres
\`\`\`

#### 2. Permission Denied

**Problem**: User lacks privileges
\`\`\`
ERROR: permission denied for table Customer
\`\`\`

**Solution**:
\`\`\`sql
-- Grant all privileges
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO water_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO water_user;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO water_user;
\`\`\`

#### 3. Foreign Data Wrapper Not Found

**Problem**: postgres_fdw extension missing
\`\`\`
ERROR: extension "postgres_fdw" does not exist
\`\`\`

**Solution**:
\`\`\`sql
-- Install extension
CREATE EXTENSION postgres_fdw;

-- Verify installation
SELECT * FROM pg_extension WHERE extname = 'postgres_fdw';
\`\`\`

#### 4. Database Link Connection Failed

**Problem**: Cannot connect to remote server
\`\`\`
ERROR: could not connect to server "node_b_server"
\`\`\`

**Solution**:
\`\`\`
# Check network connectivity
ping node_b_host

# Check PostgreSQL is listening
sudo netstat -plnt | grep 5432

# Update pg_hba.conf to allow remote connections
sudo nano /etc/postgresql/14/main/pg_hba.conf
# Add: host all all 0.0.0.0/0 md5

# Restart PostgreSQL
sudo systemctl restart postgresql
\`\`\`

#### 5. Constraint Violation

**Problem**: Data violates CHECK constraint
\`\`\`
ERROR: new row violates check constraint "chk_bill_amount_positive"
\`\`\`
\`\`\`sql
DO $$
BEGIN
    INSERT INTO Bill VALUES (999, 1, -5000, '2025-02-01', 'Pending');
EXCEPTION WHEN check_violation THEN
    RAISE NOTICE 'Constraint violation (expected)';
END $$;
\`\`\`

#### 6. Row Budget Exceeded

**Problem**: More than 10 committed rows
\`\`\`
Total committed rows: 15 (exceeds limit of 10)
\`\`\`

**Solution**:
\`\`\`sql
-- Delete excess rows
DELETE FROM Payment WHERE PaymentID > 6;
DELETE FROM Bill WHERE BillID > 2;

-- Verify count
SELECT SUM(cnt) FROM (
    SELECT COUNT(*) AS cnt FROM Customer
    UNION ALL SELECT COUNT(*) FROM Meter
    UNION ALL SELECT COUNT(*) FROM Reading_A
) AS counts;
\`\`\`

