------------Intelligence Databases-Assignment
--1.-----
Prerequisite table
CREATE TABLE PATIENT (
  ID SERIAL PRIMARY KEY,
  NAME VARCHAR(100) NOT NULL
);

-- Insert sample patients
INSERT INTO PATIENT (NAME) VALUES
('Alice Johnson'),
('Brian Smith'),
('Catherine Lee'),
('David Brown'),
('Emily Davis');

---------------------------------------------------
-- Corrected PATIENT_MED table
CREATE TABLE PATIENT_MED (
  PATIENT_MED_ID SERIAL PRIMARY KEY, -- unique id
  PATIENT_ID SERIAL NOT NULL REFERENCES PATIENT(ID), -- must reference an existing patient
  MED_NAME VARCHAR(80) NOT NULL, -- mandatory field
  DOSE_MG NUMERIC(6,2) CHECK (DOSE_MG >= 0), -- non-negative dose
  START_DT DATE,
  END_DT DATE,
  CONSTRAINT CK_RX_DATES CHECK (
    START_DT IS NULL OR END_DT IS NULL OR START_DT <= END_DT
  ) -- sensible date logic
);

---- inset data into PATIENT_MED

INSERT INTO PATIENT_MED (PATIENT_ID, MED_NAME, DOSE_MG, START_DT, END_DT)
VALUES
(1, 'Amoxicillin', 500.00, TO_DATE('2025-10-01','YYYY-MM-DD'), TO_DATE('2025-10-07','YYYY-MM-DD')),
(2, 'Ibuprofen', 200.00, TO_DATE('2025-10-10','YYYY-MM-DD'), TO_DATE('2025-10-15','YYYY-MM-DD')),
(3, 'Paracetamol', 650.00, TO_DATE('2025-09-25','YYYY-MM-DD'), TO_DATE('2025-09-30','YYYY-MM-DD')),
(4, 'Metformin', 850.00, TO_DATE('2025-08-01','YYYY-MM-DD'), TO_DATE('2025-12-31','YYYY-MM-DD')),
(5, 'Lisinopril', 10.00, TO_DATE('2025-07-01','YYYY-MM-DD'), TO_DATE('2025-12-31','YYYY-MM-DD'));

---------------------------------------------------
-- 1. Negative dose violates CHECK constraint
INSERT INTO PATIENT_MED VALUES 
(1, 1, 'Amoxicillin', -50, TO_DATE('2025-10-01','YYYY-MM-DD'),
TO_DATE('2025-10-10','YYYY-MM-DD'));
--explanation: insert statement tried to insert a negative dose value (-50):
-------------------------------------------------------
----corrected code

INSERT INTO PATIENT_MED VALUES 
(1, 1, 'Amoxicillin', 50, 
 TO_DATE('2025-10-01','YYYY-MM-DD'),
 TO_DATE('2025-10-10','YYYY-MM-DD'));
--2. ---------------------------------------------------------
SET SERVEROUTPUT ON;
SET FEEDBACK ON;

-- Helper to display bill totals and audit records
CREATE OR REPLACE PROCEDURE display_data IS
BEGIN
    DBMS_OUTPUT.PUT_LINE('----------------------------------------------------');
    DBMS_OUTPUT.PUT_LINE('BILL Totals:');
    FOR r IN (SELECT ID, TOTAL FROM BILL ORDER BY ID) LOOP
        DBMS_OUTPUT.PUT_LINE('  BILL_ID: ' || r.ID || ', Total: ' || r.TOTAL);
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('BILL_AUDIT Records:');
    FOR r IN (SELECT BILL_ID, OLD_TOTAL, NEW_TOTAL, TO_CHAR(CHANGED_AT, 'YYYY-MM-DD HH24:MI:SS') AS CHANGED_AT_STR FROM BILL_AUDIT ORDER BY CHANGED_AT, BILL_ID) LOOP
        DBMS_OUTPUT.PUT_LINE('  BILL_ID: ' || r.BILL_ID || ', Old Total: ' || r.OLD_TOTAL || ', New Total: ' || r.NEW_TOTAL || ', Changed At: ' || r.CHANGED_AT_STR);
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('----------------------------------------------------');
END;
/

-- Clean up previous data
TRUNCATE TABLE BILL_AUDIT;
TRUNCATE TABLE BILL_ITEM;
TRUNCATE TABLE BILL;

-- Initialize BILLs
INSERT INTO BILL (ID, TOTAL) VALUES (1, 0);
INSERT INTO BILL (ID, TOTAL) VALUES (2, 0);
INSERT INTO BILL (ID, TOTAL) VALUES (3, 0);
COMMIT;

DBMS_OUTPUT.PUT_LINE('*** Initial State ***');
EXEC display_data;

-- Test Case 1: Batch INSERT for multiple bills
DBMS_OUTPUT.PUT_LINE(CHR(10) || '*** Test Case 1: Batch INSERT (multiple bills) ***');
INSERT ALL
    INTO BILL_ITEM (BILL_ID, AMOUNT, UPDATED_AT) VALUES (1, 10.50, SYSDATE)
    INTO BILL_ITEM (BILL_ID, AMOUNT, UPDATED_AT) VALUES (1, 20.00, SYSDATE)
    INTO BILL_ITEM (BILL_ID, AMOUNT, UPDATED_AT) VALUES (2, 5.25, SYSDATE)
    INTO BILL_ITEM (BILL_ID, AMOUNT, UPDATED_AT) VALUES (3, 100.00, SYSDATE)
SELECT 1 FROM DUAL;
COMMIT;
EXEC display_data;
-- Expected:
-- BILL 1 Total: 30.50, Audit: (0 -> 30.50)
-- BILL 2 Total: 5.25, Audit: (0 -> 5.25)
-- BILL 3 Total: 100.00, Audit: (0 -> 100.00)


-- Test Case 2: Batch UPDATE (change amounts for existing items, single bill)
DBMS_OUTPUT.PUT_LINE(CHR(10) || '*** Test Case 2: Batch UPDATE (single bill) ***');
UPDATE BILL_ITEM
SET AMOUNT = AMOUNT + 5
WHERE BILL_ID = 1;
COMMIT;
EXEC display_data;
-- Expected:
-- BILL 1 Total: 40.50 (30.50 + 5 + 5), Audit: (30.50 -> 40.50)
-- Other BILLs unchanged.


-- Test Case 3: Batch UPDATE (change amount for one item, delete another for different bill)
DBMS_OUTPUT.PUT_LINE(CHR(10) || '*** Test Case 3: Mixed DML (Update + Delete on different bills) ***');
-- Simulate a complex transaction
BEGIN
    UPDATE BILL_ITEM
    SET AMOUNT = 10.00 -- Change an item for BILL 2
    WHERE BILL_ID = 2 AND AMOUNT = 5.25;

    DELETE FROM BILL_ITEM
    WHERE BILL_ID = 3 AND AMOUNT = 100.00; -- Delete item for BILL 3

    -- Add a new item to BILL 1
    INSERT INTO BILL_ITEM (BILL_ID, AMOUNT, UPDATED_AT) VALUES (1, 15.00, SYSDATE);
END;
/
COMMIT;
EXEC display_data;
-- Expected:
-- BILL 1 Total: 55.50 (40.50 + 15), Audit: (40.50 -> 55.50)
-- BILL 2 Total: 10.00, Audit: (5.25 -> 10.00)
-- BILL 3 Total: 0.00, Audit: (100.00 -> 0.00)

-- Test Case 4: Batch DELETE for a bill
DBMS_OUTPUT.PUT_LINE(CHR(10) || '*** Test Case 4: Batch DELETE (single bill) ***');
DELETE FROM BILL_ITEM
WHERE BILL_ID = 1;
COMMIT;
EXEC display_data;
-- Expected:
-- BILL 1 Total: 0.00, Audit: (55.50 -> 0.00)
-- Other BILLs unchanged.

-- Test Case 5: Update BILL_ID (changing an item from one bill to another)
DBMS_OUTPUT.PUT_LINE(CHR(10) || '*** Test Case 5: Update BILL_ID (moving an item) ***');
-- First, add an item to BILL 2 to move
INSERT INTO BILL_ITEM (BILL_ID, AMOUNT, UPDATED_AT) VALUES (2, 25.00, SYSDATE);
COMMIT;
DBMS_OUTPUT.PUT_LINE('--- After adding item to BILL 2 ---');
EXEC display_data;
-- Expected BILL 2 Total: 35.00 (10.00 + 25.00), Audit: (10.00 -> 35.00)

UPDATE BILL_ITEM
SET BILL_ID = 1       -- Move the item from BILL 2 to BILL 1
WHERE BILL_ID = 2 AND AMOUNT = 25.00;
COMMIT;
DBMS_OUTPUT.PUT_LINE('--- After moving item from BILL 2 to BILL 1 ---');
EXEC display_data;
-- Expected:
-- BILL 1 Total: 25.00, Audit: (0 -> 25.00)
-- BILL 2 Total: 10.00 (35.00 - 25.00), Audit: (35.00 -> 10.00)

--3.------------------------------------------
SET SERVEROUTPUT ON;
SET FEEDBACK ON;

-- Helper to display bill totals and audit records
CREATE OR REPLACE PROCEDURE display_data IS
BEGIN
    DBMS_OUTPUT.PUT_LINE('----------------------------------------------------');
    DBMS_OUTPUT.PUT_LINE('BILL Totals:');
    FOR r IN (SELECT ID, TOTAL FROM BILL ORDER BY ID) LOOP
        DBMS_OUTPUT.PUT_LINE('  BILL_ID: ' || r.ID || ', Total: ' || r.TOTAL);
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('BILL_AUDIT Records:');
    FOR r IN (SELECT BILL_ID, OLD_TOTAL, NEW_TOTAL, TO_CHAR(CHANGED_AT, 'YYYY-MM-DD HH24:MI:SS') AS CHANGED_AT_STR FROM BILL_AUDIT ORDER BY CHANGED_AT, BILL_ID) LOOP
        DBMS_OUTPUT.PUT_LINE('  BILL_ID: ' || r.BILL_ID || ', Old Total: ' || r.OLD_TOTAL || ', New Total: ' || r.NEW_TOTAL || ', Changed At: ' || r.CHANGED_AT_STR);
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('----------------------------------------------------');
END;
/

-- Clean up previous data
TRUNCATE TABLE BILL_AUDIT;
TRUNCATE TABLE BILL_ITEM;
TRUNCATE TABLE BILL;

-- Initialize BILLs
INSERT INTO BILL (ID, TOTAL) VALUES (1, 0);
INSERT INTO BILL (ID, TOTAL) VALUES (2, 0);
INSERT INTO BILL (ID, TOTAL) VALUES (3, 0);
COMMIT;

DBMS_OUTPUT.PUT_LINE('*** Initial State ***');
EXEC display_data;

-- Test Case 1: Batch INSERT for multiple bills
DBMS_OUTPUT.PUT_LINE(CHR(10) || '*** Test Case 1: Batch INSERT (multiple bills) ***');
INSERT ALL
    INTO BILL_ITEM (BILL_ID, AMOUNT, UPDATED_AT) VALUES (1, 10.50, SYSDATE)
    INTO BILL_ITEM (BILL_ID, AMOUNT, UPDATED_AT) VALUES (1, 20.00, SYSDATE)
    INTO BILL_ITEM (BILL_ID, AMOUNT, UPDATED_AT) VALUES (2, 5.25, SYSDATE)
    INTO BILL_ITEM (BILL_ID, AMOUNT, UPDATED_AT) VALUES (3, 100.00, SYSDATE)
SELECT 1 FROM DUAL;
COMMIT;
EXEC display_data;
-- Expected:
-- BILL 1 Total: 30.50, Audit: (0 -> 30.50)
-- BILL 2 Total: 5.25, Audit: (0 -> 5.25)
-- BILL 3 Total: 100.00, Audit: (0 -> 100.00)


-- Test Case 2: Batch UPDATE (change amounts for existing items, single bill)
DBMS_OUTPUT.PUT_LINE(CHR(10) || '*** Test Case 2: Batch UPDATE (single bill) ***');
UPDATE BILL_ITEM
SET AMOUNT = AMOUNT + 5
WHERE BILL_ID = 1;
COMMIT;
EXEC display_data;
-- Expected:
-- BILL 1 Total: 40.50 (30.50 + 5 + 5), Audit: (30.50 -> 40.50)
-- Other BILLs unchanged.


-- Test Case 3: Batch UPDATE (change amount for one item, delete another for different bill)
DBMS_OUTPUT.PUT_LINE(CHR(10) || '*** Test Case 3: Mixed DML (Update + Delete on different bills) ***');
-- Simulate a complex transaction
BEGIN
    UPDATE BILL_ITEM
    SET AMOUNT = 10.00 -- Change an item for BILL 2
    WHERE BILL_ID = 2 AND AMOUNT = 5.25;

    DELETE FROM BILL_ITEM
    WHERE BILL_ID = 3 AND AMOUNT = 100.00; -- Delete item for BILL 3

    -- Add a new item to BILL 1
    INSERT INTO BILL_ITEM (BILL_ID, AMOUNT, UPDATED_AT) VALUES (1, 15.00, SYSDATE);
END;
/
COMMIT;
EXEC display_data;
-- Expected:
-- BILL 1 Total: 55.50 (40.50 + 15), Audit: (40.50 -> 55.50)
-- BILL 2 Total: 10.00, Audit: (5.25 -> 10.00)
-- BILL 3 Total: 0.00, Audit: (100.00 -> 0.00)


-- Test Case 4: Batch DELETE for a bill
DBMS_OUTPUT.PUT_LINE(CHR(10) || '*** Test Case 4: Batch DELETE (single bill) ***');
DELETE FROM BILL_ITEM
WHERE BILL_ID = 1;
COMMIT;
EXEC display_data;
-- Expected:
-- BILL 1 Total: 0.00, Audit: (55.50 -> 0.00)
-- Other BILLs unchanged.

-- Test Case 5: Update BILL_ID (changing an item from one bill to another)
DBMS_OUTPUT.PUT_LINE(CHR(10) || '*** Test Case 5: Update BILL_ID (moving an item) ***');
-- First, add an item to BILL 2 to move
INSERT INTO BILL_ITEM (BILL_ID, AMOUNT, UPDATED_AT) VALUES (2, 25.00, SYSDATE);
COMMIT;
DBMS_OUTPUT.PUT_LINE('--- After adding item to BILL 2 ---');
EXEC display_data;
-- Expected BILL 2 Total: 35.00 (10.00 + 25.00), Audit: (10.00 -> 35.00)

UPDATE BILL_ITEM
SET BILL_ID = 1       -- Move the item from BILL 2 to BILL 1
WHERE BILL_ID = 2 AND AMOUNT = 25.00;
COMMIT;
DBMS_OUTPUT.PUT_LINE('--- After moving item from BILL 2 to BILL 1 ---');
EXEC display_data;

