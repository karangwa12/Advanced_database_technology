#  Municipal Water Billing and Consumption Tracking System
## Project Overview
This **Water Utility Database System** manages customers, meters, readings, bills, payments, and maintenance requests.  
It supports **automated billing**, **water usage tracking**, and **payment management** for both *residential* and *commercial* customers.

The database demonstrates:
- Proper relational schema design  
- Referential integrity via **foreign keys**  
- **Cascade deletes** for dependent records  
- Use of **triggers** for automatic bill generation  
- SQL **views** for reporting and summarization  

## 1. Database Schema

### **Tables Created**

| Table Name | Description |
|-------------|--------------|
| **Customer** | Stores customer details such as name, address, type, and contact. |
| **Meter** | Represents water meters assigned to customers, tracks installation dates and status. |
| **Reading** | Holds periodic meter readings and consumption amounts. |
| **Bill** | Contains billing details generated from readings. |
| **Payment** | Records payment transactions linked to bills. |
| **Maintenance** | Logs maintenance activities and costs for each meter. |

### **Relationships**
- **Customer Meter** (1:N)  
- **Meter Reading** (1:N)  
- **Reading  Bill** (1:1)  
- **Bill Payment** (1:1, with *ON DELETE CASCADE*)  
- **Meter  Maintenance** (1:N)  

##  2. Schema Definition (DDL)

Each table is defined with **primary keys**, **foreign keys**, and **check constraints** to maintain data consistency.
```sql
CREATE TABLE Bill (
  BillID SERIAL PRIMARY KEY,
  ReadingID INT UNIQUE NOT NULL REFERENCES Reading(ReadingID) ON DELETE CASCADE,
  AmountDue NUMERIC CHECK (AmountDue >= 0),
  DueDate DATE,
  Status TEXT CHECK (Status IN ('Unpaid','Paid','Overdue')) DEFAULT 'Unpaid'
);
```

 **Note:** The foreign key constraint in the `Payment` table enforces **cascade deletion** — deleting a bill automatically deletes its associated payment.
---
## 3. Data Insertion (DML)

### **Customers**
Inserted sample customer data representing both residential and commercial users.

```sql
INSERT INTO Customer (FullName, Address, Type, Contact) VALUES
('Alice N','12 Kabeza','Residential','0788020001'),
('Jean M','45 Murinzi','Residential','0788020002'),
('Grace U','78 Kagugu','Commercial','0788020003'),
('Paul H','9 Kagano','Residential','0788020004'),
('Sam K','5 Amataba','Residential','0788020005'),
('Lydia T','22 Bwiza','Commercial','0788020006'),
('Mark S','99 Berwa','Residential','0788020007'),
('Rita B','3 Kanombe','Residential','0788020008'),
('Omar A','11 Murama','Residential','0788020009'),
('Yvonne C','7 Bwiza','Commercial','0788020010'),
('Uwase C','11 Bwiza','Commercial','0788020010'),
('Assouman A','12 Murama','Residential','0788020009');
```
---

### **Meters**
Each customer has one meter with installation date, status, and initial last reading.

```sql
INSERT INTO Meter (CustomerID, InstallationDate, Status, LastReading)
VALUES
(1,'2022-01-01','Active',120.0),
(2,'2022-06-01','Active',50.0),
(3,'2023-03-01','Active',300.0),
(4,'2021-11-01','Active',220.0),
(5,'2022-08-15','Active',80.0),
(6,'2022-09-01','Active',150.0),
(7,'2020-12-01','Active',40.0),
(8,'2024-02-01','Active',10.0),
(9,'2023-07-01','Active',65.0),
(10,'2022-05-01','Active',500.0),
(11,'2022-01-01','Active',120.0),
(12,'2022-06-01','Active',50.0);
```
---

### **Readings and Bills**
Each reading records the new meter value and consumption. 
```sql
INSERT INTO Reading (MeterID, ReadingDate, CurrentReading, Consumption)
VALUES
(1,'2025-09-01',150,30),
(2,'2025-09-01',75,25),
(3,'2025-09-01',350,50),
(4,'2025-09-01',260,40),
(5,'2025-09-01',90,10),
(6,'2025-09-01',170,20),
(7,'2025-09-01',70,30),
(8,'2025-09-01',20,10),
(9,'2025-09-01',95,30),
(10,'2025-09-01',550,50);
```
**Bill Example:**
```sql
INSERT INTO Bill (ReadingID, AmountDue, DueDate, Status)
VALUES (1,30*0.5,'2025-10-01','Unpaid'), (6,20*0.5,'2025-10-01','Paid');
```
### **Payments**
Each payment corresponds to one bill.

```sql
INSERT INTO Payment (BillID, Amount, PaymentDate, Method)
VALUES
((SELECT BillID FROM Bill WHERE ReadingID = 1), 2500.00, '2025-09-10', 'Mobile Money'),
((SELECT BillID FROM Bill WHERE ReadingID = 2), 3000.00, '2025-09-11', 'Cash'),
((SELECT BillID FROM Bill WHERE ReadingID = 3), 4500.00, '2025-09-12', 'Bank Transfer'),
((SELECT BillID FROM Bill WHERE ReadingID = 4), 2800.00, '2025-09-13', 'Card'),
((SELECT BillID FROM Bill WHERE ReadingID = 5), 3200.00, '2025-09-14', 'Mobile Money'),
((SELECT BillID FROM Bill WHERE ReadingID = 6), 5000.00, '2025-09-15', 'Bank Transfer'),
((SELECT BillID FROM Bill WHERE ReadingID = 7), 2600.00, '2025-09-16', 'Cash'),
((SELECT BillID FROM Bill WHERE ReadingID = 8), 4100.00, '2025-09-17', 'Card'),
((SELECT BillID FROM Bill WHERE ReadingID = 9), 3500.00, '2025-09-18', 'Mobile Money'),
((SELECT BillID FROM Bill WHERE ReadingID = 10), 6000.00, '2025-09-19', 'Bank Transfer');
```

### **Maintenance**
Logs maintenance issues, technicians, dates, and costs.
```sql
INSERT INTO Maintenance (MeterID, Issue, Technician, DateFixed, Cost) VALUES
(1, 'Leaking meter valve repaired', 'Technician John', '2025-09-05', 1500.00),
(2, 'Broken display screen replaced', 'Technician Alice', '2025-09-06', 2300.00),
(3, 'Low pressure sensor fixed', 'Technician Robert', '2025-09-07', 2800.00),
(4, 'Rust buildup cleaned and recalibrated', 'Technician Ben', '2025-09-08', 3200.00),
(5, 'Meter stopped recording — replaced unit', 'Technician Grace', '2025-09-09', 4100.00),
(6, 'Loose wiring repaired', 'Technician Daniel', '2025-09-10', 2600.00),
(7, 'Valve replaced due to leakage', 'Technician Stella', '2025-09-11', 3500.00),
(8, 'Sensor malfunction corrected', 'Technician Patrick', '2025-09-12', 2900.00),
(9, 'Meter calibration adjusted', 'Technician Ruth', '2025-09-13', 3100.00),
(10, 'Complete meter replacement', 'Technician Ivan', '2025-09-14', 5200.00);

## 4. Key Queries

### **a) Retrieve Unpaid Bills Per Customer**
SELECT c.CustomerID, c.FullName, b.BillID, b.AmountDue, b.DueDate
FROM Bill b
JOIN Reading r ON b.ReadingID = r.ReadingID
JOIN Meter m ON r.MeterID = m.MeterID
JOIN Customer c ON m.CustomerID = c.CustomerID
WHERE b.Status = 'Unpaid'
ORDER BY c.FullName;

### **b) Update Meter’s Last Reading After Billing**

UPDATE Meter m
SET LastReading = r.CurrentReading
FROM Reading r
WHERE r.ReadingID = 1 AND m.MeterID = r.MeterID;

### **c) Identify High-Consumption Customers**
SELECT c.CustomerID, c.FullName, r.ReadingDate, r.Consumption
FROM Reading r
JOIN Meter m ON r.MeterID = m.MeterID
JOIN Customer c ON m.CustomerID = c.CustomerID
WHERE r.Consumption > 40
ORDER BY r.Consumption DESC;

## 5. Create View  Monthly Payment Summary
CREATE OR REPLACE VIEW vw_total_payments_per_month AS
SELECT date_trunc('month', PaymentDate)::date AS month,
       SUM(Amount) AS total_payments
FROM Payment
GROUP BY date_trunc('month', PaymentDate)::date
ORDER BY month DESC;
