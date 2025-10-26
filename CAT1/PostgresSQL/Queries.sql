---Case Study :Municipal Water Billing and Consumption Tracking System 
---Reg_number:224020114

-- -- -- -- 1. Create schema
CREATE TABLE Customer (
  CustomerID SERIAL PRIMARY KEY,
  FullName TEXT NOT NULL,
  Address TEXT,
  Type TEXT CHECK (Type IN ('Residential','Commercial')),
  Contact TEXT
);

CREATE TABLE Meter (
  MeterID SERIAL PRIMARY KEY,
  CustomerID INT NOT NULL REFERENCES Customer(CustomerID) ON DELETE CASCADE,
  InstallationDate DATE,
  Status TEXT CHECK (Status IN ('Active','Inactive','Faulty')) DEFAULT 'Active',
  LastReading NUMERIC DEFAULT 0
);

CREATE TABLE Reading (
  ReadingID SERIAL PRIMARY KEY,
  MeterID INT NOT NULL REFERENCES Meter(MeterID) ON DELETE CASCADE,
  ReadingDate DATE NOT NULL,
  CurrentReading NUMERIC CHECK (CurrentReading >= 0),
  Consumption NUMERIC CHECK (Consumption >= 0)
);

CREATE TABLE Bill (
  BillID SERIAL PRIMARY KEY,
  ReadingID INT UNIQUE NOT NULL REFERENCES Reading(ReadingID) ON DELETE CASCADE,
  AmountDue NUMERIC CHECK (AmountDue >= 0),
  DueDate DATE,
  Status TEXT CHECK (Status IN ('Unpaid','Paid','Overdue')) DEFAULT 'Unpaid'
);

CREATE TABLE Payment (
  PaymentID SERIAL PRIMARY KEY,
  BillID INT UNIQUE NOT NULL REFERENCES Bill(BillID) ON DELETE CASCADE,
  Amount NUMERIC CHECK (Amount >= 0),
  PaymentDate DATE DEFAULT CURRENT_DATE,
  Method TEXT
);

CREATE TABLE Maintenance (
  MaintID SERIAL PRIMARY KEY,
  MeterID INT NOT NULL REFERENCES Meter(MeterID) ON DELETE CASCADE,
  Issue TEXT,
  Technician TEXT,
  DateFixed DATE,
  Cost NUMERIC CHECK (Cost >= 0)
);
-- ----2. Cascade Bill -> Payment via FK ON DELETE CASCADE is set above.
-- ---3. Insert 10 customers and 10 readings
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
('Yvonne C','7 Bwiza','Commercial','0788020010');
 INSERT INTO Customer (FullName, Address, Type, Contact) VALUES
('Uwase C','11 Bwiza','Commercial','0788020010'),
('Assouman A','12 Murama','Residential','0788020009');
INSERT INTO Meter (CustomerID, InstallationDate, Status, LastReading) VALUES
(1,'2022-01-01','Active',120.0),(2,'2022-06-01','Active',50.0),(3,'2023-03-01','Active',300.0),
(4,'2021-11-01','Active',220.0),(5,'2022-08-15','Active',80.0),(6,'2022-09-01','Active',150.0),
(7,'2020-12-01','Active',40.0),(8,'2024-02-01',
-'Active',10.0),(9,'2023-07-01','Active',65.0),(10,'2022-05-01','Active',500.0);
INSERT INTO Meter (CustomerID, InstallationDate, Status, LastReading) VALUES
(11,'2022-01-01','Active',120.0),(12,'2022-06-01','Active',50.0);

------create readings and bills
INSERT INTO Reading (MeterID, ReadingDate, CurrentReading, Consumption) VALUES
(1,'2025-09-01',150,30),(2,'2025-09-01',75,25),(3,'2025-09-01',350,50),
(4,'2025-09-01',260,40),(5,'2025-09-01',90,10),(6,'2025-09-01',170,20),
(7,'2025-09-01',70,30),(8,'2025-09-01',20,10),(9,'2025-09-01',95,30),(10,'2025-09-01',550,50);

 INSERT INTO Reading (MeterID, ReadingDate, CurrentReading, Consumption) VALUES
(11,'2025-09-01',150,30),(12,'2025-09-01',75,25),(13,'2025-09-01',350,50),
(14,'2025-09-01',260,40),(15,'2025-09-01',90,10);

INSERT INTO Bill (ReadingID, AmountDue, DueDate, Status) VALUES
(1,30*0.5,'2025-10-01','Unpaid'),
(2,25*0.5,'2025-10-01','Unpaid'),
(3,50*0.5,'2025-10-01','Unpaid'),
(4,40*0.5,'2025-10-01','Unpaid'),
(5,10*0.5,'2025-10-01','Unpaid'),
(6,20*0.5,'2025-10-01','Paid'),
(7,30*0.5,'2025-10-01','Unpaid'),
(8,10*0.5,'2025-10-01','Unpaid'),
(9,30*0.5,'2025-10-01','Unpaid'),
(10,50*0.5,'2025-10-01','Unpaid');
Select * from reading;

-- ---Insert /10 payments

INSERT INTO Payment (BillID, Amount, PaymentDate, Method)
VALUES (
  (SELECT BillID FROM Bill WHERE ReadingID = (SELECT ReadingID FROM Reading WHERE MeterID = 1 ORDER BY ReadingDate DESC LIMIT 1)),
  2500.00, '2025-09-10', 'Mobile Money'
) ON CONFLICT (BillID) DO UPDATE SET Amount = EXCLUDED.Amount, PaymentDate = EXCLUDED.PaymentDate, Method = EXCLUDED.Method;

INSERT INTO Payment (BillID, Amount, PaymentDate, Method)
VALUES (
  (SELECT BillID FROM Bill WHERE ReadingID = (SELECT ReadingID FROM Reading WHERE MeterID = 2 ORDER BY ReadingDate DESC LIMIT 1)),
  3000.00, '2025-09-11', 'Cash'
) ON CONFLICT (BillID) DO UPDATE SET Amount = EXCLUDED.Amount, PaymentDate = EXCLUDED.PaymentDate, Method = EXCLUDED.Method;

INSERT INTO Payment (BillID, Amount, PaymentDate, Method)
VALUES (
  (SELECT BillID FROM Bill WHERE ReadingID = (SELECT ReadingID FROM Reading WHERE MeterID = 3 ORDER BY ReadingDate DESC LIMIT 1)),
  4500.00, '2025-09-12', 'Bank Transfer'
) ON CONFLICT (BillID) DO UPDATE SET Amount = EXCLUDED.Amount, PaymentDate = EXCLUDED.PaymentDate, Method = EXCLUDED.Method;

INSERT INTO Payment (BillID, Amount, PaymentDate, Method)
VALUES (
  (SELECT BillID FROM Bill WHERE ReadingID = (SELECT ReadingID FROM Reading WHERE MeterID = 4 ORDER BY ReadingDate DESC LIMIT 1)),
  2800.00, '2025-09-13', 'Card'
) ON CONFLICT (BillID) DO UPDATE SET Amount = EXCLUDED.Amount, PaymentDate = EXCLUDED.PaymentDate, Method = EXCLUDED.Method;

INSERT INTO Payment (BillID, Amount, PaymentDate, Method)
VALUES (
  (SELECT BillID FROM Bill WHERE ReadingID = (SELECT ReadingID FROM Reading WHERE MeterID = 5 ORDER BY ReadingDate DESC LIMIT 1)),
  3200.00, '2025-09-14', 'Mobile Money'
) ON CONFLICT (BillID) DO UPDATE SET Amount = EXCLUDED.Amount, PaymentDate = EXCLUDED.PaymentDate, Method = EXCLUDED.Method;

INSERT INTO Payment (BillID, Amount, PaymentDate, Method)
VALUES (
  (SELECT BillID FROM Bill WHERE ReadingID = (SELECT ReadingID FROM Reading WHERE MeterID = 6 ORDER BY ReadingDate DESC LIMIT 1)),
  5000.00, '2025-09-15', 'Bank Transfer'
) ON CONFLICT (BillID) DO UPDATE SET Amount = EXCLUDED.Amount, PaymentDate = EXCLUDED.PaymentDate, Method = EXCLUDED.Method;

INSERT INTO Payment (BillID, Amount, PaymentDate, Method)
VALUES (
  (SELECT BillID FROM Bill WHERE ReadingID = (SELECT ReadingID FROM Reading WHERE MeterID = 7 ORDER BY ReadingDate DESC LIMIT 1)),
  2600.00, '2025-09-16', 'Cash'
) ON CONFLICT (BillID) DO UPDATE SET Amount = EXCLUDED.Amount, PaymentDate = EXCLUDED.PaymentDate, Method = EXCLUDED.Method;

INSERT INTO Payment (BillID, Amount, PaymentDate, Method)
VALUES (
  (SELECT BillID FROM Bill WHERE ReadingID = (SELECT ReadingID FROM Reading WHERE MeterID = 8 ORDER BY ReadingDate DESC LIMIT 1)),
  4100.00, '2025-09-17', 'Card'
) ON CONFLICT (BillID) DO UPDATE SET Amount = EXCLUDED.Amount, PaymentDate = EXCLUDED.PaymentDate, Method = EXCLUDED.Method;

INSERT INTO Payment (BillID, Amount, PaymentDate, Method)
VALUES (
  (SELECT BillID FROM Bill WHERE ReadingID = (SELECT ReadingID FROM Reading WHERE MeterID = 9 ORDER BY ReadingDate DESC LIMIT 1)),
  3500.00, '2025-09-18', 'Mobile Money'
) ON CONFLICT (BillID) DO UPDATE SET Amount = EXCLUDED.Amount, PaymentDate = EXCLUDED.PaymentDate, Method = EXCLUDED.Method;

INSERT INTO Payment (BillID, Amount, PaymentDate, Method)
VALUES (
  (SELECT BillID FROM Bill WHERE ReadingID = (SELECT ReadingID FROM Reading WHERE MeterID = 10 ORDER BY ReadingDate DESC LIMIT 1)),
  6000.00, '2025-09-19', 'Bank Transfer'
) ON CONFLICT (BillID) DO UPDATE SET Amount = EXCLUDED.Amount, PaymentDate = EXCLUDED.PaymentDate, Method = EXCLUDED.Method;

--Insert 10 Maintenance Records
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

-- ---4. Retrieve unpaid bills per customer
SELECT c.CustomerID, c.FullName, b.BillID, b.AmountDue, b.DueDate
FROM Bill b
JOIN Reading r ON b.ReadingID = r.ReadingID
JOIN Meter m ON r.MeterID = m.MeterID
JOIN Customer c ON m.CustomerID = c.CustomerID
WHERE b.Status = 'Unpaid'
ORDER BY c.FullName;
---5. Update meter’s last reading after billing (example for ReadingID=1)
UPDATE Meter m
SET LastReading = r.CurrentReading
FROM Reading r
WHERE r.ReadingID = 1 AND m.MeterID = r.MeterID;

-- ---6. Identify high-consumption customers
SELECT c.CustomerID, c.FullName, r.ReadingDate, r.Consumption
FROM Reading r
JOIN Meter m ON r.MeterID = m.MeterID
JOIN Customer c ON m.CustomerID = c.CustomerID
WHERE r.Consumption > 40
ORDER BY r.Consumption DESC;
-- 7. Create view summarizing total payments per month
CREATE OR REPLACE VIEW vw_total_payments_per_month AS
SELECT date_trunc('month', PaymentDate)::date AS month,
       SUM(Amount) AS total_payments
FROM Payment
GROUP BY date_trunc('month', PaymentDate)::date
ORDER BY month DESC;

---8. Trigger that calculates bill automatically after new reading entry
CREATE OR REPLACE FUNCTION fn_calc_bill_after_reading() RETURNS TRIGGER AS $$
DECLARE consumption NUMERIC;
  rate NUMERIC := 0.5; -- example flat rate per unit
  amount NUMERIC;
BEGIN
  IF NEW.Consumption IS NULL THEN
    -- compute consumption from meter last reading
    SELECT LastReading INTO consumption FROM Meter WHERE MeterID = NEW.MeterID;
    IF consumption IS NULL THEN consumption := 0; END IF;
    NEW.Consumption := NEW.CurrentReading - consumption;
  END IF;
  IF NEW.Consumption < 0 THEN
    NEW.Consumption := 0;
  END IF;
  amount := NEW.Consumption * rate;
  -- create bill
  INSERT INTO Bill (ReadingID, AmountDue, DueDate, Status)
    VALUES (NEW.ReadingID, amount, CURRENT_DATE + INTERVAL '30 days', 'Unpaid');
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_calc_bill AFTER INSERT ON Reading
FOR EACH ROW EXECUTE FUNCTION fn_calc_bill_after_reading();



