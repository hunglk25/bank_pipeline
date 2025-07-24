CREATE TABLE IF NOT EXISTS Customer (
  CustomerID SERIAL PRIMARY KEY,
  NationalID VARCHAR(20) UNIQUE NOT NULL,   -- CCCD/Passport
  Name VARCHAR(100),
  Address VARCHAR(255),
  Contact VARCHAR(50),
  Username VARCHAR(50),
  PasswordHash VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS Device (
  DeviceID SERIAL PRIMARY KEY,
  CustomerID INT REFERENCES Customer(CustomerID),
  DeviceType VARCHAR(50),
  DeviceInfo VARCHAR(255),
  IsVerified BOOLEAN DEFAULT FALSE,
  LastUsed TIMESTAMP
);

CREATE TABLE IF NOT EXISTS Account (
  AccountID SERIAL PRIMARY KEY,
  CustomerID INT REFERENCES Customer(CustomerID),
  AccountType VARCHAR(50),
  Balance DECIMAL(15,2),
  Currency CHAR(3),
  Status VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS Transaction (
  TransactionID SERIAL PRIMARY KEY,
  FromAccountID INT REFERENCES Account(AccountID),
  ToAccountID INT REFERENCES Account(AccountID),
  DeviceID INT REFERENCES Device(DeviceID),
  TxnType VARCHAR(20),
  Amount DECIMAL(15,2),
  Timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS AuthenticationLog (
  AuthID SERIAL PRIMARY KEY,
  CustomerID INT REFERENCES Customer(CustomerID),
  DeviceID INT REFERENCES Device(DeviceID),
  AuthMethod VARCHAR(20),   -- e.g. OTP, Biometric, Password
  AuthStatus VARCHAR(10),   -- e.g. SUCCESS/FAIL
  Timestamp TIMESTAMP
);


CREATE TABLE IF NOT EXISTS RiskAlerts (
    AlertID SERIAL PRIMARY KEY,
    CustomerID INT REFERENCES Customer(CustomerID),
    TransactionID INT REFERENCES Transaction(TransactionID),
    AlertType VARCHAR(50) NOT NULL, -- HIGH_VALUE_NO_STRONG_AUTH, UNVERIFIED_DEVICE, DAILY_LIMIT_EXCEEDED
    AlertLevel VARCHAR(20) DEFAULT 'MEDIUM', -- LOW, MEDIUM, HIGH, CRITICAL
    Description VARCHAR(255),
    CreatedAt TIMESTAMP DEFAULT NOW()
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_customers_cccd ON Customer(NationalID);
CREATE INDEX IF NOT EXISTS idx_accounts_customer ON Account(CustomerID);
CREATE INDEX IF NOT EXISTS idx_devices_customer ON Device(CustomerID);
CREATE INDEX IF NOT EXISTS idx_auth_customer ON AuthenticationLog(CustomerID);
CREATE INDEX IF NOT EXISTS idx_transactions_account ON Transaction(FromAccountID);
CREATE INDEX IF NOT EXISTS idx_transactions_device ON Transaction(DeviceID);
CREATE INDEX IF NOT EXISTS idx_transactions_time ON Transaction(Timestamp);
CREATE INDEX IF NOT EXISTS idx_risk_alerts_customer ON RiskAlerts(CustomerID);
