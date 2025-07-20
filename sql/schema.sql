CREATE TABLE IF NOT EXISTS Customer (
  CustomerID SERIAL PRIMARY KEY,
  NationalID VARCHAR(20) UNIQUE NOT NULL,   -- CCCD/Passport
  Name VARCHAR(100),
  Address VARCHAR(255),
  Contact VARCHAR(50),
  Username VARCHAR(50) UNIQUE,
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
  TxnType VARCHAR(20),
  Amount DECIMAL(15,2),
  Timestamp TIMESTAMP,
  RiskFlag BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS AuthenticationLog (
  AuthID SERIAL PRIMARY KEY,
  CustomerID INT REFERENCES Customer(CustomerID),
  DeviceID INT REFERENCES Device(DeviceID),
  AuthMethod VARCHAR(20),   -- e.g. OTP, Biometric, Password
  AuthStatus VARCHAR(10),   -- e.g. SUCCESS/FAIL
  Timestamp TIMESTAMP
);
