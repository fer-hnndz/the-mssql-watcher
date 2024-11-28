-- Tabla 1: Usuarios (Ejemplo con tipos int, bigint, char, date)
CREATE TABLE Usuarios (
    ID INT PRIMARY KEY,
    Nombre CHAR(50),
    FechaNacimiento DATE,
    IngresosAnuales BIGINT
);

-- Inserts
BEGIN TRANSACTION;
INSERT INTO Usuarios (ID, Nombre, FechaNacimiento, IngresosAnuales) 
VALUES (1, 'Juan Perez', '1985-07-14', 7500000);

INSERT INTO Usuarios (ID, Nombre, FechaNacimiento, IngresosAnuales) 
VALUES (2, 'Ana Lopez', '1990-03-22', 6000000);
COMMIT;

-- Update
BEGIN TRANSACTION;
UPDATE Usuarios
SET IngresosAnuales = 8000000
WHERE ID = 1;
COMMIT;

-- Delete
BEGIN TRANSACTION;
DELETE FROM Usuarios
WHERE ID = 2;
COMMIT;

-- Tabla 2: Transacciones (Ejemplo con tipos float, money, datetime)
CREATE TABLE Transacciones (
    ID INT PRIMARY KEY,
    Monto MONEY,
    FechaTransaccion DATETIME,
    TasaCambio FLOAT
);

-- Inserts
BEGIN TRANSACTION;
INSERT INTO Transacciones (ID, Monto, FechaTransaccion, TasaCambio)
VALUES (1, 1200.50, '2024-11-28 10:15:00', 24.5);

INSERT INTO Transacciones (ID, Monto, FechaTransaccion, TasaCambio)
VALUES (2, 800.75, '2024-11-27 14:45:00', 25.0);
COMMIT;

-- Update
BEGIN TRANSACTION;
UPDATE Transacciones
SET TasaCambio = 24.8
WHERE ID = 1;
COMMIT;

-- Delete
BEGIN TRANSACTION;
DELETE FROM Transacciones
WHERE ID = 2;
COMMIT;

-- Tabla 3: Productos (Ejemplo con tipos smallint, tinyint, nchar, decimal)
CREATE TABLE Productos (
    ID SMALLINT PRIMARY KEY,
    Nombre NCHAR(100),
    Stock TINYINT,
    Precio DECIMAL(10, 2)
);

-- Inserts
BEGIN TRANSACTION;
INSERT INTO Productos (ID, Nombre, Stock, Precio)
VALUES (1, N'Producto A', 20, 15.99);

INSERT INTO Productos (ID, Nombre, Stock, Precio)
VALUES (2, N'Producto B', 50, 9.49);
COMMIT;

-- Update
BEGIN TRANSACTION;
UPDATE Productos
SET Stock = 18
WHERE ID = 1;
COMMIT;

-- Delete
BEGIN TRANSACTION;
DELETE FROM Productos
WHERE ID = 2;
COMMIT;

-- Tabla 4: Empleados (Ejemplo con tipos smallmoney y smalldatetime)
CREATE TABLE Empleados (
    ID INT PRIMARY KEY,
    Nombre CHAR(50),
    Salario SMALLMONEY,
    FechaIngreso SMALLDATETIME
);

-- Inserts
BEGIN TRANSACTION;
INSERT INTO Empleados (ID, Nombre, Salario, FechaIngreso)
VALUES (1, 'Carlos Jimenez', 3500.75, '2023-06-15 08:30:00');

INSERT INTO Empleados (ID, Nombre, Salario, FechaIngreso)
VALUES (2, 'Laura Martinez', 4200.50, '2022-11-01 09:00:00');
COMMIT;

-- Update
BEGIN TRANSACTION;
UPDATE Empleados
SET Salario = 3600.80
WHERE ID = 1;
COMMIT;

-- Delete
BEGIN TRANSACTION;
DELETE FROM Empleados
WHERE ID = 2;
COMMIT;
