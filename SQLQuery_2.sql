-- Crear las tablas
CREATE TABLE Clientes (
    ClienteID INT PRIMARY KEY,
    Nombre NVARCHAR(100),
    Email NVARCHAR(100)
);

CREATE TABLE Productos (
    ProductoID INT PRIMARY KEY,
    Nombre NVARCHAR(100),
    Precio DECIMAL(10, 2)
);

CREATE TABLE Pedidos (
    PedidoID INT PRIMARY KEY,
    ClienteID INT FOREIGN KEY REFERENCES Clientes(ClienteID),
    Fecha DATE
);

CREATE TABLE DetallesPedido (
    DetalleID INT PRIMARY KEY,
    PedidoID INT FOREIGN KEY REFERENCES Pedidos(PedidoID),
    ProductoID INT FOREIGN KEY REFERENCES Productos(ProductoID),
    Cantidad INT
);

CREATE TABLE Pagos (
    PagoID INT PRIMARY KEY,
    PedidoID INT FOREIGN KEY REFERENCES Pedidos(PedidoID),
    Monto DECIMAL(10, 2),
    FechaPago DATE
);

-- Iniciar transacción para insertar en Clientes
BEGIN TRANSACTION;
BEGIN TRY
    -- Insertar registros en la tabla Clientes
    INSERT INTO Clientes (ClienteID, Nombre, Email)
    VALUES 
        (1, 'Juan Pérez', 'juan.perez@email.com'),
        (2, 'Ana López', 'ana.lopez@email.com'),
        (3, 'Carlos Rivera', 'carlos.rivera@email.com'),
        (4, 'Lucía Martínez', 'lucia.martinez@email.com'),
        (5, 'Mario Gómez', 'mario.gomez@email.com');
    
    -- Confirmar la transacción
    COMMIT TRANSACTION;
    PRINT 'Inserción en Clientes completada con éxito.';
END TRY
BEGIN CATCH
    -- Revertir la transacción en caso de error
    ROLLBACK TRANSACTION;
    PRINT 'Error en la inserción de Clientes. Se han revertido los cambios.';
    THROW;
END CATCH;

-- Iniciar transacción para insertar en Productos
BEGIN TRANSACTION;
BEGIN TRY
    -- Insertar registros en la tabla Productos
    INSERT INTO Productos (ProductoID, Nombre, Precio)
    VALUES 
        (1, 'Laptop', 1200.00),
        (2, 'Mouse', 25.50),
        (3, 'Teclado', 45.00),
        (4, 'Monitor', 250.00),
        (5, 'Impresora', 150.00);
    
    -- Confirmar la transacción
    COMMIT TRANSACTION;
    PRINT 'Inserción en Productos completada con éxito.';
END TRY
BEGIN CATCH
    -- Revertir la transacción en caso de error
    ROLLBACK TRANSACTION;
    PRINT 'Error en la inserción de Productos. Se han revertido los cambios.';
    THROW;
END CATCH;

-- Iniciar transacción para insertar en Pedidos
BEGIN TRANSACTION;
BEGIN TRY
    -- Insertar registros en la tabla Pedidos
    INSERT INTO Pedidos (PedidoID, ClienteID, Fecha)
    VALUES 
        (1, 1, '2024-11-22'),
        (2, 2, '2024-11-21'),
        (3, 3, '2024-11-20'),
        (4, 4, '2024-11-19'),
        (5, 5, '2024-11-18');
    
    -- Confirmar la transacción
    COMMIT TRANSACTION;
    PRINT 'Inserción en Pedidos completada con éxito.';
END TRY
BEGIN CATCH
    -- Revertir la transacción en caso de error
    ROLLBACK TRANSACTION;
    PRINT 'Error en la inserción de Pedidos. Se han revertido los cambios.';
    THROW;
END CATCH;

-- Iniciar transacción para insertar en DetallesPedido
BEGIN TRANSACTION;
BEGIN TRY
    -- Insertar registros en la tabla DetallesPedido
    INSERT INTO DetallesPedido (DetalleID, PedidoID, ProductoID, Cantidad)
    VALUES 
        (1, 1, 1, 1),
        (2, 1, 2, 2),
        (3, 2, 3, 1),
        (4, 3, 4, 1),
        (5, 4, 5, 1),
        (6, 5, 1, 1),
        (7, 5, 2, 1),
        (8, 5, 3, 1),
        (9, 5, 4, 1),
        (10, 5, 5, 1);
    
    -- Confirmar la transacción
    COMMIT TRANSACTION;
    PRINT 'Inserción en DetallesPedido completada con éxito.';
END TRY
BEGIN CATCH
    -- Revertir la transacción en caso de error
    ROLLBACK TRANSACTION;
    PRINT 'Error en la inserción de DetallesPedido. Se han revertido los cambios.';
    THROW;
END CATCH;

-- Iniciar transacción para insertar en Pagos
BEGIN TRANSACTION;
BEGIN TRY
    -- Insertar registros en la tabla Pagos
    INSERT INTO Pagos (PagoID, PedidoID, Monto, FechaPago)
    VALUES 
        (1, 1, 1250.00, '2024-11-22'),
        (2, 2, 45.00, '2024-11-21'),
        (3, 3, 250.00, '2024-11-20'),
        (4, 4, 150.00, '2024-11-19'),
        (5, 5, 1720.50, '2024-11-18');
    
    -- Confirmar la transacción
    COMMIT TRANSACTION;
    PRINT 'Inserción en Pagos completada con éxito.';
END TRY
BEGIN CATCH
    -- Revertir la transacción en caso de error
    ROLLBACK TRANSACTION;
    PRINT 'Error en la inserción de Pagos. Se han revertido los cambios.';
    THROW;
END CATCH;
