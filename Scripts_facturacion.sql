/* *** MIGRACIÓN DE MODELO DE FACTURAS *** */
USE [GD1C2020]; -- TODO BP: Eliminar

-- TODO BP: COLOCARLO JUNTO A LOS DEMÁS DROPS --
-- FUNCIONES --
if object_id('DATASCIENTISTS.ObtenerUltimoIdButaca') is not null
	DROP FUNCTION DATASCIENTISTS.ObtenerUltimoIdButaca;
GO

if object_id('DATASCIENTISTS.ObtenerSucursal') is not null
	DROP FUNCTION DATASCIENTISTS.ObtenerSucursal;
GO

if object_id('DATASCIENTISTS.ObtenerCliente') is not null
	DROP FUNCTION DATASCIENTISTS.ObtenerCliente;
GO

-- PROCEDURE --
if object_id('DATASCIENTISTS.MigracionInsertarClientes') is not null
	DROP PROCEDURE DATASCIENTISTS.MigracionInsertarClientes;
GO

if object_id('DATASCIENTISTS.MigracionInsertarSucursales') is not null
	DROP PROCEDURE DATASCIENTISTS.MigracionInsertarSucursales;
GO

if object_id('DATASCIENTISTS.InsertarFactura') is not null
	DROP PROCEDURE DATASCIENTISTS.InsertarFactura;
GO

if object_id('DATASCIENTISTS.MigracionInsertarFacturasItems') is not null
	DROP PROCEDURE DATASCIENTISTS.MigracionInsertarFacturasItems;
GO

/* *** MIGRAR CLIENTES *** */
CREATE PROCEDURE DATASCIENTISTS.MigracionInsertarClientes
AS
	INSERT [DATASCIENTISTS].CLIENTE
		(CLIENTE_DNI,
		 CLIENTE_NOMBRE,
		 CLIENTE_APELLIDO,
		 CLIENTE_FECHA_NAC,
		 CLIENTE_MAIL,
		 CLIENTE_TELEFONO)
	(
		SELECT [CLIENTE_DNI]
			  ,[CLIENTE_NOMBRE]
			  ,[CLIENTE_APELLIDO]
			  ,[CLIENTE_FECHA_NAC]
			  ,[CLIENTE_MAIL]
			  ,[CLIENTE_TELEFONO]
		  FROM [GD1C2020].[gd_esquema].[Maestra]
		 WHERE [CLIENTE_APELLIDO] IS NOT NULL
		   AND [CLIENTE_NOMBRE] IS NOT NULL
		   AND [CLIENTE_DNI] IS NOT NULL
		   AND [CLIENTE_FECHA_NAC] IS NOT NULL
		   AND [CLIENTE_MAIL] IS NOT NULL
		   AND [CLIENTE_TELEFONO] IS NOT NULL
		 GROUP BY [CLIENTE_APELLIDO]
				 ,[CLIENTE_NOMBRE]
				 ,[CLIENTE_DNI]
				 ,[CLIENTE_FECHA_NAC]
				 ,[CLIENTE_MAIL]
				 ,[CLIENTE_TELEFONO]
	);
	PRINT CAST(SYSDATETIME() AS VARCHAR(25))+' Clientes insertados correctamente';
GO

/* *** MIGRAR SUCURSALES *** */
CREATE PROCEDURE DATASCIENTISTS.MigracionInsertarSucursales
AS
	INSERT [DATASCIENTISTS].SUCURSAL
		(SUCURSAL_DIR,
		 SUCURSAL_MAIL,
		 SUCURSAL_TELEFONO)
	(
		SELECT [SUCURSAL_DIR]
			  ,[SUCURSAL_MAIL]
			  ,[SUCURSAL_TELEFONO]
		  FROM [GD1C2020].[gd_esquema].[Maestra]
		 WHERE [SUCURSAL_DIR] IS NOT NULL
		   AND [SUCURSAL_MAIL] IS NOT NULL
		   AND [SUCURSAL_TELEFONO] IS NOT NULL
		 GROUP BY [SUCURSAL_DIR]
		         ,[SUCURSAL_MAIL]
				 ,[SUCURSAL_TELEFONO]
	);
	PRINT CAST(SYSDATETIME() AS VARCHAR(25))+' Sucursales insertadas correctamente';
GO

/* *** MIGRAR FACTURAS *** */

CREATE PROCEDURE DATASCIENTISTS.InsertarFactura
@facturaNro   DECIMAL(18,0),
@facturaFecha DATETIME,
@clienteID    DECIMAL(18,0),
@sucursalID   DECIMAL(18,0)
AS
	INSERT INTO DATASCIENTISTS.FACTURA(FACT_NUMERO, FACT_FECHA, FACT_CLIENTE, FACT_SUCURSAL)
		 VALUES (@facturaNro, @facturaFecha, @clienteID, @sucursalID);
GO

-- ------ --

CREATE FUNCTION DATASCIENTISTS.ObtenerSucursal(@sucursalNum int)
RETURNS DECIMAL(18,0)
BEGIN
	RETURN (SELECT SUCURSAL_ID
	          FROM DATASCIENTISTS.SUCURSAL
		     WHERE SUCURSAL_TELEFONO = @sucursalNum);
END
GO

-- ------ --

CREATE FUNCTION DATASCIENTISTS.ObtenerCliente (@clienteDNI DECIMAL(18,0), @clienteNombre NVARCHAR(255), @clienteApellido NVARCHAR(255))
RETURNS DECIMAL(18,0)
BEGIN
	RETURN (SELECT CLIENTE_ID
	          FROM DATASCIENTISTS.CLIENTE
		     WHERE CLIENTE_DNI = @clienteDNI
			   AND CLIENTE_NOMBRE = @clienteNombre
			   AND CLIENTE_APELLIDO = @clienteApellido
		    );
END
GO

-- ------ --

CREATE PROCEDURE DATASCIENTISTS.MigracionInsertarFacturasItems
AS

	DECLARE @facturaNro DECIMAL(18,0), @facturaFecha DATETIME, @pasajeCod DECIMAL(18,0), @estadiaCod DECIMAL(18,0), @sucursalID DECIMAL(18,0), @clienteID DECIMAL(18,0);

	DECLARE C_Fact_Item_Pasaje CURSOR FOR (
		SELECT FACTURA_NRO,
		       FACTURA_FECHA,
		       PASAJE_CODIGO,
		       DATASCIENTISTS.ObtenerCliente(CLIENTE_DNI, CLIENTE_NOMBRE, CLIENTE_APELLIDO) AS CLIENTE_ID,
			   DATASCIENTISTS.ObtenerSucursal(SUCURSAL_TELEFONO) AS SUCURSAL_ID
		 FROM [GD1C2020].[gd_esquema].[Maestra]
		  WHERE FACTURA_FECHA IS NOT NULL
			AND FACTURA_NRO IS NOT NULL
			AND PASAJE_CODIGO IS NOT NULL
			);
			
	DECLARE C_Fact_Item_Estadia CURSOR FOR (
		SELECT FACTURA_NRO,
		       FACTURA_FECHA,
		       ESTADIA_CODIGO,
			   DATASCIENTISTS.ObtenerCliente(CLIENTE_DNI, CLIENTE_NOMBRE, CLIENTE_APELLIDO) AS CLIENTE_ID,
			   DATASCIENTISTS.ObtenerSucursal(SUCURSAL_DIR) AS SUCURSAL_ID
		 FROM [GD1C2020].[gd_esquema].[Maestra]
		  WHERE FACTURA_FECHA IS NOT NULL
			AND FACTURA_NRO IS NOT NULL
			AND ESTADIA_CODIGO IS NOT NULL
			);			
	
	/* RECORRE facturas con item pasaje */
	OPEN C_Fact_Item_Pasaje;
	FETCH FROM C_Fact_Item_Pasaje INTO @facturaNro, @facturaFecha, @pasajeCod, @clienteID, @sucursalID;
	WHILE @@FETCH_STATUS = 0
	BEGIN
		-- INSERTAR factura
		EXECUTE DATASCIENTISTS.InsertarFactura @facturaNro, @facturaFecha, @clienteID, @sucursalID;
		
		-- INSERTAR item pasaje
		INSERT INTO DATASCIENTISTS.ITEMS_PASAJE(ITEM_PAS_COD_PASAJE, ITEM_PAS_FACT_NUMERO)
		     VALUES (@pasajeCod, @facturaNro);
		
		--FETCH
		FETCH FROM C_Fact_Item_Pasaje INTO @facturaNro, @facturaFecha, @pasajeCod, @clienteID, @sucursalID;

	END
	CLOSE C_Fact_Item_Pasaje;
	DEALLOCATE C_Fact_Item_Pasaje;
	
	PRINT CAST(SYSDATETIME() AS VARCHAR(25))+' Facturas con items pasaje insertados correctamente';
	
	/* Recorre facutas con item estadia */
	OPEN C_Fact_Item_Estadia;
	FETCH FROM C_Fact_Item_Estadia INTO @facturaNro, @facturaFecha, @estadiaCod, @clienteID, @sucursalID;
	WHILE @@FETCH_STATUS = 0
	BEGIN
		-- INSERTAR factura
		EXECUTE DATASCIENTISTS.InsertarFactura @facturaNro, @facturaFecha, @clienteID, @sucursalID;
		
		-- INSERTAR item estadia
		INSERT INTO DATASCIENTISTS.ITEMS_ESTADIA(ITEM_EST_COD_ESTADIA, ITEM_EST_FACT_NUMERO)
		     VALUES (@estadiaCod, @facturaNro);

		--FETCH
		FETCH FROM C_Fact_Item_Estadia INTO @facturaNro, @facturaFecha, @estadiaCod, @clienteID, @sucursalID;

	END
	CLOSE C_Fact_Item_Estadia;
	DEALLOCATE C_Fact_Item_Estadia;	
	
	PRINT CAST(SYSDATETIME() AS VARCHAR(25))+' Facturas con items estadía insertados correctamente';
GO

/* *** INVOCACIÓN *** */
-- TODO BP: Colocarlo en la invocación general --
/*
EXEC DATASCIENTISTS.MigracionInsertarClientes
GO

EXEC DATASCIENTISTS.MigracionInsertarSucursales
GO

EXEC DATASCIENTISTS.MigracionInsertarFacturasItems
GO
*/