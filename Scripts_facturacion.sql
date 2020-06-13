/* *** MIGRACIÓN DE MODELO DE FACTURAS *** */
USE [GD1C2020]; -- TODO BP: Eliminar

-- TODO BP: COLOCARLO JUNTO A LOS DEMÁS DROPS --
-- PROCEDURE --
if object_id('DATASCIENTISTS.MigracionInsertarSucursales') is not null
	DROP PROCEDURE DATASCIENTISTS.MigracionInsertarSucursales;
GO

if object_id('DATASCIENTISTS.InsertarCliente') is not null
	DROP PROCEDURE DATASCIENTISTS.InsertarCliente;
GO

if object_id('DATASCIENTISTS.InsertarFactura') is not null
	DROP PROCEDURE DATASCIENTISTS.InsertarFactura;
GO

if object_id('DATASCIENTISTS.MigracionInsertarFacturasItems') is not null
	DROP PROCEDURE DATASCIENTISTS.MigracionInsertarFacturasItems;
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
CREATE PROCEDURE DATASCIENTISTS.InsertarCliente
@clienteDNI DECIMAL(18, 0),
@clienteNombre nvarchar(255),
@clienteApellido nvarchar(255),
@clienteFechaNac datetime,
@clienteMail nvarchar(255),
@clienteTel int,
@clienteID DECIMAL(18,0) OUTPUT
AS
BEGIN
	INSERT [DATASCIENTISTS].CLIENTE
		(CLIENTE_DNI,
		 CLIENTE_NOMBRE,
		 CLIENTE_APELLIDO,
		 CLIENTE_FECHA_NAC,
		 CLIENTE_MAIL,
		 CLIENTE_TELEFONO)
	VALUES(
		@clienteDNI,
		@clienteNombre,
		@clienteApellido,
		@clienteFechaNac,
		@clienteMail,
		@clienteTel
	);
	
	-- Retorna ClienteID  --
	SET @clienteID = Scope_Identity();
	
	PRINT CAST(SYSDATETIME() AS VARCHAR(25))+' Clientes insertados correctamente';
	
	RETURN @clienteID;
END
GO

-- ------- --

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

CREATE PROCEDURE DATASCIENTISTS.MigracionInsertarFacturasItems
AS

	DECLARE @facturaNro DECIMAL(18,0), @facturaFecha DATETIME, @pasajeCod DECIMAL(18,0), @estadiaCod DECIMAL(18,0), @sucursalID DECIMAL(18,0);
	DECLARE @clienteID DECIMAL(18,0), @clienteDNI DECIMAL(18, 0), @clienteNombre nvarchar(255), @clienteApellido nvarchar(255), @clienteFechaNac datetime, @clienteMail nvarchar(255), @clienteTel int;

	DECLARE C_Fact_Item_Pasaje CURSOR FOR (
		SELECT FACTURA_NRO,
		       FACTURA_FECHA,
		       PASAJE_CODIGO,
			   -- Cliente --
		       CLIENTE_DNI, CLIENTE_NOMBRE, CLIENTE_APELLIDO, CLIENTE_FECHA_NAC, CLIENTE_MAIL, CLIENTE_TELEFONO,
			   -- Sucursal --
			   SUCURSAL_ID SUCURSAL_ID
		 FROM [gd_esquema].[Maestra] maestra
		INNER JOIN DATASCIENTISTS.SUCURSAL sucursal ON sucursal.SUCURSAL_TELEFONO = maestra.SUCURSAL_TELEFONO
		WHERE FACTURA_FECHA IS NOT NULL
		  AND FACTURA_NRO IS NOT NULL
		  AND PASAJE_CODIGO IS NOT NULL
		);
			
	DECLARE C_Fact_Item_Estadia CURSOR FOR (
		SELECT FACTURA_NRO,
		       FACTURA_FECHA,
		       ESTADIA_CODIGO,
			   -- Cliente --
		       CLIENTE_DNI, CLIENTE_NOMBRE, CLIENTE_APELLIDO, CLIENTE_FECHA_NAC, CLIENTE_MAIL, CLIENTE_TELEFONO,
			   -- Sucursal --
			   SUCURSAL_ID SUCURSAL_ID
		 FROM [GD1C2020].[gd_esquema].[Maestra] maestra
		INNER JOIN DATASCIENTISTS.SUCURSAL sucursal ON sucursal.SUCURSAL_TELEFONO = maestra.SUCURSAL_TELEFONO
		  WHERE FACTURA_FECHA IS NOT NULL
			AND FACTURA_NRO IS NOT NULL
			AND ESTADIA_CODIGO IS NOT NULL
		);			
	
	/* RECORRE facturas con item pasaje */
	OPEN C_Fact_Item_Pasaje;
	FETCH FROM C_Fact_Item_Pasaje INTO @facturaNro, @facturaFecha, @pasajeCod, @clienteDNI, @clienteNombre, @clienteApellido, @clienteFechaNac, @clienteMail, @clienteTel, @sucursalID;
	WHILE @@FETCH_STATUS = 0
	BEGIN
	
		-- INSERTAR Cliente --
		EXECUTE DATASCIENTISTS.InsertarCliente @clienteDNI, @clienteNombre, @clienteApellido, @clienteFechaNac, @clienteMail, @clienteTel, @clienteID OUTPUT;

		-- INSERTAR Factura --
		EXECUTE DATASCIENTISTS.InsertarFactura @facturaNro, @facturaFecha, @clienteID, @sucursalID;
		
		-- INSERTAR Item pasaje --
		INSERT INTO DATASCIENTISTS.ITEMS_PASAJE(ITEM_PAS_COD_PASAJE, ITEM_PAS_FACT_NUMERO)
		     VALUES (@pasajeCod, @facturaNro);
		
		--FETCH
		FETCH FROM C_Fact_Item_Pasaje INTO @facturaNro, @facturaFecha, @pasajeCod, @clienteDNI, @clienteNombre, @clienteApellido, @clienteFechaNac, @clienteMail, @clienteTel, @sucursalID;

	END
	CLOSE C_Fact_Item_Pasaje;
	DEALLOCATE C_Fact_Item_Pasaje;
	
	PRINT CAST(SYSDATETIME() AS VARCHAR(25))+' Facturas con items pasaje insertados correctamente';
	
	/* Recorre facutas con item estadia */
	OPEN C_Fact_Item_Estadia;
	FETCH FROM C_Fact_Item_Estadia INTO @facturaNro, @facturaFecha, @estadiaCod, @clienteDNI, @clienteNombre, @clienteApellido, @clienteFechaNac, @clienteMail, @clienteTel, @sucursalID;
	WHILE @@FETCH_STATUS = 0
	BEGIN
	
		-- INSERTAR Cliente --
		EXECUTE DATASCIENTISTS.InsertarCliente @clienteDNI, @clienteNombre, @clienteApellido, @clienteFechaNac, @clienteMail, @clienteTel, @clienteID OUTPUT;
	
		-- INSERTAR factura
		EXECUTE DATASCIENTISTS.InsertarFactura @facturaNro, @facturaFecha, @clienteID, @sucursalID;
		
		-- INSERTAR item estadia
		INSERT INTO DATASCIENTISTS.ITEMS_ESTADIA(ITEM_EST_COD_ESTADIA, ITEM_EST_FACT_NUMERO)
		     VALUES (@estadiaCod, @facturaNro);

		--FETCH
		FETCH FROM C_Fact_Item_Estadia INTO @facturaNro, @facturaFecha, @estadiaCod, @clienteDNI, @clienteNombre, @clienteApellido, @clienteFechaNac, @clienteMail, @clienteTel, @sucursalID;

	END
	CLOSE C_Fact_Item_Estadia;
	DEALLOCATE C_Fact_Item_Estadia;	
	
	PRINT CAST(SYSDATETIME() AS VARCHAR(25))+' Facturas con items estadía insertados correctamente';
GO

/* *** INVOCACIÓN *** */
-- TODO BP: Colocarlo en la invocación general --
EXEC DATASCIENTISTS.MigracionInsertarSucursales
GO

EXEC DATASCIENTISTS.MigracionInsertarFacturasItems
GO
