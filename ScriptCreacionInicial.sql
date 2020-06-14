USE [GD1C2020];

GO
--BORRAR
--CURSORES
if object_id('MigracionHotelHabitacion') is not null
	DEALLOCATE MigracionHotelHabitacion;
GO
if object_id('MigracionEstadiaFactura') is not null
	DEALLOCATE MigracionEstadiaFactura;
GO

--FUNCIONES
if object_id('DATASCIENTISTS.ObtenerUltimoIdButaca') is not null
	DROP FUNCTION DATASCIENTISTS.ObtenerUltimoIdButaca;
GO
if object_id('DATASCIENTISTS.ObtenerCodigoCiudad') is not null
	DROP FUNCTION DATASCIENTISTS.ObtenerCodigoCiudad;
GO
if object_id('DATASCIENTISTS.ObtenerIdEmpresa') is not null
	DROP FUNCTION DATASCIENTISTS.ObtenerIdEmpresa;
GO
if object_id('DATASCIENTISTS.ObtenerIDHabitacion') is not null
	DROP FUNCTION DATASCIENTISTS.ObtenerIDHabitacion;
GO


--STORED PROCEDURES
if object_id('DATASCIENTISTS.MigracionPasajes') is not null
	DROP PROCEDURE DATASCIENTISTS.MigracionPasajes;
GO
if object_id('DATASCIENTISTS.MigracionEstadias') is not null
	DROP PROCEDURE DATASCIENTISTS.MigracionEstadias;
GO
if object_id('DATASCIENTISTS.MigracionCompras') is not null
	DROP PROCEDURE DATASCIENTISTS.MigracionCompras;
GO
if object_id('DATASCIENTISTS.MigracionEmpresas') is not null
	DROP PROCEDURE DATASCIENTISTS.MigracionEmpresas;
GO
if object_id('DATASCIENTISTS.MigracionInsertarPasajes') is not null
	DROP PROCEDURE DATASCIENTISTS.MigracionInsertarPasajes;
GO
if object_id('DATASCIENTISTS.InsertarPasaje') is not null
	DROP PROCEDURE DATASCIENTISTS.InsertarPasaje;
GO
if object_id('DATASCIENTISTS.BuscarEInsertarButaca') is not null
	DROP PROCEDURE DATASCIENTISTS.BuscarEInsertarButaca;
GO
if object_id('DATASCIENTISTS.MigracionInsertarAviones') is not null
	DROP PROCEDURE DATASCIENTISTS.MigracionInsertarAviones;
GO
if object_id('DATASCIENTISTS.MigracionInsertarVuelos') is not null
	DROP PROCEDURE DATASCIENTISTS.MigracionInsertarVuelos;
GO
if object_id('DATASCIENTISTS.MigracionInsertarRutasAereas') is not null
	DROP PROCEDURE DATASCIENTISTS.MigracionInsertarRutasAereas;
GO
if object_id('DATASCIENTISTS.MigracionInsertarCiudades') is not null
	DROP PROCEDURE DATASCIENTISTS.MigracionInsertarCiudades;
GO
if object_id('DATASCIENTISTS.MigracionTipoHabitaciones') is not null
	DROP PROCEDURE DATASCIENTISTS.MigracionTipoHabitaciones;
GO
if object_id('DATASCIENTISTS.MigracionInsertarHotel') is not null
	DROP PROCEDURE DATASCIENTISTS.MigracionInsertarHotel;
GO
if object_id('DATASCIENTISTS.MigracionInsertarHabitacion') is not null
	DROP PROCEDURE DATASCIENTISTS.MigracionInsertarHabitacion;
GO
if object_id('DATASCIENTISTS.MigracionHabitacionesHoteles') is not null
	DROP PROCEDURE DATASCIENTISTS.MigracionHabitacionesHoteles;
GO
if object_id('DATASCIENTISTS.InsertarEstadias') is not null
	DROP PROCEDURE DATASCIENTISTS.InsertarEstadias;
GO
if object_id('DATASCIENTISTS.InsertarItemsEstadias') is not null
	DROP PROCEDURE DATASCIENTISTS.InsertarItemsEstadias;
GO
if object_id('DATASCIENTISTS.MigracionEstadias') is not null
	DROP PROCEDURE DATASCIENTISTS.MigracionEstadias;
GO
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

 --TABLAS
if object_id('DATASCIENTISTS.ITEMS_PASAJE') is not null
	DROP TABLE [DATASCIENTISTS].ITEMS_PASAJE;

if object_id('DATASCIENTISTS.ITEMS_ESTADIA') is not null
	DROP TABLE [DATASCIENTISTS].ITEMS_ESTADIA;

if object_id('DATASCIENTISTS.FACTURA') is not null
	DROP TABLE [DATASCIENTISTS].FACTURA;

if object_id('DATASCIENTISTS.SUCURSAL') is not null
	DROP TABLE [DATASCIENTISTS].SUCURSAL;

if object_id('DATASCIENTISTS.CLIENTE') is not null
	DROP TABLE [DATASCIENTISTS].CLIENTE;

if object_id('DATASCIENTISTS.PASAJE') is not null
	DROP TABLE [DATASCIENTISTS].PASAJE;

if object_id('DATASCIENTISTS.BUTACA') is not null
	DROP TABLE [DATASCIENTISTS].BUTACA;

if object_id('DATASCIENTISTS.VUELO') is not null
	DROP TABLE [DATASCIENTISTS].VUELO;

if object_id('DATASCIENTISTS.RUTA_AEREA') is not null
	DROP TABLE [DATASCIENTISTS].RUTA_AEREA;

if object_id('DATASCIENTISTS.CIUDADES') is not null
	DROP TABLE [DATASCIENTISTS].CIUDADES;

if object_id('DATASCIENTISTS.ESTADIA') is not null
	DROP TABLE [DATASCIENTISTS].ESTADIA;

if object_id('DATASCIENTISTS.HABITACION') is not null
	DROP TABLE [DATASCIENTISTS].[HABITACION];

if object_id('DATASCIENTISTS.TIPO_HABITACION') is not null
	DROP TABLE [DATASCIENTISTS].[TIPO_HABITACION];

if object_id('DATASCIENTISTS.HOTEL') is not null
	DROP TABLE [DATASCIENTISTS].[HOTEL];

if object_id('DATASCIENTISTS.AVION') is not null
	DROP TABLE [DATASCIENTISTS].[AVION];

if object_id('DATASCIENTISTS.COMPRA') is not null
	DROP TABLE [DATASCIENTISTS].[COMPRA];

if object_id('DATASCIENTISTS.EMPRESA') is not null
	DROP TABLE [DATASCIENTISTS].[EMPRESA];
 
 --ESQUEMA
if SCHEMA_ID('DATASCIENTISTS') is not null
	DROP SCHEMA DATASCIENTISTS;

PRINT CAST(SYSDATETIME() AS VARCHAR(25))+' Modelo de datos anterior, borrado correctamente';
GO

CREATE SCHEMA [DATASCIENTISTS];

GO


CREATE TABLE [DATASCIENTISTS].[EMPRESA]
(
	[EMPRESA_ID] decimal(18,0) IDENTITY(1,1) NOT NULL,
	[EMPRESA_RAZON_SOCIAL] nvarchar(255),
	CONSTRAINT PK_EMPRESA PRIMARY KEY(EMPRESA_ID)
);

CREATE TABLE [DATASCIENTISTS].[COMPRA]
(
	[COMPRA_NUMERO] decimal(18,0) NOT NULL,
	[COMPRA_FECHA] datetime2(3),
	[COMPRA_EMPRESA] decimal(18,0) NOT NULL,
	CONSTRAINT PK_COMPRA PRIMARY KEY(COMPRA_NUMERO),
	CONSTRAINT FK_EMPRESA_COMPRA FOREIGN KEY(COMPRA_EMPRESA)
		REFERENCES [DATASCIENTISTS].EMPRESA(EMPRESA_ID)
);

CREATE TABLE [DATASCIENTISTS].[AVION]
(
	[AVION_IDENTIFICADOR] nvarchar(50) NOT NULL,
	[AVION_MODELO] nvarchar(50),
	CONSTRAINT PK_AVION PRIMARY KEY(AVION_IDENTIFICADOR)
);

CREATE TABLE [DATASCIENTISTS].[HOTEL]
(
	[HOTEL_ID] decimal(18,0) IDENTITY(1,1) NOT NULL,
	[HOTEL_CALLE] nvarchar(50),
	[HOTEL_NUMERO_CALLE] int,					--(Y)Pongo un tipo de dato mas chico, no hay alturas de calle de un órden de millones
	[HOTEL_CANTIDAD_ESTRELLAS] tinyint,			--(Y)Pongo un tipo de dato mas chico, solo va a ser 1 número de estrellas, que les parece?
	CONSTRAINT PK_HOTEL PRIMARY KEY(HOTEL_ID),
	CHECK (HOTEL_CANTIDAD_ESTRELLAS IN (1,2,3,4,5)) --(Y)agrego un check, que piensan? 
);

CREATE TABLE [DATASCIENTISTS].[TIPO_HABITACION]
(
	[TIPO_HABITACION_CODIGO] decimal(18,0) NOT NULL,
	[TIPO_HABITACION_DESCRIPCION] nvarchar(50),
	CONSTRAINT PK_TIPO_HABITACION PRIMARY KEY(TIPO_HABITACION_CODIGO)
);

CREATE TABLE [DATASCIENTISTS].[HABITACION]
(
	[HABITACION_ID] decimal(18,0) IDENTITY(1,1) NOT NULL,
	[HABIT_HOTEL_ID] decimal(18,0) NOT NULL,
	[HABIT_TIPO_COD] decimal(18,0) NOT NULL,
	[HABIT_NUMERO] decimal(18,0),
	[HABIT_PISO] int,					--(Y)Pongo un tipo de dato mas chico, no hay pisos de un órden de millones
	[HABIT_FRENTE] nvarchar(50),
	[HABIT_COSTO] decimal(18,2),
	[HABIT_PRECIO] decimal(18,2),
	CONSTRAINT PK_HABITACION PRIMARY KEY(HABITACION_ID),
	CONSTRAINT FK_HOTEL_HABITACION FOREIGN KEY(HABIT_HOTEL_ID)
		REFERENCES [DATASCIENTISTS].HOTEL(HOTEL_ID),
	CONSTRAINT FK_TIPO_HABITACION FOREIGN KEY(HABIT_TIPO_COD)
		REFERENCES [DATASCIENTISTS].TIPO_HABITACION(TIPO_HABITACION_CODIGO)
);

CREATE TABLE [DATASCIENTISTS].ESTADIA
(
	ESTADIA_CODIGO decimal(18,0) NOT NULL,
	ESTADIA_COMPRA decimal(18,0) NOT NULL,
	ESTADIA_HABITACION decimal(18,0) NOT NULL,
	ESTADIA_FECHA_INI datetime2(3),
	ESTADIA_CANTIDAD_NOCHES int,					--(Y)Pongo un tipo de dato mas chico, un int alcanza
	CONSTRAINT PK_ESTADIA PRIMARY KEY(ESTADIA_CODIGO),
	CONSTRAINT FK_COMPRA_ESTADIA_COMPRA FOREIGN KEY(ESTADIA_COMPRA)
		REFERENCES [DATASCIENTISTS].COMPRA(COMPRA_NUMERO),
	CONSTRAINT FK_HABITACION_ESTADIA FOREIGN KEY(ESTADIA_HABITACION)
		REFERENCES [DATASCIENTISTS].HABITACION(HABITACION_ID)
)

CREATE TABLE [DATASCIENTISTS].CIUDADES
(
	CIUDAD_ID decimal(18,0) IDENTITY(1,1) NOT NULL,
	CIUDAD_NOMBRE nvarchar(255),
	CONSTRAINT PK_CIUDADES PRIMARY KEY(CIUDAD_ID)
);

/*
Para solucionar el problema de que hay más de un origen y destino para el mismo código,
modificamos la PK_RUTA_AEREA haciéndola compuesta para tener origen y destino, además del código, eliminando la multiplicidad.
*/
CREATE TABLE [DATASCIENTISTS].RUTA_AEREA
(
	RUTA_AEREA_CODIGO decimal(18,0) NOT NULL,
	RUTA_AEREA_CIU_ORIG decimal(18,0) NOT NULL,
	RUTA_AEREA_CIU_DEST decimal(18,0) NOT NULL,
	CONSTRAINT PK_RUTA_AEREA PRIMARY KEY(RUTA_AEREA_CODIGO, RUTA_AEREA_CIU_ORIG, RUTA_AEREA_CIU_DEST),
	CONSTRAINT FK_CIU_ORIG_RUTA_AEREA FOREIGN KEY(RUTA_AEREA_CIU_ORIG)
		REFERENCES [DATASCIENTISTS].CIUDADES(CIUDAD_ID),
	CONSTRAINT FK_CIU_DEST_RUTA_AEREA FOREIGN KEY(RUTA_AEREA_CIU_DEST)
		REFERENCES [DATASCIENTISTS].CIUDADES(CIUDAD_ID)
)

/*
Debido a que en la ruta aérea habían problemas de múltiples origen y destinos para un código, se hizo una clave múltiple y,
por ende, se agregan columnas faltantes al vuelo para representar la FK con una ruta aérea.
*/
CREATE TABLE [DATASCIENTISTS].VUELO
(
	VUELO_CODIGO decimal(18,0) NOT NULL,
	VUELO_FECHA_SALIDA datetime2(3),
	VUELO_FECHA_LLEGADA datetime2(3),
	VUELO_RUTA_AEREA_COD decimal(18,0) NOT NULL,
	VUELO_RUTA_AEREA_ORIG decimal(18,0) NOT NULL,
	VUELO_RUTA_AEREA_DEST decimal(18,0) NOT NULL,
	CONSTRAINT PK_VUELO PRIMARY KEY(VUELO_CODIGO),
	CONSTRAINT FK_RUTA_AEREA_VUELO FOREIGN KEY(VUELO_RUTA_AEREA_COD, VUELO_RUTA_AEREA_ORIG, VUELO_RUTA_AEREA_DEST)
		REFERENCES [DATASCIENTISTS].RUTA_AEREA(RUTA_AEREA_CODIGO, RUTA_AEREA_CIU_ORIG, RUTA_AEREA_CIU_DEST)
)

CREATE TABLE [DATASCIENTISTS].BUTACA
(
	BUTACA_ID decimal(18,0) IDENTITY(1,1) NOT NULL,
	BUTACA_NUMERO int,					--(Y)Pongo un tipo de dato mas chico, no hay aviones con un órden de millones para butacas
	BUTACA_TIPO nvarchar(50),			--(Y)Pongo un tipo de dato mas chico, los tipos de butaca no necesitan 255 caractéres
	BUTACA_AVION nvarchar(50) NOT NULL,
	CONSTRAINT PK_BUTACA PRIMARY KEY(BUTACA_ID),
	CONSTRAINT FK_AVION_BUTACA FOREIGN KEY(BUTACA_AVION)
		REFERENCES [DATASCIENTISTS].AVION(AVION_IDENTIFICADOR),
	/*Un avión no puede tener más de una butaca del mismo tipo y con el mismo número. Sería información redundante*/
	CONSTRAINT UC_BUTACA UNIQUE (BUTACA_NUMERO, BUTACA_TIPO, BUTACA_AVION)
)

CREATE TABLE [DATASCIENTISTS].PASAJE
(
	PASAJE_CODIGO decimal(18,0) NOT NULL,
	PASAJE_COSTO decimal(18,2),
	PASAJE_PRECIO decimal(18,2),
	PASAJE_VUELO decimal(18,0) NOT NULL,
	PASAJE_BUTACA decimal(18,0) NOT NULL,
	PASAJE_COMPRA decimal(18,0) NOT NULL,
	CONSTRAINT PK_PASAJE PRIMARY KEY(PASAJE_CODIGO),
	CONSTRAINT FK_VUELO_PASAJE FOREIGN KEY(PASAJE_VUELO)
		REFERENCES [DATASCIENTISTS].VUELO(VUELO_CODIGO),
	CONSTRAINT FK_BUTACA_PASAJE FOREIGN KEY(PASAJE_BUTACA)
		REFERENCES [DATASCIENTISTS].BUTACA(BUTACA_ID),
	CONSTRAINT FK_COMPRA_PASAJE FOREIGN KEY(PASAJE_COMPRA)
		REFERENCES [DATASCIENTISTS].COMPRA(COMPRA_NUMERO)
)

CREATE TABLE [DATASCIENTISTS].[CLIENTE]
(
	CLIENTE_ID decimal(18,0) IDENTITY(1,1) NOT NULL,
	CLIENTE_DNI decimal(18,0) NOT NULL,
	CLIENTE_NOMBRE nvarchar(255),
	CLIENTE_APELLIDO nvarchar(255),
	CLIENTE_FECHA_NAC datetime,
	CLIENTE_MAIL nvarchar(255),
	CLIENTE_TELEFONO int,
	CONSTRAINT PK_CLIENTE PRIMARY KEY (CLIENTE_ID)
);

CREATE TABLE [DATASCIENTISTS].[SUCURSAL]
(
	SUCURSAL_ID decimal(18,0) IDENTITY(1,1) NOT NULL,
	SUCURSAL_DIR nvarchar(255),
	SUCURSAL_MAIL nvarchar(255),
	SUCURSAL_TELEFONO int,
	CONSTRAINT PK_SUCURSAL PRIMARY KEY (SUCURSAL_ID)
);

CREATE TABLE [DATASCIENTISTS].[FACTURA]
(
	FACT_NUMERO decimal(18,0) NOT NULL,
	FACT_FECHA datetime,
	FACT_CLIENTE decimal(18,0) NOT NULL,
	FACT_SUCURSAL decimal(18,0) NOT NULL,
	CONSTRAINT PK_FACTURA PRIMARY KEY (FACT_NUMERO),
	CONSTRAINT FK_CLIENTE_FACTURA FOREIGN KEY(FACT_CLIENTE)
		REFERENCES [DATASCIENTISTS].CLIENTE(CLIENTE_ID),
	CONSTRAINT FK_SUCURSAL_FACTURA FOREIGN KEY(FACT_SUCURSAL)
		REFERENCES [DATASCIENTISTS].SUCURSAL(SUCURSAL_ID)
);

CREATE TABLE [DATASCIENTISTS].[ITEMS_PASAJE]
(
	ITEM_PASAJE_ID decimal(18,0) IDENTITY(1,1) NOT NULL,
	ITEM_PAS_COD_PASAJE decimal(18,0) NOT NULL,
	ITEM_PAS_FACT_NUMERO decimal(18,0) NOT NULL,
	CONSTRAINT PK_ITEM_PASAJE PRIMARY KEY(ITEM_PASAJE_ID),
	CONSTRAINT FK_PAS_PASAJE FOREIGN KEY(ITEM_PAS_COD_PASAJE)
		REFERENCES [DATASCIENTISTS].PASAJE(PASAJE_CODIGO),
	CONSTRAINT FK_PAS_FACTURA FOREIGN KEY(ITEM_PAS_FACT_NUMERO)
		REFERENCES [DATASCIENTISTS].FACTURA(FACT_NUMERO)	
);

CREATE TABLE [DATASCIENTISTS].[ITEMS_ESTADIA]
(
	ITEM_ESTADIA_ID decimal(18,0) IDENTITY(1,1) NOT NULL,
	ITEM_EST_COD_ESTADIA decimal(18,0) NOT NULL,
	ITEM_EST_FACT_NUMERO decimal(18,0) NOT NULL,
	CONSTRAINT PK_ITEM_ESTADIA PRIMARY KEY(ITEM_ESTADIA_ID),
	CONSTRAINT FK_EST_ESTADIA FOREIGN KEY(ITEM_EST_COD_ESTADIA)
		REFERENCES [DATASCIENTISTS].ESTADIA(ESTADIA_CODIGO),
	CONSTRAINT FK_EST_FACTURA FOREIGN KEY(ITEM_EST_FACT_NUMERO)
		REFERENCES [DATASCIENTISTS].FACTURA(FACT_NUMERO)	
);

PRINT CAST(SYSDATETIME() AS VARCHAR(25))+' Modelo de datos creado correctamente';
GO

--CURSORES
DECLARE MigracionHotelHabitacion Cursor
FOR
	SELECT HOTEL_CALLE, HOTEL_NRO_CALLE, HOTEL_CANTIDAD_ESTRELLAS, HABITACION_PISO, HABITACION_NUMERO,HABITACION_FRENTE, HABITACION_COSTO, HABITACION_PRECIO, TIPO_HABITACION_CODIGO
	FROM gd_esquema.Maestra
	WHERE HOTEL_CALLE is not null
	GROUP BY HOTEL_CALLE, HOTEL_NRO_CALLE, HOTEL_CANTIDAD_ESTRELLAS, HABITACION_PISO, HABITACION_NUMERO,HABITACION_FRENTE, HABITACION_COSTO, HABITACION_PRECIO, TIPO_HABITACION_CODIGO
	ORDER BY 1, 2, 4, 5

GO
--DADO A QUE ACTUALMENTE TODAS LAS ESTADIAS TIENEN FACTURA ASOCIADA HAGO ESTE SOLO CURSOR CON 1 SELECT
DECLARE MigracionEstadiaFactura Cursor
For
	SELECT COMPRA_NUMERO, ESTADIA_CANTIDAD_NOCHES, ESTADIA_CODIGO, ESTADIA_FECHA_INI, HOTEL_CALLE, HOTEL_NRO_CALLE, HABITACION_NUMERO, HABITACION_PISO, HABITACION_FRENTE, FACTURA_NRO 
	FROM gd_esquema.Maestra
	WHERE ESTADIA_CODIGO IS NOT NULL 
	AND FACTURA_NRO IS NOT NULL
	ORDER BY ESTADIA_CODIGO

GO
--FUNCIONES

CREATE FUNCTION DATASCIENTISTS.ObtenerIDHabitacion (@CALLE nvarchar(50),@NROCALLE decimal(18,0), @HABITACIONNRO decimal(18,0)
,@PISO int,@FRENTE nvarchar(50))
RETURNS DECIMAL(18,0)
BEGIN
	DECLARE @HOTEL decimal(18,0), @HABITACION decimal(18,0);

	SELECT top 1 @HOTEL = HOTEL_ID FROM DATASCIENTISTS.HOTEL
	WHERE @CALLE = HOTEL_CALLE AND @NROCALLE = HOTEL_NUMERO_CALLE;

	SELECT top 1 @HABITACION = HABITACION_ID FROM DATASCIENTISTS.HABITACION
	WHERE HABIT_HOTEL_ID = @HOTEL AND HABIT_NUMERO = @HABITACIONNRO AND HABIT_PISO = @PISO AND HABIT_FRENTE = @FRENTE;

	RETURN @HABITACION;
END

GO

--STORED PROCEDURES
CREATE PROCEDURE [DATASCIENTISTS].MigracionInsertarHotel (@CALLE nvarchar(50),@NROCALLE decimal(18,0), @ESTRELLAS tinyint)
AS
BEGIN
	INSERT INTO [DATASCIENTISTS].HOTEL (HOTEL_CALLE,HOTEL_NUMERO_CALLE,HOTEL_CANTIDAD_ESTRELLAS)
	VALUES (@CALLE,@NROCALLE,@ESTRELLAS);
	PRINT CAST(SYSDATETIME() AS VARCHAR(25))+ 'nuevo hotel';
END

GO

CREATE PROCEDURE [DATASCIENTISTS].MigracionInsertarHabitacion (@HOTEL decimal(18,0), @PISO int, @NUMERO decimal (18,0), @FRENTE nvarchar(50), @PRECIO decimal(18,2), @COSTO decimal(18,2), @TIPO decimal(18,0))
AS
BEGIN
	INSERT INTO [DATASCIENTISTS].HABITACION (HABIT_HOTEL_ID, HABIT_PISO, HABIT_NUMERO, HABIT_FRENTE, HABIT_PRECIO, HABIT_COSTO, HABIT_TIPO_COD)
	VALUES (@HOTEL, @PISO, @NUMERO, @FRENTE, @PRECIO, @COSTO, @TIPO);
	PRINT CAST(SYSDATETIME() AS VARCHAR(25))+ 'nueva habitacion';
END
	
GO

CREATE PROCEDURE [DATASCIENTISTS].MigracionHabitacionesHoteles
AS
BEGIN
	OPEN MigracionHotelHabitacion;
	DECLARE @CALLE nvarchar(50), @CALLENRO decimal(18,0), @ESTRELLAS tinyint, @PISO int, @HABITACIONNRO decimal(18,0);
	DECLARE @FRENTE nvarchar(50), @COSTO decimal(18,2), @PRECIO decimal(18,2), @TIPO decimal(18,0);
	DECLARE @ID decimal(18,0);
	SET @ID = 0;

	FETCH next FROM MigracionHotelHabitacion INTO @CALLE , @CALLENRO, @ESTRELLAS, @PISO, @HABITACIONNRO, @FRENTE, @COSTO, @PRECIO, @TIPO;
	
	WHILE(@@FETCH_STATUS=0)
	BEGIN
		IF(@CALLE IN (SELECT HOTEL_CALLE FROM [DATASCIENTISTS].HOTEL)) 
		BEGIN
			IF(@CALLENRO IN (SELECT HOTEL_NUMERO_CALLE FROM [DATASCIENTISTS].HOTEL WHERE HOTEL_CALLE = @CALLE))
			BEGIN
				EXEC DATASCIENTISTS.MigracionInsertarHabitacion @ID , @PISO, @HABITACIONNRO, @FRENTE, @COSTO, @PRECIO, @TIPO
			END
			ELSE
			BEGIN
				EXEC DATASCIENTISTS.MigracionInsertarHotel @CALLE, @CALLENRO, @ESTRELLAS
				SELECT top 1 @ID = HOTEL_ID FROM [DATASCIENTISTS].HOTEL WHERE HOTEL_CALLE = @CALLE AND HOTEL_NUMERO_CALLE = @CALLENRO
				EXEC DATASCIENTISTS.MigracionInsertarHabitacion @ID , @PISO, @HABITACIONNRO, @FRENTE, @COSTO, @PRECIO, @TIPO
			END
		END
		ELSE
		BEGIN
			EXEC DATASCIENTISTS.MigracionInsertarHotel @CALLE, @CALLENRO, @ESTRELLAS
			SELECT top 1 @ID = HOTEL_ID FROM [DATASCIENTISTS].HOTEL WHERE HOTEL_CALLE = @CALLE AND HOTEL_NUMERO_CALLE = @CALLENRO
			EXEC DATASCIENTISTS.MigracionInsertarHabitacion @ID , @PISO, @HABITACIONNRO, @FRENTE, @COSTO, @PRECIO, @TIPO
		END
		FETCH NEXT FROM MigracionHotelHabitacion INTO @CALLE, @CALLENRO, @ESTRELLAS, @PISO, @HABITACIONNRO, @FRENTE, @COSTO, @PRECIO, @TIPO;
	END

	CLOSE MigracionHotelHabitacion;
	DEALLOCATE MigracionHotelHabitacion;
END

GO
CREATE PROCEDURE [DATASCIENTISTS].MigracionTipoHabitaciones
AS
BEGIN
	INSERT [DATASCIENTISTS].TIPO_HABITACION (TIPO_HABITACION_CODIGO, TIPO_HABITACION_DESCRIPCION)
	(	SELECT DISTINCT TIPO_HABITACION_CODIGO,TIPO_HABITACION_DESC
		FROM gd_esquema.Maestra
		WHERE TIPO_HABITACION_CODIGO is NOT NULL
		GROUP BY TIPO_HABITACION_CODIGO, TIPO_HABITACION_DESC 
	);
	PRINT CAST(SYSDATETIME() AS VARCHAR(25))+ 'tipos de habitaciones insertados correctamente';
END

GO

CREATE FUNCTION DATASCIENTISTS.ObtenerCodigoCiudad
(@nombreCiudad NVARCHAR(255))
RETURNS DECIMAL(18,0)
BEGIN
	RETURN ISNULL((SELECT CIUDAD_ID FROM DATASCIENTISTS.CIUDADES
		WHERE CIUDAD_NOMBRE=@nombreCiudad),0);
END
GO

CREATE FUNCTION DATASCIENTISTS.ObtenerUltimoIdButaca()
RETURNS DECIMAL(18,0)
BEGIN
	RETURN ISNULL((SELECT MAX(BUTACA_ID) FROM DATASCIENTISTS.BUTACA),0);
END
GO

CREATE FUNCTION DATASCIENTISTS.ObtenerIdEmpresa
(@razonSocial NVARCHAR(255))
RETURNS DECIMAL(18,0)
BEGIN
	RETURN ISNULL((SELECT EMPRESA_ID FROM DATASCIENTISTS.EMPRESA
		WHERE EMPRESA_RAZON_SOCIAL=@razonSocial),0);
END
GO

CREATE PROCEDURE DATASCIENTISTS.MigracionInsertarCiudades
AS
	INSERT [DATASCIENTISTS].CIUDADES (CIUDAD_NOMBRE) (
		SELECT DISTINCT RUTA_AEREA_CIU_ORIG
		FROM gd_esquema.Maestra
		WHERE RUTA_AEREA_CIU_ORIG IS NOT NULL
		UNION 
		SELECT DISTINCT RUTA_AEREA_CIU_DEST
		FROM gd_esquema.Maestra
		WHERE RUTA_AEREA_CIU_DEST IS NOT NULL
	);
	PRINT CAST(SYSDATETIME() AS VARCHAR(25))+' Ciudades insertadas correctamente';
GO

CREATE PROCEDURE DATASCIENTISTS.MigracionInsertarRutasAereas
AS
	INSERT [DATASCIENTISTS].RUTA_AEREA (RUTA_AEREA_CODIGO, RUTA_AEREA_CIU_ORIG, RUTA_AEREA_CIU_DEST) (
		SELECT RUTA_AEREA_CODIGO, 
			DATASCIENTISTS.ObtenerCodigoCiudad(RUTA_AEREA_CIU_ORIG), DATASCIENTISTS.ObtenerCodigoCiudad(RUTA_AEREA_CIU_DEST)
		FROM gd_esquema.Maestra
		WHERE PASAJE_CODIGO is not null
		GROUP BY RUTA_AEREA_CODIGO, RUTA_AEREA_CIU_ORIG, RUTA_AEREA_CIU_DEST
	);
	PRINT CAST(SYSDATETIME() AS VARCHAR(25))+' Rutas aereas insertadas correctamente';
GO

CREATE PROCEDURE DATASCIENTISTS.MigracionInsertarVuelos
AS
	INSERT [DATASCIENTISTS].VUELO 
	(VUELO_CODIGO, VUELO_FECHA_SALIDA, VUELO_FECHA_LLEGADA, VUELO_RUTA_AEREA_COD, VUELO_RUTA_AEREA_ORIG, VUELO_RUTA_AEREA_DEST) (
		SELECT VUELO_CODIGO, VUELO_FECHA_SALUDA, VUELO_FECHA_LLEGADA, RUTA_AEREA_CODIGO,
		DATASCIENTISTS.ObtenerCodigoCiudad(RUTA_AEREA_CIU_ORIG), DATASCIENTISTS.ObtenerCodigoCiudad(RUTA_AEREA_CIU_DEST)
		FROM gd_esquema.Maestra
		WHERE PASAJE_CODIGO is not null AND RUTA_AEREA_CODIGO is not null
		GROUP BY VUELO_CODIGO, VUELO_FECHA_SALUDA, VUELO_FECHA_LLEGADA, RUTA_AEREA_CODIGO, RUTA_AEREA_CIU_ORIG, RUTA_AEREA_CIU_DEST
	);
	PRINT CAST(SYSDATETIME() AS VARCHAR(25))+' Vuelos insertados correctamente';
GO

CREATE PROCEDURE DATASCIENTISTS.MigracionInsertarAviones
AS
	INSERT [DATASCIENTISTS].AVION 
	(AVION_IDENTIFICADOR, AVION_MODELO) (
		SELECT DISTINCT AVION_IDENTIFICADOR, AVION_MODELO
		FROM gd_esquema.Maestra
		WHERE AVION_IDENTIFICADOR is not null
	);
	PRINT CAST(SYSDATETIME() AS VARCHAR(25))+' Aviones insertados correctamente';
GO

CREATE PROCEDURE DATASCIENTISTS.BuscarEInsertarButaca
@butacaNumero INT, 
@butacaTipo  NVARCHAR(50), 
@butacaAvion NVARCHAR(50),
@butacaId DECIMAL(18,0) OUTPUT
AS
	--Si existe la butaca en cuestión, obtener el id de la butaca existente y retornarlo
	SET @butacaId = ISNULL((
			SELECT BUTACA_ID FROM DATASCIENTISTS.BUTACA 
			WHERE BUTACA_AVION=@butacaAvion AND BUTACA_TIPO=@butacaTipo AND BUTACA_NUMERO=@butacaNumero),0);
	IF @butacaId = 0
		BEGIN
			--Si no existe la butaca, insertarla
			INSERT [DATASCIENTISTS].BUTACA 
			(BUTACA_NUMERO, BUTACA_TIPO, BUTACA_AVION) VALUES (
				@butacaNumero,
				@butacaTipo,
				@butacaAvion 
			);
			--Y como se insertó recién, obtener el último id con DATASCIENTISTS.ObtenerUltimoIdButaca
			SET @butacaId = DATASCIENTISTS.ObtenerUltimoIdButaca();
		END
	
	RETURN @butacaId;
GO


CREATE PROCEDURE DATASCIENTISTS.InsertarPasaje
@pasajeCodigo DECIMAL(18,0),
@pasajeCosto  DECIMAL(18,0),
@pasajePrecio DECIMAL(18,0),
@pasajeVuelo  DECIMAL(18,0),
@pasajeButaca DECIMAL(18,0),
@pasajeCompra DECIMAL(18,0)
AS
	INSERT [DATASCIENTISTS].PASAJE 
	(PASAJE_CODIGO, PASAJE_COSTO, PASAJE_PRECIO, PASAJE_VUELO, PASAJE_BUTACA, PASAJE_COMPRA) VALUES (
		@pasajeCodigo,
		@pasajeCosto ,
		@pasajePrecio,
		@pasajeVuelo ,
		@pasajeButaca,
		@pasajeCompra	
	);
GO

CREATE PROCEDURE DATASCIENTISTS.MigracionInsertarPasajes
AS
	DECLARE @butacaId DECIMAL(18,0), @pasajeCodigo DECIMAL(18,0), @pasajeCosto DECIMAL(18,2), @pasajePrecio DECIMAL(18,2), 
		@codigoVuelo DECIMAL(18,0), @numeroButaca DECIMAL(18,0), @tipoButaca NVARCHAR(255), @avionId NVARCHAR(50), @numeroCompra DECIMAL(18,0);
	DECLARE pasajes CURSOR
	FOR (SELECT PASAJE_CODIGO, PASAJE_COSTO, PASAJE_PRECIO, 
			VUELO_CODIGO, BUTACA_NUMERO, BUTACA_TIPO, AVION_IDENTIFICADOR, COMPRA_NUMERO
		FROM gd_esquema.Maestra
		WHERE PASAJE_CODIGO is not null
		GROUP BY PASAJE_CODIGO, PASAJE_COSTO, PASAJE_PRECIO, 
			VUELO_CODIGO, BUTACA_NUMERO, BUTACA_TIPO, AVION_IDENTIFICADOR, COMPRA_NUMERO);
	OPEN pasajes;
	FETCH FROM pasajes INTO @pasajeCodigo, @pasajeCosto, @pasajePrecio, @codigoVuelo, @numeroButaca, @tipoButaca, @avionId, @numeroCompra;
	WHILE @@FETCH_STATUS = 0
	BEGIN
		--Llama a DATASCIENTISTS.BuscarEInsertarButaca con el numero, tipo y el avión, y guarda el id devuelto
		EXECUTE DATASCIENTISTS.BuscarEInsertarButaca @numeroButaca, @tipoButaca, @avionId, @butacaId OUTPUT;
		
		--Llama a DATASCIENTISTS.InsertarPasaje para finalmente guardar el pasaje con toda la información necesaria
		EXECUTE DATASCIENTISTS.InsertarPasaje @pasajeCodigo, @pasajeCosto, @pasajePrecio, @codigoVuelo, @butacaId, @numeroCompra;
		
		FETCH FROM pasajes INTO @pasajeCodigo, @pasajeCosto, @pasajePrecio, @codigoVuelo, @numeroButaca, @tipoButaca, @avionId, @numeroCompra;
	END
	CLOSE pasajes;
	DEALLOCATE pasajes;
	PRINT CAST(SYSDATETIME() AS VARCHAR(25))+' Pasajes y butacas insertados correctamente';
GO

CREATE PROCEDURE DATASCIENTISTS.MigracionEmpresas
AS
	INSERT [DATASCIENTISTS].EMPRESA (EMPRESA_RAZON_SOCIAL) (
		SELECT DISTINCT EMPRESA_RAZON_SOCIAL
		FROM gd_esquema.Maestra
		WHERE EMPRESA_RAZON_SOCIAL IS NOT NULL
	);
	PRINT CAST(SYSDATETIME() AS VARCHAR(25))+' Empresas insertadas correctamente';
GO

CREATE PROCEDURE DATASCIENTISTS.MigracionCompras
AS
	INSERT [DATASCIENTISTS].COMPRA (COMPRA_NUMERO, COMPRA_FECHA, COMPRA_EMPRESA) (
		SELECT COMPRA_NUMERO, COMPRA_FECHA,
			DATASCIENTISTS.ObtenerIdEmpresa(EMPRESA_RAZON_SOCIAL)
		FROM gd_esquema.Maestra
		WHERE COMPRA_NUMERO is not null
		GROUP BY COMPRA_NUMERO, COMPRA_FECHA, EMPRESA_RAZON_SOCIAL
	);
	PRINT CAST(SYSDATETIME() AS VARCHAR(25))+' Compras insertadas correctamente';
GO

CREATE PROCEDURE DATASCIENTISTS.MigracionPasajes
AS
	EXEC DATASCIENTISTS.MigracionInsertarCiudades;
	EXEC DATASCIENTISTS.MigracionInsertarRutasAereas;
	EXEC DATASCIENTISTS.MigracionInsertarVuelos;
	EXEC DATASCIENTISTS.MigracionInsertarAviones;
	EXEC DATASCIENTISTS.MigracionInsertarPasajes;
GO

CREATE PROCEDURE DATASCIENTISTS.InsertarEstadias 
(@ESTADIA decimal(18,0), @FECHA datetime2(3), @NOCHES decimal(18,0), @COMPRA decimal (18,0), @HABITACION decimal(18,0))
AS
	INSERT DATASCIENTISTS.ESTADIA
	(ESTADIA_CODIGO,ESTADIA_FECHA_INI,ESTADIA_CANTIDAD_NOCHES,ESTADIA_COMPRA,ESTADIA_HABITACION)
	VALUES(@ESTADIA,@FECHA,@NOCHES,@COMPRA,@HABITACION)

GO

CREATE PROCEDURE DATASCIENTISTS.InsertarItemsEstadias (@ESTADIA decimal(18,0), @FACTURA decimal(18,0))
AS
	INSERT DATASCIENTISTS.ITEMS_ESTADIA
	(ITEM_EST_COD_ESTADIA,ITEM_EST_FACT_NUMERO)
	VALUES(@ESTADIA,@FACTURA)

GO

CREATE PROCEDURE DATASCIENTISTS.MigracionEstadias
AS

	OPEN MigracionEstadiaFactura;

	DECLARE @COMPRA decimal(18,0), @NOCHES decimal(18,0), @ESTADIA decimal(18,0), @FECHA_INI datetime2(3), @CALLE nvarchar(50); 
	DECLARE @NRO_CALLE decimal(18,0), @HNUMERO decimal (18,0), @HPISO int, @HFRENTE nvarchar(50), @FACTURA decimal(18,0), @HABITACIONID decimal(18,0);

	FETCH FROM MigracionEstadiaFactura INTO @COMPRA, @NOCHES, @ESTADIA, @FECHA_INI, @CALLE, @NRO_CALLE, @HNUMERO, @HPISO, @HFRENTE, @FACTURA;

	WHILE(@@FETCH_STATUS=0)
	BEGIN
		SET @HABITACIONID = DATASCIENTISTS.ObtenerIDHabitacion(@CALLE,@NRO_CALLE,@HNUMERO,@HPISO,@HFRENTE);
		
		EXECUTE DATASCIENTISTS.InsertarEstadias @ESTADIA, @FECHA_INI, @NOCHES, @COMPRA, @HABITACIONID;
		--EXECUTE DATASCIENTISTS.InsertarItemsEstadias @ESTADIA, @FACTURA;

		FETCH FROM MigracionEstadiaFactura INTO @COMPRA, @NOCHES, @ESTADIA, @FECHA_INI, @CALLE, @NRO_CALLE, @HNUMERO, @HPISO, @HFRENTE, @FACTURA

	END
	CLOSE MigracionEstadiaFactura;
	DEALLOCATE MigracionEstadiaFactura;


PRINT CAST(SYSDATETIME() AS VARCHAR(25))+' Estadias insertadas correctamente';
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

/* *** MIGRACIÓN *** */

EXEC DATASCIENTISTS.MigracionEmpresas
GO

EXEC DATASCIENTISTS.MigracionCompras
GO

EXEC DATASCIENTISTS.MigracionPasajes;
GO

EXEC DATASCIENTISTS.MigracionTipoHabitaciones
GO

EXEC DATASCIENTISTS.MigracionHabitacionesHoteles
GO

EXEC DATASCIENTISTS.MigracionEstadias
GO

EXEC DATASCIENTISTS.MigracionInsertarSucursales
GO

EXEC DATASCIENTISTS.MigracionInsertarFacturasItems
GO

--Sigue..
