USE [GD1C2020];

GO
--BORRAR
--CURSORES
if object_id('MigracionHotelHabitacion') is not null
	DEALLOCATE MigracionHotelHabitacion;
GO

--STORED PROCEDURES
if object_id('DATASCIENTISTS.DatascientistsMigracionTipoHabitaciones') is not null
	DROP PROCEDURE DATASCIENTISTS.DatascientistsMigracionTipoHabitaciones;
GO
if object_id('DATASCIENTISTS.MigracionInsertarHotel') is not null
	DROP PROCEDURE DATASCIENTISTS.MigracionInsertarHotel;
GO
if object_id('DATASCIENTISTS.MigracionInsertarHabitacion') is not null
	DROP PROCEDURE DATASCIENTISTS.MigracionInsertarHabitacion;
GO
if object_id('DATASCIENTISTS.DatascientistsMigracionHabitacionesHoteles') is not null
	DROP PROCEDURE DATASCIENTISTS.DatascientistsMigracionHabitacionesHoteles;
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
	CONSTRAINT�PK_EMPRESA PRIMARY KEY(EMPRESA_ID)
);

CREATE TABLE [DATASCIENTISTS].[COMPRA]
(
	[COMPRA_NUMERO] decimal(18,0) NOT NULL,
	[COMPRA_FECHA] datetime2(3),
	CONSTRAINT�PK_COMPRA PRIMARY KEY(COMPRA_NUMERO)
);

CREATE TABLE [DATASCIENTISTS].[AVION]
(
	[AVION_IDENTIFICADOR] decimal(18,0) NOT NULL,
	[AVION_MODELO] nvarchar(50),
	CONSTRAINT�PK_AVION PRIMARY KEY(AVION_IDENTIFICADOR)
);

CREATE TABLE [DATASCIENTISTS].[HOTEL]
(
	[HOTEL_ID] decimal(18,0) IDENTITY(1,1) NOT NULL,
	[HOTEL_CALLE] nvarchar(50),
	[HOTEL_NUMERO_CALLE] int,					--(Y)Pongo un tipo de dato mas chico, no hay alturas de calle de un �rden de millones
	[HOTEL_CANTIDAD_ESTRELLAS] tinyint,			--(Y)Pongo un tipo de dato mas chico, solo va a ser 1 n�mero de estrellas, que les parece?
	CONSTRAINT�PK_HOTEL PRIMARY KEY(HOTEL_ID),
	CHECK (HOTEL_CANTIDAD_ESTRELLAS IN (1,2,3,4,5)) --(Y)agrego un check, que piensan? 
);

CREATE TABLE [DATASCIENTISTS].[TIPO_HABITACION]
(
	[TIPO_HABITACION_CODIGO] decimal(18,0) NOT NULL,
	[TIPO_HABITACION_DESCRIPCION] nvarchar(50),
	CONSTRAINT�PK_TIPO_HABITACION PRIMARY KEY(TIPO_HABITACION_CODIGO)
);

CREATE TABLE [DATASCIENTISTS].[HABITACION]
(
	[HABITACION_ID] decimal(18,0) IDENTITY(1,1) NOT NULL,
	[HABIT_HOTEL_ID] decimal(18,0) NOT NULL,
	[HABIT_TIPO_COD] decimal(18,0) NOT NULL,
	[HABIT_NUMERO] decimal(18,0),
	[HABIT_PISO] int,					--(Y)Pongo un tipo de dato mas chico, no hay pisos de un �rden de millones
	[HABIT_FRENTE] nvarchar(50),
	[HABIT_COSTO] decimal(18,2),
	[HABIT_PRECIO] decimal(18,2)
	CONSTRAINT�PK_HABITACION PRIMARY KEY(HABITACION_ID),
	CONSTRAINT�FK_HOTEL_HABITACION FOREIGN KEY(HABIT_HOTEL_ID)
		REFERENCES [DATASCIENTISTS].HOTEL(HOTEL_ID),
	CONSTRAINT�FK_TIPO_HABITACION FOREIGN KEY(HABIT_TIPO_COD)
		REFERENCES [DATASCIENTISTS].TIPO_HABITACION(TIPO_HABITACION_CODIGO)
);

CREATE TABLE [DATASCIENTISTS].ESTADIA
(
	ESTADIA_CODIGO decimal(18,0) NOT NULL,
	ESTADIA_COMPRA decimal(18,0) NOT NULL,
	ESTADIA_HABITACION decimal(18,0) NOT NULL,
	ESTADIA_FECHA_INI datetime2(3),
	ESTADIA_CANTIDAD_NOCHES int,					--(Y)Pongo un tipo de dato mas chico, un int alcanza
	CONSTRAINT�PK_ESTADIA PRIMARY KEY(ESTADIA_CODIGO),
	CONSTRAINT�FK_COMPRA_ESTADIA_COMPRA FOREIGN KEY(ESTADIA_COMPRA)
		REFERENCES [DATASCIENTISTS].COMPRA(COMPRA_NUMERO),
	CONSTRAINT�FK_HABITACION_ESTADIA FOREIGN KEY(ESTADIA_HABITACION)
		REFERENCES [DATASCIENTISTS].HABITACION(HABITACION_ID)
)

CREATE TABLE [DATASCIENTISTS].CIUDADES
(
	CIUDAD_ID decimal(18,0) IDENTITY(1,1) NOT NULL,
	CIUDAD_NOMBRE nvarchar(255),
	CONSTRAINT�PK_CIUDADES PRIMARY KEY(CIUDAD_ID)
);

CREATE TABLE [DATASCIENTISTS].RUTA_AEREA
(
	RUTA_AEREA_CODIGO decimal(18,0) NOT NULL,
	RUTA_AEREA_CIU_ORIG decimal(18,0) NOT NULL,
	RUTA_AEREA_CIU_DEST decimal(18,0) NOT NULL,
	CONSTRAINT�PK_RUTA_AEREA PRIMARY KEY(RUTA_AEREA_CODIGO),
	CONSTRAINT�FK_CIU_ORIG_RUTA_AEREA FOREIGN KEY(RUTA_AEREA_CIU_ORIG)
		REFERENCES [DATASCIENTISTS].CIUDADES(CIUDAD_ID),
	CONSTRAINT�FK_CIU_DEST_RUTA_AEREA FOREIGN KEY(RUTA_AEREA_CIU_DEST)
		REFERENCES [DATASCIENTISTS].CIUDADES(CIUDAD_ID),
)

CREATE TABLE [DATASCIENTISTS].VUELO
(
	VUELO_CODIGO decimal(18,0) NOT NULL,
	VUELO_FECHA_SALIDA datetime2(3),
	VUELO_FECHA_LLEGADA datetime2(3),
	VUELO_RUTA_AEREA decimal(18,0) NOT NULL,
	CONSTRAINT�PK_VUELO PRIMARY KEY(VUELO_CODIGO),
	CONSTRAINT�FK_RUTA_AEREA_VUELO FOREIGN KEY(VUELO_RUTA_AEREA)
		REFERENCES [DATASCIENTISTS].RUTA_AEREA(RUTA_AEREA_CODIGO)
)

CREATE TABLE [DATASCIENTISTS].BUTACA
(
	BUTACA_ID decimal(18,0) IDENTITY(1,1) NOT NULL,
	BUTACA_NUMERO int,					--(Y)Pongo un tipo de dato mas chico, no hay aviones con un �rden de millones para butacas
	BUTACA_TIPO nvarchar(50),			--(Y)Pongo un tipo de dato mas chico, los tipos de butaca no necesitan 255 caract�res
	BUTACA_AVION decimal(18,0) NOT NULL,
	CONSTRAINT�PK_BUTACA PRIMARY KEY(BUTACA_ID),
	CONSTRAINT�FK_AVION_BUTACA FOREIGN KEY(BUTACA_AVION)
		REFERENCES [DATASCIENTISTS].AVION(AVION_IDENTIFICADOR)
)

CREATE TABLE [DATASCIENTISTS].PASAJE
(
	PASAJE_CODIGO decimal(18,0) NOT NULL,
	PASAJE_COSTO decimal(18,2),
	PASAJE_PRECIO decimal(18,2),
	PASAJE_VUELO decimal(18,0) NOT NULL,
	PASAJE_BUTACA decimal(18,0) NOT NULL,
	PASAJE_COMPRA decimal(18,0) NOT NULL,
	CONSTRAINT�PK_PASAJE PRIMARY KEY(PASAJE_CODIGO),
	CONSTRAINT�FK_VUELO_PASAJE FOREIGN KEY(PASAJE_VUELO)
		REFERENCES [DATASCIENTISTS].VUELO(VUELO_CODIGO),
	CONSTRAINT�FK_BUTACA_PASAJE FOREIGN KEY(PASAJE_BUTACA)
		REFERENCES [DATASCIENTISTS].BUTACA(BUTACA_ID),
	CONSTRAINT�FK_COMPRA_PASAJE FOREIGN KEY(PASAJE_COMPRA)
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
	CONSTRAINT�FK_CLIENTE_FACTURA FOREIGN KEY(FACT_CLIENTE)
		REFERENCES [DATASCIENTISTS].CLIENTE(CLIENTE_ID),
	CONSTRAINT�FK_SUCURSAL_FACTURA FOREIGN KEY(FACT_SUCURSAL)
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
DECLARE MigracionHotelHabitacion Cursor
FOR
	SELECT HOTEL_CALLE, HOTEL_NRO_CALLE, HOTEL_CANTIDAD_ESTRELLAS, HABITACION_PISO, HABITACION_NUMERO,HABITACION_FRENTE, HABITACION_COSTO, HABITACION_PRECIO, TIPO_HABITACION_CODIGO
	FROM gd_esquema.Maestra
	WHERE HOTEL_CALLE is not null
	GROUP BY HOTEL_CALLE, HOTEL_NRO_CALLE, HOTEL_CANTIDAD_ESTRELLAS, HABITACION_PISO, HABITACION_NUMERO,HABITACION_FRENTE, HABITACION_COSTO, HABITACION_PRECIO, TIPO_HABITACION_CODIGO
	ORDER BY 1, 2, 4, 5

GO

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



CREATE PROCEDURE [DATASCIENTISTS].DatascientistsMigracionHabitacionesHoteles
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
CREATE PROCEDURE [DATASCIENTISTS].DatascientistsMigracionTipoHabitaciones
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

CREATE PROCEDURE DatascientistsMigracionPasajes
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

CREATE PROCEDURE DatascientistsMigracionEstadias
AS
PRINT CAST(SYSDATETIME() AS VARCHAR(25))+' Estadias insertadas correctamente';
GO



dbo.DatascientistsMigracionPasajes;
GO

dbo.DatascientistsMigracionEstadias;
GO

EXEC DATASCIENTISTS.DatascientistsMigracionTipoHabitaciones
GO

EXEC DATASCIENTISTS.DatascientistsMigracionHabitacionesHoteles
GO
if object_id('dbo.DatascientistsMigracionPasajes') is not null
	DROP PROCEDURE dbo.DatascientistsMigracionPasajes;
GO

if object_id('dbo.DatascientistsMigracionEstadias') is not null
	DROP PROCEDURE dbo.DatascientistsMigracionEstadias;
GO


--Sigue..
