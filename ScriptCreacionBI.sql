USE GD1C2020;

GO
--BORRAR

--STORED PROCEDURES
if object_id('DATASCIENTISTS.MigracionMesAnio') is not null
	DROP PROCEDURE DATASCIENTISTS.MigracionMesAnio;
GO
if object_id('DATASCIENTISTS.MigracionProveedores') is not null
	DROP PROCEDURE DATASCIENTISTS.MigracionProveedores;
GO
if object_id('DATASCIENTISTS.MigracionTiposPasaje') is not null
	DROP PROCEDURE DATASCIENTISTS.MigracionTiposPasaje;
GO

--TABLAS
if object_id('DATASCIENTISTS.DIMENSION_MES_ANIO') is not null
	DROP TABLE [DATASCIENTISTS].DIMENSION_MES_ANIO;
GO
if object_id('DATASCIENTISTS.DIMENSION_PROVEEDORES') is not null
	DROP TABLE [DATASCIENTISTS].DIMENSION_PROVEEDORES;
GO
if object_id('DATASCIENTISTS.DIMENSION_TIPOS_PASAJE') is not null
	DROP TABLE [DATASCIENTISTS].DIMENSION_TIPOS_PASAJE;
GO
	
PRINT CAST(SYSDATETIME() AS VARCHAR(25))+' Modelo dimensional de datos anterior, borrado correctamente';
GO

CREATE TABLE [DATASCIENTISTS].[DIMENSION_PROVEEDORES]
(
	[PROV_ID] decimal(18,0) NOT NULL,
	[PROV_RAZON_SOCIAL] nvarchar(255),
	[PROV_CANT_PASAJES_VENDIDA] decimal(18,0),
	[PROV_CANT_ESTADIAS_VENDIDA] decimal(18,0),
	[PROV_MAYOR_TIPO_BUTACA] nvarchar(50),
	[PROV_CANT_ESTRELLAS_PROM] decimal(18,2),
	[PROV_MAYOR_DESTINO] nvarchar(255),
	[PROV_GANANCIA_TOTAL] decimal(18,2),
	CONSTRAINT PK_DIMENSION_PROVEEDORES PRIMARY KEY(PROV_ID)
);

CREATE TABLE [DATASCIENTISTS].[DIMENSION_MES_ANIO]
(
	[MA_MES_ANIO_ID] decimal(18,0) IDENTITY(1,1) NOT NULL,
	[MA_MES] tinyint,
	[MA_ANIO] int,
	[MA_GANANCIA] decimal(18,2),
	[MA_FACTURACION] decimal(18,2),
	[MA_MAYOR_DESTINO] nvarchar(255),
	[MA_MAYOR_CLIENTE] nvarchar(100),
	[MA_MAYOR_SUCURSAL] nvarchar(255),
	[MA_MAYOR_PROVEEDOR] nvarchar(255),
	CONSTRAINT PK_DIMENSION_MES_ANIO PRIMARY KEY(MA_MES_ANIO_ID)
);

CREATE TABLE [DATASCIENTISTS].[DIMENSION_TIPOS_PASAJE]
(
	[TIP_PAS_ID] decimal(18,0) IDENTITY(1,1) NOT NULL,
	[TIP_PAS_TIPO_BUTACA] nvarchar(50),
	[TIP_CANT_PASAJES] decimal(18,0),
	[TIP_PAS_EDAD_PROM] decimal(18,2),
	[TIP_PAS_MAYOR_DESTINO] nvarchar(255),
	[TIP_PAS_MAYOR_PROVEEDOR] nvarchar(255),
	[TIP_PAS_GANANCIA_TOTAL] decimal(18,0),
	CONSTRAINT PK_DIMENSION_TIPOS_PASAJE PRIMARY KEY(TIP_PAS_ID)
);

PRINT CAST(SYSDATETIME() AS VARCHAR(25))+' Modelo de datos dimensional creado correctamente';
GO

--STORED PROCEDURES

--DIMENSION_MES_ANIO
CREATE PROCEDURE [DATASCIENTISTS].MigracionMesAnio
AS
BEGIN
	INSERT [DATASCIENTISTS].DIMENSION_MES_ANIO (MA_MES, MA_ANIO, MA_GANANCIA, MA_FACTURACION, MA_MAYOR_DESTINO,
		MA_MAYOR_CLIENTE, MA_MAYOR_SUCURSAL, MA_MAYOR_PROVEEDOR)
	SELECT MONTH(factEx.FACT_FECHA) MA_MES, YEAR(factEx.FACT_FECHA) MA_ANIO, 
	--SUMO GANANCIA POR PASAJES MÁS ESTADÍAS.
	ISNULL((SELECT SUM(PASAJE_PRECIO-PASAJE_COSTO) FROM [DATASCIENTISTS].ITEMS_PASAJE
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_PAS_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].PASAJE ON ITEM_PAS_COD_PASAJE=PASAJE_CODIGO
	WHERE YEAR(factIn.FACT_FECHA)=YEAR(factEx.FACT_FECHA) AND MONTH(factIn.FACT_FECHA)=MONTH(factEx.FACT_FECHA)
	GROUP BY YEAR(FACT_FECHA), MONTH(FACT_FECHA)),0)+
	ISNULL((SELECT SUM(HABIT_PRECIO-HABIT_COSTO) FROM [DATASCIENTISTS].ITEMS_ESTADIA
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_EST_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].ESTADIA ON ITEM_EST_COD_ESTADIA=ESTADIA_CODIGO
	INNER JOIN [DATASCIENTISTS].HABITACION ON ESTADIA_HABITACION=HABITACION_ID
	WHERE YEAR(factIn.FACT_FECHA)=YEAR(factEx.FACT_FECHA) AND MONTH(factIn.FACT_FECHA)=MONTH(factEx.FACT_FECHA)
	GROUP BY YEAR(FACT_FECHA), MONTH(FACT_FECHA)),0)  MA_GANANCIA,
	--SUMO FACTURACION POR PASAJES MÁS ESTADÍAS.
	ISNULL((SELECT SUM(PASAJE_PRECIO) FROM [DATASCIENTISTS].ITEMS_PASAJE
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_PAS_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].PASAJE ON ITEM_PAS_COD_PASAJE=PASAJE_CODIGO
	WHERE YEAR(factIn.FACT_FECHA)=YEAR(factEx.FACT_FECHA) AND MONTH(factIn.FACT_FECHA)=MONTH(factEx.FACT_FECHA)
	GROUP BY YEAR(FACT_FECHA), MONTH(FACT_FECHA)),0)+
	ISNULL((SELECT SUM(HABIT_COSTO) FROM [DATASCIENTISTS].ITEMS_ESTADIA
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_EST_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].ESTADIA ON ITEM_EST_COD_ESTADIA=ESTADIA_CODIGO
	INNER JOIN [DATASCIENTISTS].HABITACION ON ESTADIA_HABITACION=HABITACION_ID
	WHERE YEAR(factIn.FACT_FECHA)=YEAR(factEx.FACT_FECHA) AND MONTH(factIn.FACT_FECHA)=MONTH(factEx.FACT_FECHA)
	GROUP BY YEAR(FACT_FECHA), MONTH(FACT_FECHA)),0) MA_FACTURACION,
	--CASE PREGUNTANDO LA MAYOR CANTIDAD DE FACTURAS. SI FUERON POR PASAJES DEVOLVEMOS EL DESTINO
	--SI NO LA DIRECCIÓN DEL HOTEL
	CASE WHEN (SELECT TOP 1 COUNT(*) 
	FROM [DATASCIENTISTS].ITEMS_PASAJE
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_PAS_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].PASAJE ON ITEM_PAS_COD_PASAJE=PASAJE_CODIGO
	INNER JOIN [DATASCIENTISTS].VUELO ON PASAJE_VUELO=VUELO_CODIGO
	INNER JOIN [DATASCIENTISTS].CIUDADES ON CIUDAD_ID=VUELO_RUTA_AEREA_DEST
	WHERE YEAR(factIn.FACT_FECHA)=YEAR(factEx.FACT_FECHA) AND MONTH(factIn.FACT_FECHA)=MONTH(factEx.FACT_FECHA) 
	GROUP BY CIUDAD_NOMBRE
	ORDER BY COUNT(*) DESC)>(SELECT TOP 1 COUNT(*)
	FROM [DATASCIENTISTS].ITEMS_ESTADIA
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_EST_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].ESTADIA ON ITEM_EST_COD_ESTADIA=ESTADIA_CODIGO
	INNER JOIN [DATASCIENTISTS].HABITACION ON ESTADIA_HABITACION=HABITACION_ID
	INNER JOIN [DATASCIENTISTS].HOTEL ON HOTEL_ID=HABIT_HOTEL_ID
	WHERE YEAR(factIn.FACT_FECHA)=YEAR(factEx.FACT_FECHA) AND MONTH(factIn.FACT_FECHA)=MONTH(factEx.FACT_FECHA)
	GROUP BY HOTEL_CALLE, HOTEL_NUMERO_CALLE
	ORDER BY COUNT(*) DESC) THEN ((SELECT TOP 1 LEFT(CIUDAD_NOMBRE, 1)+LOWER(SUBSTRING(CIUDAD_NOMBRE,2, 254)) CIUDAD_NOMBRE 
	FROM [DATASCIENTISTS].ITEMS_PASAJE
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_PAS_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].PASAJE ON ITEM_PAS_COD_PASAJE=PASAJE_CODIGO
	INNER JOIN [DATASCIENTISTS].VUELO ON PASAJE_VUELO=VUELO_CODIGO
	INNER JOIN [DATASCIENTISTS].CIUDADES ON CIUDAD_ID=VUELO_RUTA_AEREA_DEST
	WHERE YEAR(factIn.FACT_FECHA)=YEAR(factEx.FACT_FECHA) AND MONTH(factIn.FACT_FECHA)=MONTH(factEx.FACT_FECHA) 
	GROUP BY CIUDAD_NOMBRE
	ORDER BY COUNT(*) DESC))
	ELSE (SELECT TOP 1 HOTEL_CALLE+' '+CAST(HOTEL_NUMERO_CALLE AS VARCHAR(10)) DESTINO
	FROM [DATASCIENTISTS].ITEMS_ESTADIA
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_EST_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].ESTADIA ON ITEM_EST_COD_ESTADIA=ESTADIA_CODIGO
	INNER JOIN [DATASCIENTISTS].HABITACION ON ESTADIA_HABITACION=HABITACION_ID
	INNER JOIN [DATASCIENTISTS].HOTEL ON HOTEL_ID=HABIT_HOTEL_ID
	WHERE YEAR(factIn.FACT_FECHA)=YEAR(factEx.FACT_FECHA) AND MONTH(factIn.FACT_FECHA)=MONTH(factEx.FACT_FECHA)
	GROUP BY HOTEL_CALLE, HOTEL_NUMERO_CALLE
	ORDER BY COUNT(*) DESC) END MA_MAYOR_DESTINO,
	--CASE PREGUNTANDO LA MAYOR GANANCIA. SI ES POR UNA ESTADIA, DEVOLVER ESE CLIENTE
	--SI NO, DEVOLVER EL CLIENTE DEL PASAJE
	CASE WHEN (SELECT TOP 1 PASAJE_PRECIO-PASAJE_COSTO FROM [DATASCIENTISTS].ITEMS_PASAJE
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_PAS_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].PASAJE ON ITEM_PAS_COD_PASAJE=PASAJE_CODIGO
	WHERE YEAR(factIn.FACT_FECHA)=YEAR(factEx.FACT_FECHA) AND MONTH(factIn.FACT_FECHA)=MONTH(factEx.FACT_FECHA) 
	GROUP BY FACT_CLIENTE, PASAJE_PRECIO, PASAJE_COSTO
	ORDER BY PASAJE_PRECIO-PASAJE_COSTO DESC)>(SELECT TOP 1 HABIT_PRECIO-HABIT_COSTO FROM [DATASCIENTISTS].ITEMS_ESTADIA
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_EST_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].ESTADIA ON ITEM_EST_COD_ESTADIA=ESTADIA_CODIGO
	INNER JOIN [DATASCIENTISTS].HABITACION ON ESTADIA_HABITACION=HABITACION_ID
	WHERE YEAR(factIn.FACT_FECHA)=YEAR(factEx.FACT_FECHA) AND MONTH(factIn.FACT_FECHA)=MONTH(factEx.FACT_FECHA) 
	GROUP BY FACT_CLIENTE, HABIT_PRECIO, HABIT_COSTO
	ORDER BY HABIT_PRECIO-HABIT_COSTO DESC) THEN (SELECT TOP 1 LEFT(CLIENTE_NOMBRE, 1)+LOWER(SUBSTRING(CLIENTE_NOMBRE,2, 254))+' '+CLIENTE_APELLIDO  
	FROM [DATASCIENTISTS].ITEMS_PASAJE
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_PAS_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].PASAJE ON ITEM_PAS_COD_PASAJE=PASAJE_CODIGO
	INNER JOIN DATASCIENTISTS.CLIENTE ON CLIENTE_ID=FACT_CLIENTE
	WHERE YEAR(factIn.FACT_FECHA)=YEAR(factEx.FACT_FECHA) AND MONTH(factIn.FACT_FECHA)=MONTH(factEx.FACT_FECHA) 
	GROUP BY CLIENTE_NOMBRE, CLIENTE_APELLIDO, PASAJE_PRECIO, PASAJE_COSTO
	ORDER BY PASAJE_PRECIO-PASAJE_COSTO DESC)
	ELSE (SELECT TOP 1 LEFT(CLIENTE_NOMBRE, 1)+LOWER(SUBSTRING(CLIENTE_NOMBRE,2, 254))+' '+CLIENTE_APELLIDO FROM [DATASCIENTISTS].ITEMS_ESTADIA
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_EST_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].ESTADIA ON ITEM_EST_COD_ESTADIA=ESTADIA_CODIGO
	INNER JOIN [DATASCIENTISTS].HABITACION ON ESTADIA_HABITACION=HABITACION_ID
	INNER JOIN DATASCIENTISTS.CLIENTE ON CLIENTE_ID=FACT_CLIENTE
	WHERE YEAR(factIn.FACT_FECHA)=YEAR(factEx.FACT_FECHA) AND MONTH(factIn.FACT_FECHA)=MONTH(factEx.FACT_FECHA) 
	GROUP BY CLIENTE_NOMBRE, CLIENTE_APELLIDO, HABIT_PRECIO, HABIT_COSTO
	ORDER BY HABIT_PRECIO-HABIT_COSTO DESC) END MA_MAYOR_CLIENTE,
	--CASE PREGUNTANDO LA MAYOR GANANCIA POR SUCURSAL.
	CASE WHEN (SELECT TOP 1 SUM(PASAJE_PRECIO-PASAJE_COSTO) FROM [DATASCIENTISTS].ITEMS_PASAJE
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_PAS_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].PASAJE ON ITEM_PAS_COD_PASAJE=PASAJE_CODIGO
	INNER JOIN DATASCIENTISTS.SUCURSAL ON SUCURSAL_ID=FACT_SUCURSAL
	WHERE YEAR(factIn.FACT_FECHA)=YEAR(factEx.FACT_FECHA) AND MONTH(factIn.FACT_FECHA)=MONTH(factEx.FACT_FECHA) 
	GROUP BY SUCURSAL_ID
	ORDER BY SUM(PASAJE_PRECIO-PASAJE_COSTO) DESC)>(SELECT TOP 1 SUM(HABIT_PRECIO-HABIT_COSTO) FROM [DATASCIENTISTS].ITEMS_ESTADIA
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_EST_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].ESTADIA ON ITEM_EST_COD_ESTADIA=ESTADIA_CODIGO
	INNER JOIN [DATASCIENTISTS].HABITACION ON ESTADIA_HABITACION=HABITACION_ID
	INNER JOIN DATASCIENTISTS.SUCURSAL ON SUCURSAL_ID=FACT_SUCURSAL
	WHERE YEAR(factIn.FACT_FECHA)=YEAR(factEx.FACT_FECHA) AND MONTH(factIn.FACT_FECHA)=MONTH(factEx.FACT_FECHA) 
	GROUP BY SUCURSAL_ID
	ORDER BY SUM(HABIT_PRECIO-HABIT_COSTO) DESC) THEN (SELECT TOP 1 SUCURSAL_DIR FROM [DATASCIENTISTS].ITEMS_PASAJE
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_PAS_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].PASAJE ON ITEM_PAS_COD_PASAJE=PASAJE_CODIGO
	INNER JOIN DATASCIENTISTS.SUCURSAL ON SUCURSAL_ID=FACT_SUCURSAL
	WHERE YEAR(factIn.FACT_FECHA)=YEAR(factEx.FACT_FECHA) AND MONTH(factIn.FACT_FECHA)=MONTH(factEx.FACT_FECHA) 
	GROUP BY SUCURSAL_DIR
	ORDER BY SUM(PASAJE_PRECIO-PASAJE_COSTO) DESC)
	ELSE (SELECT TOP 1 SUCURSAL_DIR FROM [DATASCIENTISTS].ITEMS_ESTADIA
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_EST_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].ESTADIA ON ITEM_EST_COD_ESTADIA=ESTADIA_CODIGO
	INNER JOIN [DATASCIENTISTS].HABITACION ON ESTADIA_HABITACION=HABITACION_ID
	INNER JOIN DATASCIENTISTS.SUCURSAL ON SUCURSAL_ID=FACT_SUCURSAL
	WHERE YEAR(factIn.FACT_FECHA)=YEAR(factEx.FACT_FECHA) AND MONTH(factIn.FACT_FECHA)=MONTH(factEx.FACT_FECHA) 
	GROUP BY SUCURSAL_DIR
	ORDER BY SUM(HABIT_PRECIO-HABIT_COSTO) DESC) END MA_MAYOR_SUCURSAL,
	--CASE PREGUNTANDO POR LA MAYOR GANANCIA POR EMPRESA. SI ES AEROCOMECIAL DEVOLVEMOS ESA
	--SI NO, LA HOTELERA
	CASE WHEN (SELECT TOP 1 SUM(PASAJE_PRECIO-PASAJE_COSTO) FROM [DATASCIENTISTS].ITEMS_PASAJE
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_PAS_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].PASAJE ON ITEM_PAS_COD_PASAJE=PASAJE_CODIGO
	INNER JOIN DATASCIENTISTS.COMPRA ON COMPRA_NUMERO=PASAJE_COMPRA
	INNER JOIN DATASCIENTISTS.EMPRESA ON COMPRA_EMPRESA=EMPRESA_ID
	WHERE YEAR(factIn.FACT_FECHA)=YEAR(factEx.FACT_FECHA) AND MONTH(factIn.FACT_FECHA)=MONTH(factEx.FACT_FECHA) 
	GROUP BY EMPRESA_ID
	ORDER BY SUM(PASAJE_PRECIO-PASAJE_COSTO) DESC)>(SELECT TOP 1 SUM(HABIT_PRECIO-HABIT_COSTO) FROM [DATASCIENTISTS].ITEMS_ESTADIA
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_EST_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].ESTADIA ON ITEM_EST_COD_ESTADIA=ESTADIA_CODIGO
	INNER JOIN [DATASCIENTISTS].HABITACION ON ESTADIA_HABITACION=HABITACION_ID
	INNER JOIN DATASCIENTISTS.COMPRA ON COMPRA_NUMERO=ESTADIA_COMPRA
	INNER JOIN DATASCIENTISTS.EMPRESA ON COMPRA_EMPRESA=EMPRESA_ID
	WHERE YEAR(factIn.FACT_FECHA)=YEAR(factEx.FACT_FECHA) AND MONTH(factIn.FACT_FECHA)=MONTH(factEx.FACT_FECHA) 
	GROUP BY EMPRESA_ID
	ORDER BY SUM(HABIT_PRECIO-HABIT_COSTO) DESC) THEN (SELECT TOP 1 EMPRESA_RAZON_SOCIAL FROM [DATASCIENTISTS].ITEMS_PASAJE
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_PAS_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].PASAJE ON ITEM_PAS_COD_PASAJE=PASAJE_CODIGO
	INNER JOIN DATASCIENTISTS.COMPRA ON COMPRA_NUMERO=PASAJE_COMPRA
	INNER JOIN DATASCIENTISTS.EMPRESA ON COMPRA_EMPRESA=EMPRESA_ID
	WHERE YEAR(factIn.FACT_FECHA)=YEAR(factEx.FACT_FECHA) AND MONTH(factIn.FACT_FECHA)=MONTH(factEx.FACT_FECHA) 
	GROUP BY EMPRESA_RAZON_SOCIAL
	ORDER BY SUM(PASAJE_PRECIO-PASAJE_COSTO) DESC)
	ELSE (SELECT TOP 1 EMPRESA_RAZON_SOCIAL FROM [DATASCIENTISTS].ITEMS_ESTADIA
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_EST_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].ESTADIA ON ITEM_EST_COD_ESTADIA=ESTADIA_CODIGO
	INNER JOIN [DATASCIENTISTS].HABITACION ON ESTADIA_HABITACION=HABITACION_ID
	INNER JOIN DATASCIENTISTS.COMPRA ON COMPRA_NUMERO=ESTADIA_COMPRA
	INNER JOIN DATASCIENTISTS.EMPRESA ON COMPRA_EMPRESA=EMPRESA_ID
	WHERE YEAR(factIn.FACT_FECHA)=YEAR(factEx.FACT_FECHA) AND MONTH(factIn.FACT_FECHA)=MONTH(factEx.FACT_FECHA) 
	GROUP BY EMPRESA_RAZON_SOCIAL
	ORDER BY SUM(HABIT_PRECIO-HABIT_COSTO) DESC) END MA_MAYOR_PROVEEDOR
	FROM [DATASCIENTISTS].FACTURA factEx
	GROUP BY YEAR(factEx.FACT_FECHA), MONTH(factEx.FACT_FECHA)
	ORDER BY YEAR(factEx.FACT_FECHA), MONTH(factEx.FACT_FECHA);
	PRINT CAST(SYSDATETIME() AS VARCHAR(25))+' Mes anio insertados correctamente';
END
GO

--DIMENSION_PROVEEDORES
CREATE PROCEDURE [DATASCIENTISTS].MigracionProveedores
AS
BEGIN
	INSERT [DATASCIENTISTS].DIMENSION_PROVEEDORES (PROV_ID, PROV_RAZON_SOCIAL, PROV_CANT_PASAJES_VENDIDA, PROV_CANT_ESTADIAS_VENDIDA, PROV_MAYOR_TIPO_BUTACA
		, PROV_CANT_ESTRELLAS_PROM, PROV_MAYOR_DESTINO, PROV_GANANCIA_TOTAL)
	SELECT EMPRESA_ID PROV_ID, EMPRESA_RAZON_SOCIAL PROV_RAZON_SOCIAL,
	--CUENTO LOS PASAJES DE LA EMPRESA
	ISNULL((SELECT COUNT(DISTINCT ITEM_PASAJE_ID) FROM [DATASCIENTISTS].ITEMS_PASAJE
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_PAS_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].PASAJE ON ITEM_PAS_COD_PASAJE=PASAJE_CODIGO
	INNER JOIN DATASCIENTISTS.COMPRA ON COMPRA_NUMERO=PASAJE_COMPRA
	INNER JOIN DATASCIENTISTS.EMPRESA empIn ON COMPRA_EMPRESA=empIn.EMPRESA_ID
	WHERE empIn.EMPRESA_ID=empEx.EMPRESA_ID
	GROUP BY empIn.EMPRESA_ID),0) PROV_CANT_PASAJES_VENDIDA,
	--CUENTO LAS ESTADIAS DE LA EMPRESA
	ISNULL((SELECT COUNT(DISTINCT ITEM_ESTADIA_ID) FROM [DATASCIENTISTS].ITEMS_ESTADIA
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_EST_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].ESTADIA ON ITEM_EST_COD_ESTADIA=ESTADIA_CODIGO
	INNER JOIN DATASCIENTISTS.COMPRA ON COMPRA_NUMERO=ESTADIA_COMPRA
	INNER JOIN DATASCIENTISTS.EMPRESA empIn ON COMPRA_EMPRESA=empIn.EMPRESA_ID
	WHERE empIn.EMPRESA_ID=empEx.EMPRESA_ID
	GROUP BY empIn.EMPRESA_ID),0) PROV_CANT_ESTADIAS_VENDIDA,
	--VEO CUÁL ES EL MAYOR TIPO DE BUTACA
	ISNULL((SELECT TOP 1 BUTACA_TIPO FROM [DATASCIENTISTS].ITEMS_PASAJE
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_PAS_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].PASAJE ON ITEM_PAS_COD_PASAJE=PASAJE_CODIGO
	INNER JOIN DATASCIENTISTS.COMPRA ON COMPRA_NUMERO=PASAJE_COMPRA
	INNER JOIN DATASCIENTISTS.EMPRESA empIn ON COMPRA_EMPRESA=empIn.EMPRESA_ID
	INNER JOIN DATASCIENTISTS.BUTACA ON BUTACA_ID=PASAJE_BUTACA
	WHERE empIn.EMPRESA_ID=empEx.EMPRESA_ID
	GROUP BY BUTACA_TIPO
	ORDER BY COUNT(DISTINCT ITEM_PASAJE_ID) DESC),'') PROV_MAYOR_TIPO_BUTACA,
	--VEO CUÁL ES EL PROMEDIO DE ESTRELLAS DE ESTADIAS
	ISNULL((SELECT AVG(HOTEL_CANTIDAD_ESTRELLAS) FROM [DATASCIENTISTS].ITEMS_ESTADIA
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_EST_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].ESTADIA ON ITEM_EST_COD_ESTADIA=ESTADIA_CODIGO
	INNER JOIN DATASCIENTISTS.COMPRA ON COMPRA_NUMERO=ESTADIA_COMPRA
	INNER JOIN DATASCIENTISTS.EMPRESA empIn ON COMPRA_EMPRESA=empIn.EMPRESA_ID
	INNER JOIN DATASCIENTISTS.HABITACION ON HABITACION_ID=ESTADIA_HABITACION
	INNER JOIN DATASCIENTISTS.HOTEL ON HABIT_HOTEL_ID=HOTEL_ID
	WHERE empIn.EMPRESA_ID=empEx.EMPRESA_ID
	GROUP BY empIn.EMPRESA_ID),0) PROV_CANT_ESTRELLAS_PROM,
	--CASE PREGUNTANDO LA MAYOR CANTIDAD DE FACTURAS. SI FUERON POR PASAJES DEVOLVEMOS EL DESTINO
	--SI NO LA DIRECCIÓN DEL HOTEL
	CASE WHEN ISNULL((SELECT TOP 1 COUNT(*) 
	FROM [DATASCIENTISTS].ITEMS_PASAJE
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_PAS_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].PASAJE ON ITEM_PAS_COD_PASAJE=PASAJE_CODIGO
	INNER JOIN [DATASCIENTISTS].VUELO ON PASAJE_VUELO=VUELO_CODIGO
	INNER JOIN [DATASCIENTISTS].CIUDADES ON CIUDAD_ID=VUELO_RUTA_AEREA_DEST
	INNER JOIN DATASCIENTISTS.COMPRA ON COMPRA_NUMERO=PASAJE_COMPRA
	INNER JOIN DATASCIENTISTS.EMPRESA empIn ON COMPRA_EMPRESA=empIn.EMPRESA_ID
	WHERE empIn.EMPRESA_ID=empEx.EMPRESA_ID
	GROUP BY CIUDAD_NOMBRE
	ORDER BY COUNT(*) DESC),0)>ISNULL((SELECT TOP 1 COUNT(*)
	FROM [DATASCIENTISTS].ITEMS_ESTADIA
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_EST_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].ESTADIA ON ITEM_EST_COD_ESTADIA=ESTADIA_CODIGO
	INNER JOIN [DATASCIENTISTS].HABITACION ON ESTADIA_HABITACION=HABITACION_ID
	INNER JOIN DATASCIENTISTS.COMPRA ON COMPRA_NUMERO=ESTADIA_COMPRA
	INNER JOIN DATASCIENTISTS.EMPRESA empIn ON COMPRA_EMPRESA=empIn.EMPRESA_ID
	INNER JOIN [DATASCIENTISTS].HOTEL ON HOTEL_ID=HABIT_HOTEL_ID
	WHERE empIn.EMPRESA_ID=empEx.EMPRESA_ID
	GROUP BY HOTEL_CALLE, HOTEL_NUMERO_CALLE
	ORDER BY COUNT(*) DESC),0) THEN ((SELECT TOP 1 LEFT(CIUDAD_NOMBRE, 1)+LOWER(SUBSTRING(CIUDAD_NOMBRE,2, 254)) CIUDAD_NOMBRE 
	FROM [DATASCIENTISTS].ITEMS_PASAJE
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_PAS_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].PASAJE ON ITEM_PAS_COD_PASAJE=PASAJE_CODIGO
	INNER JOIN [DATASCIENTISTS].VUELO ON PASAJE_VUELO=VUELO_CODIGO
	INNER JOIN [DATASCIENTISTS].CIUDADES ON CIUDAD_ID=VUELO_RUTA_AEREA_DEST
	INNER JOIN DATASCIENTISTS.COMPRA ON COMPRA_NUMERO=PASAJE_COMPRA
	INNER JOIN DATASCIENTISTS.EMPRESA empIn ON COMPRA_EMPRESA=empIn.EMPRESA_ID
	WHERE empIn.EMPRESA_ID=empEx.EMPRESA_ID
	GROUP BY CIUDAD_NOMBRE
	ORDER BY COUNT(*) DESC))
	ELSE (SELECT TOP 1 HOTEL_CALLE+' '+CAST(HOTEL_NUMERO_CALLE AS VARCHAR(10)) DESTINO
	FROM [DATASCIENTISTS].ITEMS_ESTADIA
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_EST_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].ESTADIA ON ITEM_EST_COD_ESTADIA=ESTADIA_CODIGO
	INNER JOIN [DATASCIENTISTS].HABITACION ON ESTADIA_HABITACION=HABITACION_ID
	INNER JOIN [DATASCIENTISTS].HOTEL ON HOTEL_ID=HABIT_HOTEL_ID
	INNER JOIN DATASCIENTISTS.COMPRA ON COMPRA_NUMERO=ESTADIA_COMPRA
	INNER JOIN DATASCIENTISTS.EMPRESA empIn ON COMPRA_EMPRESA=empIn.EMPRESA_ID
	WHERE empIn.EMPRESA_ID=empEx.EMPRESA_ID
	GROUP BY HOTEL_CALLE, HOTEL_NUMERO_CALLE
	ORDER BY COUNT(*) DESC) END PROV_MAYOR_DESTINO,
	--SUMO LAS GANANCIAS POR ESTADÍAS A LAS DE PASAJES
	ISNULL((SELECT SUM(PASAJE_PRECIO-PASAJE_COSTO) FROM [DATASCIENTISTS].ITEMS_PASAJE
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_PAS_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].PASAJE ON ITEM_PAS_COD_PASAJE=PASAJE_CODIGO
	INNER JOIN DATASCIENTISTS.COMPRA ON COMPRA_NUMERO=PASAJE_COMPRA
	INNER JOIN DATASCIENTISTS.EMPRESA empIn ON COMPRA_EMPRESA=empIn.EMPRESA_ID
	WHERE empIn.EMPRESA_ID=empEx.EMPRESA_ID
	GROUP BY empIn.EMPRESA_ID),0)+
	ISNULL((SELECT SUM(HABIT_PRECIO-HABIT_COSTO) FROM [DATASCIENTISTS].ITEMS_ESTADIA
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_EST_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].ESTADIA ON ITEM_EST_COD_ESTADIA=ESTADIA_CODIGO
	INNER JOIN [DATASCIENTISTS].HABITACION ON ESTADIA_HABITACION=HABITACION_ID
	INNER JOIN DATASCIENTISTS.COMPRA ON COMPRA_NUMERO=ESTADIA_COMPRA
	INNER JOIN DATASCIENTISTS.EMPRESA empIn ON COMPRA_EMPRESA=empIn.EMPRESA_ID
	WHERE empIn.EMPRESA_ID=empEx.EMPRESA_ID
	GROUP BY empIn.EMPRESA_ID),0) PROV_GANANCIA_TOTAL
	FROM DATASCIENTISTS.EMPRESA empEx;
	PRINT CAST(SYSDATETIME() AS VARCHAR(25))+' Proveedores insertados correctamente';
END
GO

--DIMENSION_TIPOS_PASAJE
CREATE PROCEDURE [DATASCIENTISTS].MigracionTiposPasaje
AS
BEGIN
	INSERT [DATASCIENTISTS].DIMENSION_TIPOS_PASAJE (TIP_PAS_TIPO_BUTACA, TIP_CANT_PASAJES, TIP_PAS_EDAD_PROM, TIP_PAS_MAYOR_DESTINO, 
		TIP_PAS_MAYOR_PROVEEDOR, TIP_PAS_GANANCIA_TOTAL)
	SELECT DISTINCT butEx.BUTACA_TIPO TIP_PAS_TIPO_BUTACA,
	--CALCULO CANTIDAD DE PASAJES
	(SELECT COUNT(DISTINCT ITEM_PASAJE_ID) 
	FROM [DATASCIENTISTS].ITEMS_PASAJE
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_PAS_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].PASAJE ON ITEM_PAS_COD_PASAJE=PASAJE_CODIGO
	INNER JOIN DATASCIENTISTS.BUTACA butIn ON butIn.BUTACA_ID=PASAJE_BUTACA
	WHERE butIn.BUTACA_TIPO=butEx.BUTACA_TIPO
	GROUP BY butIn.BUTACA_TIPO) TIP_CANT_PASAJES,
	--CALCULO EDAD PROMEDIO
	(SELECT AVG(DATEDIFF(YEAR, CLIENTE_FECHA_NAC, Getdate())) 
	FROM [DATASCIENTISTS].ITEMS_PASAJE
	INNER JOIN [DATASCIENTISTS].FACTURA factIn ON ITEM_PAS_FACT_NUMERO=factIn.FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].PASAJE ON ITEM_PAS_COD_PASAJE=PASAJE_CODIGO
	INNER JOIN DATASCIENTISTS.CLIENTE ON CLIENTE_ID=FACT_CLIENTE
	INNER JOIN DATASCIENTISTS.BUTACA butIn ON butIn.BUTACA_ID=PASAJE_BUTACA
	WHERE butIn.BUTACA_TIPO=butEx.BUTACA_TIPO
	GROUP BY butIn.BUTACA_TIPO) TIP_PAS_EDAD_PROM,
	--CALCULO MAYOR DESTINO DEL TIPO DE PASAJE
	(SELECT TOP 1 LEFT(CIUDAD_NOMBRE, 1)+LOWER(SUBSTRING(CIUDAD_NOMBRE,2, 254)) CIUDAD_NOMBRE 
	FROM [DATASCIENTISTS].ITEMS_PASAJE
	INNER JOIN [DATASCIENTISTS].FACTURA ON ITEM_PAS_FACT_NUMERO=FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].PASAJE ON ITEM_PAS_COD_PASAJE=PASAJE_CODIGO
	INNER JOIN [DATASCIENTISTS].VUELO ON PASAJE_VUELO=VUELO_CODIGO
	INNER JOIN [DATASCIENTISTS].CIUDADES ON CIUDAD_ID=VUELO_RUTA_AEREA_DEST
	INNER JOIN DATASCIENTISTS.BUTACA butIn ON butIn.BUTACA_ID=PASAJE_BUTACA
	WHERE butIn.BUTACA_TIPO=butEx.BUTACA_TIPO
	GROUP BY CIUDAD_NOMBRE
	ORDER BY COUNT(DISTINCT ITEM_PASAJE_ID) DESC) TIP_PAS_MAYOR_DESTINO,
	--CALCULO MAYOR PROOVEDOR PARA EL TIPO DE PASAJE
	(SELECT TOP 1 empIn.EMPRESA_RAZON_SOCIAL FROM [DATASCIENTISTS].ITEMS_PASAJE
	INNER JOIN [DATASCIENTISTS].FACTURA ON ITEM_PAS_FACT_NUMERO=FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].PASAJE ON ITEM_PAS_COD_PASAJE=PASAJE_CODIGO
	INNER JOIN DATASCIENTISTS.COMPRA ON COMPRA_NUMERO=PASAJE_COMPRA
	INNER JOIN DATASCIENTISTS.EMPRESA empIn ON COMPRA_EMPRESA=empIn.EMPRESA_ID
	INNER JOIN DATASCIENTISTS.BUTACA butIn ON butIn.BUTACA_ID=PASAJE_BUTACA
	WHERE butIn.BUTACA_TIPO=butEx.BUTACA_TIPO
	GROUP BY EMPRESA_RAZON_SOCIAL
	ORDER BY COUNT(DISTINCT ITEM_PASAJE_ID) DESC) TIP_PAS_MAYOR_PROVEEDOR,
	--CALCULO LA GANANCIA TOTAL DE ESE TIPO DE PASAJE
	(SELECT SUM(PASAJE_PRECIO-PASAJE_COSTO) FROM [DATASCIENTISTS].ITEMS_PASAJE
	INNER JOIN [DATASCIENTISTS].FACTURA ON ITEM_PAS_FACT_NUMERO=FACT_NUMERO
	INNER JOIN [DATASCIENTISTS].PASAJE ON ITEM_PAS_COD_PASAJE=PASAJE_CODIGO
	INNER JOIN DATASCIENTISTS.BUTACA butIn ON butIn.BUTACA_ID=PASAJE_BUTACA
	WHERE butIn.BUTACA_TIPO=butEx.BUTACA_TIPO
	GROUP BY butIn.BUTACA_TIPO) TIP_PAS_GANANCIA_TOTAL
	FROM [DATASCIENTISTS].BUTACA butEx;
	PRINT CAST(SYSDATETIME() AS VARCHAR(25))+' Tipos Pasaje insertados correctamente';
END
GO
--Carga HECHOS_VIAJES y DIMENSION_CLIENTES relacionados con pasajes 
/*
SELECT YEAR(FACT_FECHA) AÑO, MONTH(FACT_FECHA) MES,LEFT(CLIENTE_NOMBRE, 1)+LOWER(SUBSTRING(CLIENTE_NOMBRE,2, 254)) CLIENTE_NOMBRE, 
CLIENTE_APELLIDO, DATEDIFF(YEAR, CLIENTE_FECHA_NAC, Getdate()) EDAD, CLIENTE_DNI,CLIENTE_MAIL, CLIENTE_TELEFONO, EMPRESA_RAZON_SOCIAL,
NULL TIPO_HABITACION, BUTACA_TIPO, LEFT(CIUDAD_NOMBRE, 1)+LOWER(SUBSTRING(CIUDAD_NOMBRE,2, 254))CIUDAD_NOMBRE,AVION_IDENTIFICADOR, 
VUELO_RUTA_AEREA_ORIG, VUELO_RUTA_AEREA_DEST, SUCURSAL_DIR,'PASAJE' DISCRIMINADOR,PASAJE_PRECIO-PASAJE_COSTO GANANCIA,1 CANTIDAD
FROM [DATASCIENTISTS].ITEMS_PASAJE
INNER JOIN [DATASCIENTISTS].FACTURA ON ITEM_PAS_FACT_NUMERO=FACT_NUMERO
INNER JOIN [DATASCIENTISTS].SUCURSAL ON SUCURSAL_ID=FACT_SUCURSAL
INNER JOIN [DATASCIENTISTS].PASAJE ON ITEM_PAS_COD_PASAJE=PASAJE_CODIGO
INNER JOIN [DATASCIENTISTS].BUTACA ON BUTACA_ID=PASAJE_BUTACA
INNER JOIN [DATASCIENTISTS].AVION ON BUTACA_AVION=AVION_IDENTIFICADOR
INNER JOIN [DATASCIENTISTS].COMPRA ON PASAJE_COMPRA=COMPRA_NUMERO
INNER JOIN [DATASCIENTISTS].EMPRESA ON COMPRA_EMPRESA=EMPRESA_ID
INNER JOIN [DATASCIENTISTS].VUELO ON PASAJE_VUELO=VUELO_CODIGO
INNER JOIN [DATASCIENTISTS].CIUDADES ON CIUDAD_ID=VUELO_RUTA_AEREA_DEST
INNER JOIN DATASCIENTISTS.CLIENTE ON FACT_CLIENTE=CLIENTE_ID;*/

/* *** MIGRACIÓN *** */

EXEC DATASCIENTISTS.MigracionMesAnio
GO

EXEC DATASCIENTISTS.MigracionProveedores
GO

EXEC DATASCIENTISTS.MigracionTiposPasaje
GO