CREATE TABLE IF NOT EXISTS sales (row STRING);

LOAD DATA LOCAL INPATH 'Sales.txt' OVERWRITE INTO TABLE sales;

-- Creo una vista che contiene i valori di sales divisi e riportati in un array --
CREATE VIEW IF NOT EXISTS salesArray (product, rowArray)
AS SELECT product,rowArray
FROM (SELECT split(row,',') as rowArray FROM sales) as sales2 LATERAL VIEW explode(rowArray) tableExploded AS product
WHERE product NOT LIKE '20%';

-- Creo le viste relative ai tre mesi del primo trimestre --
CREATE VIEW IF NOT EXISTS salesArrayJanuary (product,sales)
AS SELECT product, COUNT(rowArray)
FROM salesArray
WHERE rowArray[0] like '2015-1-%'
GROUP BY product;

CREATE VIEW IF NOT EXISTS salesArrayFebruary (product,sales)
AS SELECT product, COUNT(rowArray)
FROM salesArray
WHERE rowArray[0] like '2015-2-%'
GROUP BY product;

CREATE VIEW IF NOT EXISTS salesArrayMarch (product,sales)
AS SELECT product, COUNT(rowArray)
FROM salesArray
WHERE rowArray[0] like '2015-3-%'
GROUP BY product;

-- Creo due viste per collegare le vendite dei vari mesi --
CREATE VIEW IF NOT EXISTS salesJanuaryFebruary (product,salesJanuary, salesFebruary)
  AS SELECT 
    COALESCE(salesArrayJanuary.product,salesArrayFebruary.product), salesArrayJanuary.sales as salesJanuary,salesArrayFebruary.sales as salesFebruary
  FROM salesArrayJanuary FULL OUTER JOIN salesArrayFebruary ON (salesArrayJanuary.product=salesArrayFebruary.product);

CREATE VIEW IF NOT EXISTS salesFebruaryMarch (product,salesFebruary,salesMarch)
  AS SELECT
    COALESCE(salesArrayFebruary.product,salesArrayMarch.product), salesArrayFebruary.sales as salesFebruary,salesArrayMarch.sales as salesMarch
  FROM salesArrayFebruary FULL OUTER JOIN salesArrayMarch ON (salesArrayFebruary.product=salesArrayMarch.product);

-- Unisco le due viste --
INSERT OVERWRITE LOCAL DIRECTORY 'SalesTrendResult'
SELECT COALESCE(salesJanuaryFebruary.product,salesFebruaryMarch.product),
	CONCAT('1/2015:',COALESCE(salesJanuaryFebruary.salesJanuary,0),
	' 2/2015:',COALESCE(salesJanuaryFebruary.salesFebruary,salesFebruaryMarch.salesFebruary,0),
	' 3/2015:',COALESCE(salesFebruaryMarch.salesMarch,0))
FROM salesJanuaryFebruary FULL OUTER JOIN salesFebruaryMarch ON (salesJanuaryFebruary.product=salesFebruaryMarch.product);

-- Elimino le tabelle una volta scritto il risultato --
DROP VIEW salesArray;
DROP VIEW salesArrayJanuary;
DROP VIEW salesArrayFebruary;
DROP VIEW salesArrayMarch;
DROP VIEW salesJanuaryFebruary;
DROP VIEW salesFebruaryMarch;
DROP TABLE sales;