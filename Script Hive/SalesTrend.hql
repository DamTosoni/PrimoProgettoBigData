CREATE TABLE IF NOT EXISTS sales (row STRING);

LOAD DATA LOCAL INPATH 'Sales_old.txt' OVERWRITE INTO TABLE sales;

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

-- Unisco le tre viste --
INSERT OVERWRITE LOCAL DIRECTORY 'SalesTrendresult'
SELECT salesArrayJanuary.product,CONCAT('1/2015:',salesArrayJanuary.sales,' 2/2015:',salesArrayFebruary.sales,' 3/2015:',salesArrayMarch.sales)
FROM salesArrayJanuary FULL OUTER JOIN salesArrayFebruary ON (salesArrayJanuary.product=salesArrayFebruary.product) FULL OUTER JOIN salesArrayMarch ON (salesArrayJanuary.product=salesArrayMarch.product);


-- Elimino le tabelle una volta scritto il risultato --
DROP VIEW salesArray;
DROP VIEW salesArrayJanuary;
DROP VIEW salesArrayFebruary;
DROP VIEW salesArrayMarch;
DROP TABLE sales;