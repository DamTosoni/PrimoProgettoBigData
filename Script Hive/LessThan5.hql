CREATE TABLE IF NOT EXISTS sales (row STRING);

LOAD DATA LOCAL INPATH 'Sales_old.txt' OVERWRITE INTO TABLE sales;

-- Creo una vista che contiene i prodotti e le righe in cui compaiono --

add jar CreateCombinationsUDF.jar;
CREATE TEMPORARY FUNCTION createCombination AS 'CreateCombinationsUDF.CreateCombinations';


CREATE VIEW IF NOT EXISTS salesSplitted (products)
AS SELECT products
FROM (SELECT row, createCombination(row) as rowArray FROM sales) as sales2 LATERAL VIEW explode(rowArray) tableExploded AS products;

-- Scrivo il risultato --
INSERT OVERWRITE LOCAL DIRECTORY 'LessThan5result'
SELECT products, COUNT(*) as sales
FROM salesSplitted
GROUP BY products
ORDER BY sales DESC
LIMIT 10;

-- Elimino le tabelle create --
DROP TABLE sales;
DROP VIEW salesSplitted;
