CREATE TABLE IF NOT EXISTS sales (row STRING);

LOAD DATA LOCAL INPATH 'Sales.txt' OVERWRITE INTO TABLE sales;

-- Creo una vista che contiene i prodotti e le righe in cui compaiono --
CREATE VIEW IF NOT EXISTS salesSplitted (product, row)
AS SELECT product,row
FROM (SELECT row, split(row,',') as rowArray FROM sales) as sales2 LATERAL VIEW explode(rowArray) tableExploded AS product
WHERE product NOT LIKE '20%';

CREATE VIEW IF NOT EXISTS couplesSales (couple, product1, sales)
AS SELECT concat(s1.product, ',' ,s2.product) as couple, s1.product as product1, COUNT(*) as sales
FROM salesSplitted s1, salesSplitted s2
WHERE s1.product<>s2.product AND s1.row == s2.row
GROUP BY concat(s1.product, ',' ,s2.product), s1.product;

CREATE VIEW IF NOT EXISTS totalSales (product, totalSales)
AS SELECT product, COUNT(row) as totalsales
FROM salesSplitted
GROUP BY product;

INSERT OVERWRITE LOCAL DIRECTORY 'CouplesFrequencyresult'
SELECT couplesSales.couple, couplesSales.sales/totalSales.totalsales as percentual
FROM couplesSales JOIN totalSales ON couplesSales.product1==totalSales.product
SORT BY percentual DESC
LIMIT 10;



-- Elimino le tabelle create --
DROP TABLE sales;
DROP VIEW salesSplitted;
DROP VIEW couplesSales;
DROP VIEW totalSales;
