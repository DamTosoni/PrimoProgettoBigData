CREATE TABLE IF NOT EXISTS sales (row STRING);

LOAD DATA LOCAL INPATH 'Sales.txt' OVERWRITE INTO TABLE sales;

-- Creo una vista che contiene i prodotti e le righe in cui compaiono --
CREATE VIEW IF NOT EXISTS salesSplitted (product, row)
AS SELECT product,row
FROM (SELECT row, split(row,',') as rowArray FROM sales) as sales2 LATERAL VIEW explode(rowArray) tableExploded AS product
WHERE product NOT LIKE '20%';

-- Scrivo il risultato --
INSERT OVERWRITE LOCAL DIRECTORY 'Top10SalesResult'
SELECT concat(s1.product, ',' ,s2.product) as couples, COUNT(*) as sales
FROM salesSplitted s1, salesSplitted s2
WHERE s1.product<s2.product AND s1.row == s2.row
GROUP BY concat(s1.product, ',' ,s2.product)
SORT BY sales DESC
LIMIT 10;

-- Elimino le tabelle create --
DROP TABLE sales;
DROP VIEW salesSplitted;
