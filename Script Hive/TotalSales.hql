CREATE TABLE IF NOT EXISTS sales (row STRING);

LOAD DATA LOCAL INPATH 'Sales_old.txt' OVERWRITE INTO TABLE sales;

-- SalesArray contiene gli stessi dati di sales sotto forma di array --
INSERT OVERWRITE LOCAL DIRECTORY 'TotalSalesresult'
SELECT product, COUNT(rowArray) as totalsales
FROM (SELECT split(row,',') as rowArray FROM sales) as salesArray LATERAL VIEW explode(rowArray) tableExploded AS product
WHERE product NOT LIKE '20%'
GROUP BY product
SORT BY totalsales DESC;
