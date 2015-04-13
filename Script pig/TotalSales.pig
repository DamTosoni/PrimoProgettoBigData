sales = LOAD 'Sales.txt' USING PigStorage('\n');
flattenSales = FOREACH sales GENERATE FLATTEN(TOKENIZE($0)),$0;
filteredSales = FILTER flattenSales BY NOT(STARTSWITH($0,'2'));
groupedProducts = GROUP filteredSales BY $0;
result = FOREACH groupedProducts GENERATE $0,SIZE($1);
store result into 'TotalSalesResult';