sales = LOAD 'Sales.txt' USING PigStorage('\n');
flattenSales = FOREACH sales GENERATE FLATTEN(TOKENIZE($0)),$0;
filteredSales = FILTER flattenSales BY NOT(STARTSWITH($0,'2'));

/* Prendo le vendite relative ai vari mesi del trimestre */
filteredSalesOnlyFirstMonth = FILTER filteredSales BY STARTSWITH($1,'2015-1-');
filteredSalesOnlySecondMonth = FILTER filteredSales BY STARTSWITH($1,'2015-2-');
filteredSalesOnlyThirdMonth = FILTER filteredSales BY STARTSWITH($1,'2015-3-');

/* Raggruppo i mesi per il nome del prodotto */
groupedProductsFirstMonth = GROUP filteredSalesOnlyFirstMonth BY $0;
groupedProductsSecondMonth = GROUP filteredSalesOnlySecondMonth BY $0;
groupedProductsThirdMonth = GROUP filteredSalesOnlyThirdMonth BY $0;

/* Trovo le vendite totali per mese per ogni prodotto */
resultFirstMonth = FOREACH groupedProductsFirstMonth GENERATE $0,SPRINTF('1/2015:%d',SIZE($1));
resultSecondMonth = FOREACH groupedProductsSecondMonth GENERATE $0,SPRINTF('2/2015:%d',SIZE($1));
resultThirdMonth = FOREACH groupedProductsThirdMonth GENERATE $0,SPRINTF('3/2015:%d',SIZE($1));

/* Eseguo il join delle tre relazioni per ottenere il risultato finale */
resultTwoMonths = JOIN resultFirstMonth BY $0 FULL, resultSecondMonth BY $0;
result = JOIN resultTwoMonths BY $0 FULL, resultThirdMonth BY $0;

formattedResult = FOREACH result GENERATE ($0 is not null?$0:($2 is not null?$2:$4)),($1 is not null?$1:'1/2015:0'),($3 is not null?$3:'2/2015:0'),($5 is not null?$5:'3/2015:0');

store formattedResult into 'SalesTrendResult';
