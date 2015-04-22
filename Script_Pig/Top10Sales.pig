sales = LOAD 'Sales.txt' USING PigStorage('\n');

/*Genero tutte le possibili coppie*/
flattenSales = FOREACH sales GENERATE FLATTEN(TOKENIZE($0)),$0;
filteredSales = FILTER flattenSales BY NOT(STARTSWITH($0,'2'));
groupedProducts = GROUP filteredSales BY $0;
products = FOREACH groupedProducts GENERATE $0;
products2 = FOREACH groupedProducts GENERATE $0;
couples = CROSS products, products2;
couplesFiltered = FILTER couples BY NOT($0 == $1);

/*Affianco ad ogni coppia tutte le vendite*/
flattenSalesLists = DISTINCT (FOREACH flattenSales GENERATE $1);
couples2salesLists = CROSS couplesFiltered, flattenSalesLists;

/*Lascio solo le coppie esistenti*/
existingCouples = FILTER couples2salesLists BY ($2 matches SPRINTF('.*%s.*%s.*',$0,$1)) OR ($2 matches  SPRINTF('.*%s.*%s.*',$1,$0));

/*Conto*/
existingCouplesGrouped = GROUP existingCouples BY ($0,$1);
couplesGrouped2salesLists = FOREACH existingCouplesGrouped GENERATE $0,SIZE($1);

/*Ordino*/
sorted = ORDER couplesGrouped2salesLists BY $1 desc;

/*Elimino i duplicati*/
flattenedSorted = FOREACH sorted GENERATE FLATTEN($0),$1;

/*Ordino alfabeticamente i primi due campi in modo da poter poi filtrare*/
alphabethicallySorted = FOREACH flattenedSorted GENERATE ($0 < $1? ($0,$1,$2):($1,$0,$2));
distincted = DISTINCT (FOREACH alphabethicallySorted GENERATE FLATTEN($0));
orderedDistincted = ORDER distincted BY $2 desc;

/*Limito ai primi 10*/
limited = LIMIT orderedDistincted 10;

result = FOREACH limited GENERATE SPRINTF('%s,%s,%s',$0,$1,$2);

store result into 'Top10SalesResult';

