sales = LOAD 'Sales.txt' USING PigStorage('\n');

/*Genero tutte le possibili coppie*/
flattenSales = FOREACH sales GENERATE FLATTEN(TOKENIZE($0)),$0;

/* Filtro i risultati per togliere le date dai prodotti */
filteredSales = FILTER flattenSales BY NOT(STARTSWITH($0,'2'));
filteredSalesCopy = FILTER flattenSales BY NOT(STARTSWITH($0,'2'));;

/* Eseguo il JOIN per costruire le coppie di prodotti */
salesJoined = JOIN filteredSales by $1, filteredSalesCopy by $1;

/* Filtro i prodotti per eliminare le coppie di prodotti uguali e quelle invertite */
salesJoinedFiltered = FILTER salesJoined BY $0!=$2 AND $0<$2;

/* Genero le coppie e le raggruppo */
couples = FOREACH salesJoinedFiltered GENERATE SPRINTF('%s,%s',$0,$2),$1;
groupedCouples = GROUP couples BY ($0);
couplesSales = FOREACH groupedCouples GENERATE $0,SIZE($1);

/* Ordino, filtro e costruisco l'output */
orderedCouplesSales = ORDER couplesSales BY $1 desc;
limitedCouplesSales = LIMIT orderedCouplesSales 10;
top10Sales =  FOREACH limitedCouplesSales GENERATE SPRINTF('%s,%s',$0,$1);

store top10Sales into 'Top10SalesResult';