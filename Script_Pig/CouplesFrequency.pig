sales = LOAD 'Sales.txt' USING PigStorage('\n');

/*Genero tutte le possibili coppie*/
flattenSales = FOREACH sales GENERATE FLATTEN(TOKENIZE($0)),$0;

/* Filtro i risultati per togliere le date dai prodotti */
filteredSales = FILTER flattenSales BY NOT(STARTSWITH($0,'2'));
filteredSalesCopy = FILTER flattenSales BY NOT(STARTSWITH($0,'2'));;

/* Eseguo il JOIN per costruire le coppie di prodotti */
salesJoined = JOIN filteredSales by $1, filteredSalesCopy by $1;

/* Filtro i prodotti per eliminare le coppie di prodotti uguali */
salesJoinedFiltered = FILTER salesJoined BY $0!=$2;

/* Genero le coppie e le raggruppo */
couples = FOREACH salesJoinedFiltered GENERATE $0,$2,$1;
groupedCouples = GROUP couples BY ($0,$1);
couplesSales = FOREACH groupedCouples GENERATE $0.$0,$0.$1,SIZE($1);

/* Adesso ottengo le vendite per ogni prodotto */
groupedProducts = GROUP filteredSales BY $0;
totalSalesPerProduct = FOREACH groupedProducts GENERATE $0,SIZE($1);

/* Unisco le vendite con le coppie di prodotti */
couplesAndTotalSales = JOIN couplesSales by $0, totalSalesPerProduct by $0;
couplesFrequency = FOREACH couplesAndTotalSales GENERATE SPRINTF('%s,%s',$0,$1),(double) $2/$4;

/* Ordino e filtro */
orderedCouplesFrequency = ORDER couplesFrequency BY $1 desc;
couplesFrequencyResult = LIMIT orderedCouplesFrequency 10;

store couplesFrequencyResult into 'CouplesFrequencyResult';

