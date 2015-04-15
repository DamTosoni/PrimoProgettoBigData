REGISTER myudfs.jar;

sales = LOAD 'Sales.txt' USING PigStorage('\n');

/*Genero tutte le possibili coppie*/
coppie = FOREACH sales GENERATE myudfs.CreateCouples($0);


store coppie into 'LessThen5';

