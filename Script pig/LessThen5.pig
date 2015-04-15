REGISTER myudfs.jar;

sales = LOAD 'Sales.txt' USING PigStorage('\n');

/*Genero tutte le possibili combinazioni per ogni riga*/
combinations = FOREACH sales GENERATE myudfs.CreateCombinations($0);


store combinations into 'LessThen5';
