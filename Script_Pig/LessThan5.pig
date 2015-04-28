REGISTER myudfs.jar;

sales = LOAD 'Sales.txt' USING PigStorage('\n');

/*Genero tutte le possibili combinazioni per ogni riga*/
combinations = FOREACH sales GENERATE myudfs.CreateCombinations($0);
combinationsTokenized = FOREACH combinations GENERATE FLATTEN(TOKENIZE ($0,'%'));

/*Lascio solo le combinazioni esistenti*/
combinations2sales = CROSS combinationsTokenized, sales;
combinations2salesDistinct = DISTINCT combinations2sales;
combinationsChecked = FOREACH combinations2salesDistinct GENERATE myudfs.PresenceCheck($0,$1);
combinationsFlattened = FOREACH combinationsChecked GENERATE FLATTEN($0);

/*Conto*/
combinationsFiltered = FILTER combinationsFlattened BY NOT($0 matches ' ');
combinationsGrouped = GROUP combinationsFiltered BY $0;
combinationsGrouped2salesLists = FOREACH combinationsGrouped GENERATE $0,SIZE($1);

/*Ordino*/
result = ORDER combinationsGrouped2salesLists BY $1 desc;
limitedResult = LIMIT result 10;

store limitedResult into 'LessThan5Result';
