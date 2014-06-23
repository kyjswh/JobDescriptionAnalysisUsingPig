REGISTER 'myudfs.jar';
records= LOAD '/user/huser38/Homework5/jobs/20140212_descriptions.csv' USING PigStorage(',') AS (Job_ID:chararray,Description:chararray);

Tokens= FOREACH records GENERATE Job_ID, TOKENIZE(Description) AS token_bag;

Tokens_Final= FOREACH Tokens GENERATE myudfs.Yang_1(*);

Y = LIMIT Tokens_Final 5;

STORE Y INTO 'output';
