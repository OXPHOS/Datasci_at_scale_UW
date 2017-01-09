--UW Assignment2
--load reuters.db with table: frequency(docid, term, count)
--$ sqlite3 reuters.db SQLBasics.sql

/* (a) select: 
 * Write a query that is equivalent to the following relational algebra expression.
 * σdocid=10398_txt_earn(frequency)
 */
SELECT COUNT(*) FROM (
    SELECT * FROM frequency WHERE docid="10398_txt_earn"
)x;
/* COUNT(*) counts the entries number */


/* (b) select project: 
 * Write a query that is equivalent to the following relational algebra expression.
 * πterm(σdocid=10398_txt_earn and count=1(frequency))
 */
SELECT COUNT(*) FROM(
    SELECT term FROM frequency
    WHERE docid="10398_txt_earn" AND count=1
)x;


/* (c) union: 
 * Write a query that is equivalent to the following relational algebra expression. 
 * (Hint: you can use the UNION keyword in SQL)
 * πterm(σdocid=10398_txt_earn and count=1(frequency)) U πterm(σdocid=925_txt_trade and count=1(frequency))
 */
SELECT COUNT(*) FROM(
    SELECT term FROM frequency
    WHERE docid="10398_txt_earn" AND count=1
    UNION
    SELECT term FROM frequency
    WHERE docid="925_txt_trade" AND count=1
)x;


/* (d) count
 * count the number of unique documents containing the word "law" or containing 
 * the word "legal" (If a document contains both law and legal, it should only 
 * be counted once) 
 */
SELECT COUNT(*) FROM(
    SELECT DISTINCT docid FROM frequency WHERE term="law"
    UNION
    SELECT DISTINCT docid FROM frequency WHERE term="legal"
)x;


/* (e) big documents
 * Write a SQL statement to find all documents that have more than 300 total terms,
 * including duplicate terms. (Hint: You can use the HAVING clause, or you can 
 * use a nested query. Another hint: Remember that the count column contains the 
 * term frequencies, and you want to consider duplicates.) (docid, term_count)
 */
SELECT COUNT(*) FROM (
    SELECT * FROM (
        SELECT docid, SUM(count) AS total_count FROM frequency
        GROUP BY docid
    ) WHERE total_count > 300
);

/* or using HAVING */
SELECT COUNT(*) FROM (
    SELECT docid, SUM(count) AS total_count FROM frequency
    GROUP BY docid
    HAVING total_count > 300
);



/* (f) two words: 
 * Write a SQL statement to count the number of unique documents that contain 
 * both the word 'transactions' and the word 'world'. (Hint: Find the docs that 
 * contain one word and the docs that contain the other word separately, 
 * then find the intersection.)
 */
 
SELECT COUNT(*) FROM 
    (SELECT docid FROM frequency
    WHERE term="transactions") table1
JOIN
    (SELECT docid FROM frequency
    WHERE term="world") table2
ON table1.docid=table2.docid;
