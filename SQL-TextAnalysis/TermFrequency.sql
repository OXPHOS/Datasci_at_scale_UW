--Term-document matrix is an important representation for text analytics
--Each row of the matrix is a document vector, with one column for every term in the entire corpus.
--Naturally, some documents may not contain a given term, so this atrix is rather sparse. 
--The value in each cell of the matrix is the term frequency.

--"tf-idf": term frequency - inverse document frequency. Weighted term frequency

--S = D1D2T is the similarity between matrix D1 and D2. 
--Can be normailzed to range(0, 1) dividing the dot product by the magnitude of the two vectors

--load reuters.db with table: frequency(docid, term, count)



/* Compute the similarity matrix of the two documents '10080_txt_crude' and '17035_txt_earn'
 * Isolate each document vector firet
 */
CREATE TEMP VIEW vec1 AS
SELECT term, count FROM frequency
WHERE docid="10080_txt_crude";

CREATE TEMP VIEW vec2 AS
SELECT term, count FROM frequency 
WHERE docid="17035_txt_earn";

SELECT SUM(value) FROM (
    SELECT (vec1.count*vec2.count) AS value FROM
    vec1 JOIN vec2
    ON vec1.term=vec2.term
)x;

DROP VIEW vec1;
DROP VIEW vec2;

/* Find the best matching document to the keyword query "washington taxes treasury". 
 * Use sparse matrix multiply
 */
SELECT *, MAX(similarity) FROM (    /* This selects entire row + extra similarity. Need improve */
    SELECT a.docid,  b.docid, SUM(a.count*b.count) as similarity FROM
        (SELECT * FROM frequency
        UNION
        SELECT 'q' as docid, 'washington' as term, 1 as count 
        UNION
        SELECT 'q' as docid, 'taxes' as term, 1 as count
        UNION 
        SELECT 'q' as docid, 'treasury' as term, 1 as count) a
    JOIN
        (SELECT * FROM frequency a
        UNION
        SELECT 'q' as docid, 'washington' as term, 1 as count 
        UNION
        SELECT 'q' as docid, 'taxes' as term, 1 as count
        UNION 
        SELECT 'q' as docid, 'treasury' as term, 1 as count) b
    ON a.term=b.term
    WHERE a.docid='q'
    AND b.docid <> 'q'
    GROUP BY a.docid, b.docid
);
    

    