--Sparse Matrix Product with Matrix A and B, A*B
--Load database matrix.db with table: A and B.
--A(row_num, col_num, value),
--B(row_num, col_num, value)


SELECT row, col, SUM(prod) FROM (
    SELECT A.row_num AS row,
           B.col_num AS col,
           A.value * B.value AS prod
    FROM A JOIN B
    ON A.col_num=B.row_num 
) GROUP BY row, col;


--TEST
CREATE TABLE mat1(
    row_num INT NOT NULL,
    col_num INT NOT NULL,
    value INT NOT NULL
);
INSERT INTO mat1(row_num, col_num, value)
VALUES(0, 0, 1);
INSERT INTO mat1(row_num, col_num, value)
VALUES(0, 2, 3);
INSERT INTO mat1(row_num, col_num, value)
VALUES(1, 1, 2);
INSERT INTO mat1(row_num, col_num, value)
VALUES(2, 1, 2);

CREATE TABLE mat2(
    row_num INT NOT NULL,
    col_num INT NOT NULL,
    value INT NOT NULL
);
INSERT INTO mat2(row_num, col_num, value)
VALUES(0, 1, 1);
INSERT INTO mat2(row_num, col_num, value)
VALUES(1, 0, 5);
INSERT INTO mat2(row_num, col_num, value)
VALUES(2, 1, 3);
INSERT INTO mat2(row_num, col_num, value)
VALUES(2, 2, 4);

SELECT row, col, SUM(prod) FROM (
    SELECT mat1.row_num AS row,
           mat2.col_num AS col,
           mat1.value * mat2.value AS prod
    FROM (
        mat1 JOIN mat2
        on mat1.col_num=mat2.row_num 
    ) 
) GROUP BY row, col;

DROP TABLE mat1;
DROP TABLE mat2;