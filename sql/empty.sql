CREATE EXTENSION empty;

SELECT '(1 1 1)'::matrix;

SELECT '(1 1 1 a)'::matrix;

SELECT '(1 1 1 2)'::matrix + '(2 3 4 5)'::matrix;
