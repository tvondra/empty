/* empty--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION empty" to load this file. \quit

CREATE FUNCTION int4_plus(a int4, b int4)
	RETURNS int4
	AS 'MODULE_PATHNAME', 'empty_int4_plus'
	LANGUAGE C
	IMMUTABLE STRICT
PARALLEL SAFE;

CREATE FUNCTION numeric_plus(a numeric, b numeric)
	RETURNS numeric
	AS 'MODULE_PATHNAME', 'empty_numeric_plus'
	LANGUAGE C
	IMMUTABLE STRICT
PARALLEL SAFE;

CREATE FUNCTION random_string(len int)
	RETURNS text
	AS 'MODULE_PATHNAME', 'empty_random_string'
	LANGUAGE C
	STRICT
PARALLEL SAFE;


CREATE TYPE matrix;

CREATE FUNCTION matrix_in(cstring)
	RETURNS matrix
	AS 'MODULE_PATHNAME', 'matrix_in'
	LANGUAGE C
	IMMUTABLE STRICT
PARALLEL SAFE;

CREATE FUNCTION matrix_out(matrix)
	RETURNS cstring
	AS 'MODULE_PATHNAME', 'matrix_out'
	LANGUAGE C
	IMMUTABLE STRICT
PARALLEL SAFE;

CREATE TYPE matrix (
    INPUT = matrix_in,
    OUTPUT = matrix_out
);

CREATE FUNCTION matrix_plus(matrix, matrix)
	RETURNS matrix
	AS 'MODULE_PATHNAME', 'matrix_plus'
	LANGUAGE C
	IMMUTABLE STRICT
PARALLEL SAFE;

CREATE OPERATOR + (
	FUNCTION = matrix_plus,
	LEFTARG = matrix,
	RIGHTARG = matrix
);

CREATE FUNCTION matrix_multiply(matrix, matrix)
	RETURNS matrix
	AS 'MODULE_PATHNAME', 'matrix_multiply'
	LANGUAGE C
	IMMUTABLE STRICT
PARALLEL SAFE;

CREATE OPERATOR * (
	FUNCTION = matrix_multiply,
	LEFTARG = matrix,
	RIGHTARG = matrix
);

-- CREATE FUNCTION array_to_matrix(int[][]) RETURNS matrix ...;
-- CREATE CAST (int[][] as matrix) WITH FUNCTION array_to_matrix(int[][]);

CREATE FUNCTION matrix_powers(matrix, int, out matrix, out int)
	RETURNS SETOF record
	AS 'MODULE_PATHNAME', 'matrix_powers'
	LANGUAGE C
	IMMUTABLE STRICT
PARALLEL SAFE;




CREATE FUNCTION read_table(regclass)
	RETURNS void
	AS 'MODULE_PATHNAME', 'empty_read_table'
	LANGUAGE C
	IMMUTABLE STRICT
PARALLEL SAFE;


CREATE FUNCTION empty_fdw_handler() RETURNS fdw_handler
	AS 'MODULE_PATHNAME', 'empty_fdw_handler'
	LANGUAGE C;

CREATE FOREIGN DATA WRAPPER empty_fdw HANDLER empty_fdw_handler;
