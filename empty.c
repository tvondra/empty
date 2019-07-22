/*-------------------------------------------------------------------------
 *
 * empty.c
 *	  empty extension
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/numeric.h"

#include "empty.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(empty_int4_plus);
PG_FUNCTION_INFO_V1(empty_numeric_plus);
PG_FUNCTION_INFO_V1(empty_random_string);

PG_FUNCTION_INFO_V1(matrix_in);
PG_FUNCTION_INFO_V1(matrix_out);

typedef struct matrix {
	int	n;
	int	values[FLEXIBLE_ARRAY_MEMBER];
} matrix;

static int
matrix_len(int n)
{
	return (offsetof(matrix, values) + (n*n) * sizeof(int));
}

Datum
matrix_in(PG_FUNCTION_ARGS)
{
	int		p, q, r, s;
	int		len;
	char   *a = PG_GETARG_CSTRING(0);
	char   *v;
	matrix *m;

	if (sscanf(a, "(%d %d %d %d)", &p, &q, &r, &s) != 4)
		elog(ERROR, "kabooom!");

	len = matrix_len(2);

	v = palloc(VARHDRSZ + len);
	SET_VARSIZE(v, VARHDRSZ + len);
	m = (matrix *) VARDATA(v);

	m->n = 2;
	m->values[0] = p;
	m->values[1] = q;
	m->values[2] = r;
	m->values[3] = s;

	PG_RETURN_POINTER(v);
}

Datum
matrix_out(PG_FUNCTION_ARGS)
{
	char   *v = PG_GETARG_POINTER(0);
	matrix *m = (matrix *) VARDATA(v);
	char   *buff;

	buff = palloc(1024);

	sprintf(buff, "(%d %d %d %d)", m->values[0], m->values[1], m->values[2], m->values[3]);

	PG_RETURN_CSTRING(buff);
}


Datum
empty_int4_plus(PG_FUNCTION_ARGS)
{
	int32	a = PG_GETARG_INT32(0);
	int32	b = PG_GETARG_INT32(1);

	PG_RETURN_INT32(a+b);
}

Datum
empty_numeric_plus(PG_FUNCTION_ARGS)
{
	Datum c;

	Datum	a = PG_GETARG_DATUM(0);
	Datum	b = PG_GETARG_DATUM(1);

	elog(LOG, "a = %s", DatumGetCString(DirectFunctionCall1(numeric_out, a)));
	elog(LOG, "b = %s", DatumGetCString(DirectFunctionCall1(numeric_out, b)));

	c = DirectFunctionCall2(numeric_add, a, b);

	PG_RETURN_NUMERIC(c);
}

Datum
empty_random_string(PG_FUNCTION_ARGS)
{
	int		i;
	int32	len = PG_GETARG_INT32(0);
	char	values[] = {'a', 'b', 'c', 'd', 'e', 'f' };
	char   *rand_value;
	text   *v;

	if (len <= 0)
		PG_RETURN_NULL();

	rand_value = palloc(sizeof(char) * (len + 1));

	for (i = 0; i < len; i++)
		rand_value[i] = values[(random() % 6)];

	rand_value[len] = '\0';

	v = palloc(VARHDRSZ + strlen(rand_value));
	SET_VARSIZE(v, VARHDRSZ + strlen(rand_value));
	memcpy(VARDATA(v), rand_value, strlen(rand_value));

	// VARDATA_ANY
	// VARSIZE_ANY
	// VARSIZE_ANY_EXHDR
	// rand_value = VARDATA(v);

	// PG_RETURN_TEXT_P(cstring_to_text(rand_value));
	PG_RETURN_TEXT_P(v);
}
