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
PG_FUNCTION_INFO_V1(matrix_plus);
PG_FUNCTION_INFO_V1(matrix_multiply);
PG_FUNCTION_INFO_V1(matrix_powers);

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
matrix_plus(PG_FUNCTION_ARGS)
{
	char   *v1 = PG_GETARG_POINTER(0);
	char   *v2 = PG_GETARG_POINTER(1);
	char   *v3;

	matrix *m1 = (matrix *) VARDATA(v1);
	matrix *m2 = (matrix *) VARDATA(v2);
	matrix *m3;

	v3 = palloc(VARHDRSZ + matrix_len(2));
	SET_VARSIZE(v3, VARHDRSZ + matrix_len(2));
	m3 = (matrix *) VARDATA(v3);

	m3->values[0] = m1->values[0] + m2->values[0];
	m3->values[1] = m1->values[1] + m2->values[1];
	m3->values[2] = m1->values[2] + m2->values[2];
	m3->values[3] = m1->values[3] + m2->values[3];

	PG_RETURN_CSTRING(v3);
}

Datum
matrix_multiply(PG_FUNCTION_ARGS)
{
	char   *v1 = PG_GETARG_POINTER(0);
	char   *v2 = PG_GETARG_POINTER(1);
	char   *v3;

	matrix *m1 = (matrix *) VARDATA(v1);
	matrix *m2 = (matrix *) VARDATA(v2);
	matrix *m3;

	v3 = palloc(VARHDRSZ + matrix_len(2));
	SET_VARSIZE(v3, VARHDRSZ + matrix_len(2));
	m3 = (matrix *) VARDATA(v3);

	m3->values[0] = m1->values[0] * m2->values[0] + m1->values[1] * m2->values[2];
	m3->values[1] = m1->values[0] * m2->values[1] + m1->values[1] * m2->values[3];
	m3->values[2] = m1->values[2] * m2->values[0] + m1->values[3] * m2->values[2];
	m3->values[3] = m1->values[2] * m2->values[1] + m1->values[3] * m2->values[3];

	PG_RETURN_CSTRING(v3);
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


Datum
matrix_powers(PG_FUNCTION_ARGS)
{
	FuncCallContext	 *funcctx;
	int				  call_cntr;
	int				  max_calls;
	TupleDesc			tupdesc;

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext   oldcontext;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* total number of tuples to be returned */
		funcctx->max_calls = PG_GETARG_INT32(1);

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("function returning record called in context "
							"that cannot accept type record")));

		funcctx->tuple_desc = tupdesc;

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	call_cntr = funcctx->call_cntr;
	max_calls = funcctx->max_calls;

	if (call_cntr < max_calls)	/* do when there is more left to send */
	{
		Datum		values[2];
		bool		nulls[2];
		HeapTuple	tuple;
		Datum		result;

		memset(nulls, 0, sizeof(nulls));

		values[0] = PointerGetDatum(PG_GETARG_POINTER(0));
		values[1] = call_cntr;

		/* Build and return the tuple. */
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	else	/* do when there is no more left */
	{
		SRF_RETURN_DONE(funcctx);
	}
}
