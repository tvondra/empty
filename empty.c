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
#include "utils/elog.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "access/heapam.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "miscadmin.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "replication/logical.h"
#include "replication/output_plugin.h"
#include "commands/defrem.h"
#include "foreign/fdwapi.h"
#include "optimizer/pathnode.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/planmain.h"
#include "optimizer/optimizer.h"
#include "commands/explain.h"
#include "optimizer/cost.h"
#include "stdlib.h"
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <stdio.h>

#include "empty.h"

PG_MODULE_MAGIC;

void		_PG_init(void);
void		_PG_output_plugin_init(OutputPluginCallbacks *cb);

PG_FUNCTION_INFO_V1(empty_int4_plus);
PG_FUNCTION_INFO_V1(empty_numeric_plus);
PG_FUNCTION_INFO_V1(empty_random_string);

PG_FUNCTION_INFO_V1(matrix_in);
PG_FUNCTION_INFO_V1(matrix_out);
PG_FUNCTION_INFO_V1(matrix_plus);
PG_FUNCTION_INFO_V1(matrix_multiply);
PG_FUNCTION_INFO_V1(matrix_powers);
PG_FUNCTION_INFO_V1(empty_read_table);
PG_FUNCTION_INFO_V1(empty_fdw_handler);


typedef struct matrix {
	int	n;
	int	values[FLEXIBLE_ARRAY_MEMBER];
} matrix;



typedef struct EmptySharedState {
	LWLock	   *lock;
	int count_notice;
	int count_info;
	int count_error;
	int count_warning;
} EmptySharedState;

static EmptySharedState *emptySharedState = NULL;


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
		int			i;

		memset(nulls, 0, sizeof(nulls));

		values[0] = PG_GETARG_DATUM(0);

		for (i = 0; i < call_cntr; i++)
			values[0] = DirectFunctionCall2(matrix_multiply,
											values[0],
											PG_GETARG_DATUM(0));

		values[1] = call_cntr;

		elog(LOG, "call_cntr = %d", call_cntr);

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

static emit_log_hook_type prev_emit_log_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static void
empty_emit_log_hook(ErrorData *edata)
{
	if (prev_emit_log_hook)
		prev_emit_log_hook(edata);

	if (!emptySharedState)
		return;

	LWLockAcquire(emptySharedState->lock, LW_EXCLUSIVE);

	if (edata->elevel == ERROR)
		emptySharedState->count_error++;
	else if (edata->elevel == WARNING)
		emptySharedState->count_warning++;
	else if (edata->elevel == NOTICE)
		emptySharedState->count_notice++;
	else if (edata->elevel == INFO)
		emptySharedState->count_info++;

	LWLockRelease(emptySharedState->lock);
}

/*
 * Estimate shared memory space needed.
 */
static Size
empty_memsize(void)
{
	return sizeof(EmptySharedState);
}

static void
empty_shmem_startup(void)
{
	bool		found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	emptySharedState = ShmemInitStruct("empty",
						   sizeof(EmptySharedState),
						   &found);

	if (!found)
	{
		/* First time through ... */
		emptySharedState->lock = &(GetNamedLWLockTranche("empty"))->lock;
		emptySharedState->count_notice = 0;
		emptySharedState->count_info = 0;
		emptySharedState->count_error = 0;
		emptySharedState->count_warning = 0;
	}
}

/*
 * Module load callback.
 */
void
_PG_init(void)
{
	RequestAddinShmemSpace(empty_memsize());
	RequestNamedLWLockTranche("empty", 1);

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = empty_shmem_startup;

	prev_emit_log_hook = emit_log_hook;
	emit_log_hook = empty_emit_log_hook;
}

Datum
empty_read_table(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Relation	rel;
	TupleDesc	tdesc;
	HeapTuple	tuple;
	TableScanDesc scan;
	StringInfoData	str;

	initStringInfo(&str);

	rel = relation_open(relid, AccessShareLock);

	tdesc = RelationGetDescr(rel);

	scan = table_beginscan(rel, GetActiveSnapshot(), 0, NULL);

	/* scan the relation */
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		int i;

		for (i = 0; i < tdesc->natts; i++)
		{
			Datum	value;
			bool	isnull;

			value = heap_getattr(tuple, tdesc->attrs[i].attnum,
								 tdesc, &isnull);

			if (isnull)
				appendStringInfo(&str, "%s=(null)", NameStr(tdesc->attrs[i].attname));

			else
			{
				Datum outval;
				Oid		outfunc;
				bool	isvarlena;

				getTypeOutputInfo(tdesc->attrs[i].atttypid, &outfunc, &isvarlena);

				outval = OidFunctionCall1(outfunc, value);

				appendStringInfo(&str, "%s=%s ", NameStr(tdesc->attrs[i].attname),
										 DatumGetCString(outval));

			}
		}

		elog(WARNING, "radek = %s", str.data);
		resetStringInfo(&str);
	}

	table_endscan(scan);

	relation_close(rel, AccessShareLock);

	PG_RETURN_VOID();
}

typedef struct MyDecodingState {
	bool	moje_option;
	int64	cislo;
} MyDecodingState;

static void
empty_startup_cb (struct LogicalDecodingContext *ctx,
										OutputPluginOptions *options,
										bool is_init)
{
	ListCell	*option;
	MyDecodingState *s = palloc(sizeof(MyDecodingState));

	options->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;

	s->moje_option = false;

	foreach(option, ctx->output_plugin_options)
	{
		DefElem    *elem = lfirst(option);

		Assert(elem->arg == NULL || IsA(elem->arg, String));

		if (strcmp(elem->defname, "moje-option") == 0)
		{
			/* if option does not provide a value, it means its value is true */
			if (elem->arg == NULL)
				s->moje_option = true;
			else if (!parse_bool(strVal(elem->arg), &s->moje_option))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
								strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "moje-cislo") == 0)
		{
			s->cislo = atoi(strVal(elem->arg));
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("option \"%s\" = \"%s\" is unknown",
							elem->defname,
							elem->arg ? strVal(elem->arg) : "(null)")));
		}
	}

	ctx->output_plugin_private = s;

	elog(WARNING, "startup");
}

static void
empty_begin_cb (struct LogicalDecodingContext *ctx,
									  ReorderBufferTXN *txn)
{
	elog(WARNING, "begin");
}

static void
empty_change_cb (struct LogicalDecodingContext *ctx,
									   ReorderBufferTXN *txn,
									   Relation relation,
									   ReorderBufferChange *change)
{
	if (change->action == REORDER_BUFFER_CHANGE_INSERT)
	{
		TupleDesc	tdesc = RelationGetDescr(relation);
		ReorderBufferTupleBuf *buf = change->data.tp.newtuple;
		HeapTuple	tuple = &buf->tuple;
		int i;

		OutputPluginPrepareWrite(ctx, true);

		for (i = 0; i < tdesc->natts; i++)
		{
			Datum	value;
			bool	isnull;

			value = heap_getattr(tuple, tdesc->attrs[i].attnum,
								 tdesc, &isnull);

			if (isnull)
				appendStringInfo(ctx->out, "%s=(null)", NameStr(tdesc->attrs[i].attname));

			else
			{
				Datum outval;
				Oid		outfunc;
				bool	isvarlena;

				getTypeOutputInfo(tdesc->attrs[i].atttypid, &outfunc, &isvarlena);

				outval = OidFunctionCall1(outfunc, value);

				appendStringInfo(ctx->out, "%s=%s ", NameStr(tdesc->attrs[i].attname),
										 DatumGetCString(outval));

			}
		}

		OutputPluginWrite(ctx, true);
	}
}

static void
empty_truncate_cb (struct LogicalDecodingContext *ctx,
										 ReorderBufferTXN *txn,
										 int nrelations,
										 Relation relations[],
										 ReorderBufferChange *change)
{
	elog(WARNING, "truncate");
}

static void
empty_commit_cb (struct LogicalDecodingContext *ctx,
									   ReorderBufferTXN *txn,
									   XLogRecPtr commit_lsn)
{
	elog(WARNING, "commit");
}

static void
empty_message_cb (struct LogicalDecodingContext *ctx,
										ReorderBufferTXN *txn,
										XLogRecPtr message_lsn,
										bool transactional,
										const char *prefix,
										Size message_size,
										const char *message)
{
	MyDecodingState *s = (MyDecodingState *) ctx->output_plugin_private;

	if (strcmp(prefix, "empty") == 0)
	{
		OutputPluginPrepareWrite(ctx, true);
		// appendStringInfo(ctx->out, "MESSAGE %s", message);
		appendStringInfo(ctx->out, "MESSAGE %d %ld ", s->moje_option, s->cislo);
		appendBinaryStringInfo(ctx->out, message, message_size);
		OutputPluginWrite(ctx, true);
	}
}

static bool
empty_filter_origin_cb (struct LogicalDecodingContext *ctx,
											   RepOriginId origin_id)
{
	elog(WARNING, "origin");
	return false;
}

static void
empty_shutdown_cb (struct LogicalDecodingContext *ctx)
{
	elog(WARNING, "shutdown");
}

void
_PG_output_plugin_init (OutputPluginCallbacks *cb)
{
	cb->startup_cb = empty_startup_cb;
	cb->begin_cb = empty_begin_cb;
	cb->change_cb = empty_change_cb;
	cb->truncate_cb = empty_truncate_cb;
	cb->commit_cb = empty_commit_cb;
	cb->message_cb = empty_message_cb;
	cb->filter_by_origin_cb = empty_filter_origin_cb;
	cb->shutdown_cb = empty_shutdown_cb;
}

typedef struct CSVScanPrivate {
	char   *directory;
	int64	rows;
	int64	bytes;
	pg_atomic_uint32 *fileno;	/* index of the next file */
} CSVScanPrivate;

static int sample_file(char *filename)
{
	FILE		   *f;
	int				nlines;
	int				nbytes;

	f = fopen(filename, "r");

	nbytes = 0;
	nlines = 0;
	while (!feof(f) && nlines < 1000)
	{
		char			buffer[2048];
		if (fgets(buffer, 2048, f) == buffer)
		{
			nbytes += strlen(buffer);
			nlines++;
		}
	}

	fclose(f);

	return (nbytes / nlines);
}

static int64 total_size(char *directory)
{
	int i = 0;
	int64	total_size = 0;

	while (true)
	{
		struct stat             buf;
		char	filename[1024];
		sprintf(filename, "%s/%d.csv", directory, i);

		if (stat(filename, &buf) != 0)
			break;
		
		total_size += buf.st_size;
		i++;
	}

	return total_size;
}

static void sample_files(char *directory, int64 *rows, int64 *bytes)
{
	char	filename[1024];
	int		row_len;
	int64	size;

	sprintf(filename, "%s/0.csv", directory);

	row_len = sample_file(filename);
	size = total_size(directory);

	*rows = size / row_len;
	*bytes = size;
}

static FILE *next_file(CSVScanPrivate *data)
{
	char	filename[1024];

	int		fileno = pg_atomic_fetch_add_u32(data->fileno, 1);

	sprintf(filename, "%s/%d.csv", data->directory, fileno);

	return fopen(filename, "r");
}

static void empty_GetForeignRelSize (PlannerInfo *root,
											RelOptInfo *baserel,
											Oid foreigntableid)
{
	CSVScanPrivate *data = palloc(sizeof(CSVScanPrivate));

	// hardcoded for now
	data->directory = "/home/user/tmp/data/";

	sample_files(data->directory, &data->rows, &data->bytes);

	/* TODO podivat se na soubor, stat(), odhady podle velikosti ... */
	// alokace struktury (velikost souboru, pocet radek, jmeno souboru ..)
	// ulozit do basrel->fdw_private
	// upravit odhad poctu radku
	baserel->rows = data->rows;
	baserel->fdw_private = data;
}

static void empty_GetForeignPaths (PlannerInfo *root,
										  RelOptInfo *baserel,
										  Oid foreigntableid)
{
	// ForeignPath *path = makeNode(ForeignPath);
	// path->...
	// upravit odhady startup/total podle informaci ze struktury
	CSVScanPrivate *data = (CSVScanPrivate *) baserel->fdw_private;

	Cost	startup = seq_page_cost + cpu_tuple_cost;

	Cost	total = (data->bytes / BLCKSZ) * seq_page_cost +
					(data->rows * cpu_tuple_cost);

	ForeignPath *path = create_foreignscan_path(root, baserel,
											NULL,
											baserel->rows,
											startup,
											total,
											NIL,
											baserel->lateral_relids,
											NULL,
											NULL);

	add_path(baserel, (Path *) path);

	// paralelni cesta
	if (baserel->consider_parallel)
	{
		path = create_foreignscan_path(root, baserel,
												NULL,
												baserel->rows,
												startup,
												total / max_parallel_workers_per_gather,
												NIL,
												baserel->lateral_relids,
												NULL,
												NULL);

		((Path *) path)->parallel_workers = max_parallel_workers_per_gather;
		((Path *) path)->parallel_aware = true;

		add_partial_path(baserel, (Path *) path);
	}
}

static ForeignScan *empty_GetForeignPlan (PlannerInfo *root,
												 RelOptInfo *baserel,
												 Oid foreigntableid,
												 ForeignPath *best_path,
												 List *tlist,
												 List *scan_clauses,
												 Plan *outer_plan)
{
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	return make_foreignscan(tlist,
							scan_clauses,
							baserel->relid,
							NIL,
							baserel->fdw_private,
							NIL,
							NIL,
							outer_plan);
}

static void empty_BeginForeignScan (ForeignScanState *node,
										   int eflags)
{
	ForeignScan *scan = (ForeignScan *) node->ss.ps.plan;
	CSVScanPrivate *data = (CSVScanPrivate *) scan->fdw_private;

	data->fileno = palloc(sizeof(pg_atomic_uint64));
	pg_atomic_init_u32(data->fileno, 0);

	// otevrit soubor (FILE *)
	// ulozit do ForeignScanState->fdw_state
	node->fdw_state = next_file(data);
}

static TupleTableSlot * empty_IterateForeignScan (ForeignScanState *node)
{
	ForeignScan *scan = (ForeignScan *) node->ss.ps.plan;
	CSVScanPrivate *data = (CSVScanPrivate *) scan->fdw_private;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	FILE		   *f = node->fdw_state;
	char			radek[1024];
	char			a[2];
	int				b;
	double			c;

	while (node->fdw_state)
	{
		if (!fgets(radek, 1024, f)) // precti radek z CSV souboru, pokud zadne dalsi radky vrat NULL
		{
			node->fdw_state = next_file(data);
			continue;
		}

		sscanf(radek, "%c,%d,%lf", a, &b, &c);	// parsovani radky se znamym formatem

		// data do -> slot->tts_values/slot->tts_isnull;
		ExecClearTuple(slot);

		a[1] = '\0';
		slot->tts_values[0] = PointerGetDatum(cstring_to_text(a));		// a text
		slot->tts_isnull[0] = false;

		slot->tts_values[1] = Int32GetDatum(b); 	// b int
		slot->tts_isnull[1] = false;

		slot->tts_values[2] = Float8GetDatum(c);	// c double
		slot->tts_isnull[2] = false;

		ExecStoreVirtualTuple(slot);

		return slot;
	}

	return NULL;
}

static void empty_ReScanForeignScan (ForeignScanState *node)
{ }

static void empty_EndForeignScan (ForeignScanState *node)
{
	// zavri soubor
	fclose(node->fdw_state);
}

static void empty_ExplainForeignScan (ForeignScanState *node,
											 struct ExplainState *es)
{
	CSVScanPrivate *data = (CSVScanPrivate *) ((ForeignScan *) node->ss.ps.plan)->fdw_private;

	ExplainPropertyText("directory", data->directory, es);
}

static Size empty_EstimateDSMForeignScan (ForeignScanState *node,
										 ParallelContext *pcxt)
{
	return sizeof(pg_atomic_uint32);
}

static void empty_InitializeDSMForeignScan (ForeignScanState *node,
												   ParallelContext *pcxt,
												   void *coordinate)
{
	CSVScanPrivate *csv = (CSVScanPrivate *) node->fdw_state;

	csv->fileno = shm_toc_allocate(pcxt->toc, sizeof(pg_atomic_uint32));
	pg_atomic_init_u32(csv->fileno, 0);

	shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id, csv->fileno);
}

static void empty_ReInitializeDSMForeignScan (ForeignScanState *node,
													 ParallelContext *pcxt,
													 void *coordinate)
{
}

static void empty_InitializeWorkerForeignScan (ForeignScanState *node,
													  shm_toc *toc,
													  void *coordinate)
{
	CSVScanPrivate *csv = (CSVScanPrivate *) node->fdw_state;

	csv->fileno = shm_toc_lookup(toc, node->ss.ps.plan->plan_node_id, false);
}

static bool empty_IsForeignScanParallelSafe (PlannerInfo *root,
													RelOptInfo *rel,
													RangeTblEntry *rte)
{
	return true;
}


Datum
empty_fdw_handler(PG_FUNCTION_ARGS)
{
	// alokace FdwRoutine
	// FdwRoutine *routine = palloc(sizeof(FdwRoutine));
	// memset(routine, 0, sizeof(FdwRoutine));
	// routine->type = T_FdwRoutine;

	FdwRoutine *routine = makeNode(FdwRoutine);

	// nasetovani callbacku
	routine->GetForeignRelSize = empty_GetForeignRelSize;
	routine->GetForeignPaths = empty_GetForeignPaths;
	routine->GetForeignPlan = empty_GetForeignPlan;
	routine->BeginForeignScan = empty_BeginForeignScan;
	routine->IterateForeignScan = empty_IterateForeignScan;
	routine->ReScanForeignScan = empty_ReScanForeignScan;
	routine->EndForeignScan = empty_EndForeignScan;
	routine->ExplainForeignScan = empty_ExplainForeignScan;

	// paralelni dotazy
	routine->EstimateDSMForeignScan = empty_EstimateDSMForeignScan;
	routine->InitializeDSMForeignScan = empty_InitializeDSMForeignScan;
	routine->ReInitializeDSMForeignScan = empty_ReInitializeDSMForeignScan;
	routine->InitializeWorkerForeignScan = empty_InitializeWorkerForeignScan;
	routine->IsForeignScanParallelSafe = empty_IsForeignScanParallelSafe;

	// vratit jako pointer
	PG_RETURN_POINTER(routine);
}







