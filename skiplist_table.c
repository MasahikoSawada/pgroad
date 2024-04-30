/*-------------------------------------------------------------------------
 *
 * skiptable.c
 *
 * Portions Copyright (c) 2024, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amapi.h"
#include "access/heapam.h"
#include "access/hio.h"
#include "catalog/index.h"
#include "catalog/pg_am.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_type.h"
#include "catalog/storage.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/smgr.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"

#include "road_am.h"
#include "skiplist_table.h"

#define Anum_skip_list_first_row_number		1
#define Anum_skip_list_offset				2
#define Natts_skip_list	2

static void
skiplist_table_name(Relation rel, char *relname, Size sz)
{
	snprintf(relname, sz, "__skiplist_%u", RelationGetRelid(rel));
}

static void
skiplist_index_name(Relation rel, char *relname, Size sz)
{
	snprintf(relname, sz, "__skiplist_%u_index", RelationGetRelid(rel));
}

/*
 * Return a palloc'ed heap tuple for skiplist table.
 */
static HeapTuple
skiplist_make_tuple(Relation skip_rel, RowNumber first_rownum, uint64 offset)
{
	HeapTuple	htup;
	Datum		values[Natts_skip_list];
	bool		nulls[Natts_skip_list];

	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	values[Anum_skip_list_first_row_number - 1] = Int64GetDatum(first_rownum);
	values[Anum_skip_list_offset - 1] = UInt64GetDatum(offset);;

	htup = heap_form_tuple(RelationGetDescr(skip_rel), values, nulls);

	htup->t_data->t_infomask &= ~(HEAP_XACT_MASK);
	htup->t_data->t_infomask |= HEAP_XMIN_FROZEN;
	htup->t_data->t_infomask2 &= ~(HEAP2_XACT_MASK);

	HeapTupleHeaderSetCmin(htup->t_data, InvalidCommandId);
	HeapTupleHeaderSetXmax(htup->t_data, 0);	/* for cleanliness */
	HeapTupleHeaderSetXmin(htup->t_data, InvalidTransactionId);

	return htup;
}

void
skiplist_insert_entry(Oid skip_relid, RowNumber first_rownum, uint64 offset)
{
	Relation	skip_rel;
	HeapTuple	tuple;

	skip_rel = table_open(skip_relid, RowExclusiveLock);

	tuple = skiplist_make_tuple(skip_rel, first_rownum, offset);
	CatalogTupleInsert(skip_rel, tuple);

	table_close(skip_rel, NoLock);
	heap_freetuple(tuple);
}

Relation
skiplist_table_open(Relation rel, LOCKMODE lockmode)
{
	char		skiplist_relname[NAMEDATALEN];
	Oid			relid;

	skiplist_table_name(rel, skiplist_relname, sizeof(skiplist_relname));

	relid = get_relname_relid(skiplist_relname, rel->rd_rel->relnamespace);
	if (!OidIsValid(relid))
		elog(ERROR, "could not open table %s", skiplist_relname);

	return table_open(relid, lockmode);
}

Oid
skiplist_create_for_rel(Relation rel)
{
	Relation	skip_rel;
	Oid			relid = RelationGetRelid(rel);
	Oid			skip_relid,
				skip_idxoid;
	char		skip_relname[NAMEDATALEN];
	char		skip_idxname[NAMEDATALEN];
	ObjectAddress baseobject,
				toastobject;
	TupleDesc	tupdesc;
	IndexInfo  *indexInfo;
	Oid			collationIds[1];
	Oid			opclassIds[1];
	int16		coloptions[1];
#if PG_VERSION_NUM >= 170000
	NullableDatum	stattargets[1];
#endif

	ROAD_DEBUG_LOG("create skiplist table for rel %u", RelationGetRelid(rel));

	skiplist_table_name(rel, skip_relname, sizeof(skip_relname));
	skiplist_index_name(rel, skip_idxname, sizeof(skip_idxname));

	tupdesc = CreateTemplateTupleDesc(Natts_skip_list);
	TupleDescInitEntry(tupdesc, (AttrNumber) Anum_skip_list_first_row_number,
					   "first_row_number",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) Anum_skip_list_offset,
					   "offset",
					   INT8OID, -1, 0);

	skip_relid = heap_create_with_catalog(skip_relname,
										  RelationGetNamespace(rel),
										  rel->rd_rel->reltablespace,
										  InvalidOid,
										  InvalidOid,
										  InvalidOid,
										  rel->rd_rel->relowner,
										  HEAP_TABLE_AM_OID,
										  tupdesc,
										  NIL,
										  RELKIND_RELATION,
										  rel->rd_rel->relpersistence,
										  false,
										  false,
										  ONCOMMIT_NOOP,
										  0,
										  false,
										  true,
										  true,
										  InvalidOid,
										  NULL);
	Assert(OidIsValid(skip_relid));

	/* make the chunkentry relation visible */
	CommandCounterIncrement();

	/* record dependency between archive table and skiplist table */
	baseobject.classId = RelationRelationId;
	baseobject.objectId = relid;
	baseobject.objectSubId = 0;
	toastobject.classId = RelationRelationId;
	toastobject.objectId = skip_relid;
	toastobject.objectSubId = 0;
	recordDependencyOn(&toastobject, &baseobject, DEPENDENCY_INTERNAL);

	CommandCounterIncrement();

	skip_rel = table_open(skip_relid, ShareLock);

	/*
	 * Create unique index on first_row_number.
	 */
	indexInfo = makeNode(IndexInfo);
	indexInfo->ii_NumIndexAttrs = 1;
	indexInfo->ii_NumIndexKeyAttrs = 1;
	indexInfo->ii_IndexAttrNumbers[0] = 1;
	indexInfo->ii_Expressions = NIL;
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_Predicate = NIL;
	indexInfo->ii_PredicateState = NULL;
	indexInfo->ii_ExclusionOps = NULL;
	indexInfo->ii_ExclusionProcs = NULL;
	indexInfo->ii_ExclusionStrats = NULL;
	indexInfo->ii_Unique = true;
	indexInfo->ii_NullsNotDistinct = false;
	indexInfo->ii_ReadyForInserts = true;
	indexInfo->ii_CheckedUnchanged = false;
	indexInfo->ii_IndexUnchanged = false;
	indexInfo->ii_Concurrent = false;
	indexInfo->ii_BrokenHotChain = false;
	indexInfo->ii_ParallelWorkers = 0;
	indexInfo->ii_Am = BTREE_AM_OID;
	indexInfo->ii_AmCache = NULL;
	indexInfo->ii_Context = CurrentMemoryContext;

	collationIds[0] = InvalidOid;

	opclassIds[0] = INT8_BTREE_OPS_OID;

	coloptions[0] = 0;

#if PG_VERSION_NUM >= 170000
	stattargets[0].value = 0;
	stattargets[0].isnull = false;
#endif

	skip_idxoid = index_create(skip_rel,
							   skip_idxname,
							   InvalidOid,	/* indexRelationId */
							   InvalidOid,	/* parentIndexRelid */
							   InvalidOid,	/* parentConstraintId */
							   InvalidRelFileNumber,	/* relFileNumber */
							   indexInfo,
							   list_make1("skiplist_first_rownum"),
							   BTREE_AM_OID,
							   rel->rd_rel->reltablespace,
							   collationIds,
							   opclassIds,
#if PG_VERSION_NUM >= 170000
							   NULL,	/* opclassOptions */
#endif
							   coloptions,
#if PG_VERSION_NUM >= 170000
							   stattargets,
#endif
							   (Datum) 0,	/* reloptions */
							   INDEX_CREATE_IS_PRIMARY,
							   0,
							   false,	/* allow_system_table_mods */
							   true,	/* is_internal */
							   NULL);

	table_close(skip_rel, NoLock);

	CommandCounterIncrement();

	/* Record dependency between skiplist-table and skiplist-index */
	baseobject.classId = RelationRelationId;
	baseobject.objectId = skip_relid;
	baseobject.objectSubId = 0;
	toastobject.classId = RelationRelationId;
	toastobject.objectId = skip_idxoid;
	toastobject.objectSubId = 0;
	recordDependencyOn(&toastobject, &baseobject, DEPENDENCY_INTERNAL);

	CommandCounterIncrement();

	return skip_relid;
}

/*
 * Search the skiplist table entry that could have the given rownum,
 * and return its offset and first_rownum.
 */
int64
skiplist_get_chunk_offset_for_rownum(Relation rel, RowNumber rownum,
									 Snapshot snapshot,
									 RowNumber * first_rownum)
{
	Relation	skip_rel;
	ScanKeyData skey[1];
	Oid			skip_indrelid;
	char		skip_idxname[NAMEDATALEN];
	SysScanDesc desc;
	HeapTuple	tuple;
	Datum		values[Natts_skip_list];
	bool		nulls[Natts_skip_list];
	uint64		offset;

	skip_rel = skiplist_table_open(rel, AccessShareLock);

	skiplist_index_name(rel, skip_idxname, sizeof(skip_idxname));
	skip_indrelid = get_relname_relid(skip_idxname,
									  RelationGetNamespace(skip_rel));
	if (!OidIsValid(skip_indrelid))
		elog(ERROR, "could not find index \"%s\"", skip_idxname);

	ScanKeyInit(&skey[0], Anum_skip_list_first_row_number,
				BTLessEqualStrategyNumber, F_INT8LE,
				Int64GetDatum(rownum));
	desc = systable_beginscan(skip_rel, skip_indrelid, true,
							  snapshot, 1, skey);

	tuple = systable_getnext_ordered(desc, BackwardScanDirection);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "could not find skiplist table tuple for rownum %lu",
			 rownum);

	heap_deform_tuple(tuple, RelationGetDescr(skip_rel), values, nulls);

	/* Get the tuple data */
	offset = DatumGetUInt64(values[Anum_skip_list_offset - 1]);
	*first_rownum =
		DatumGetUInt64(values[Anum_skip_list_first_row_number - 1]);

	systable_endscan(desc);

	table_close(skip_rel, NoLock);

	return offset;
}
