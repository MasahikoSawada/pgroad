/*-------------------------------------------------------------------------
 *
 * road_am.c
 *
 * Portions Copyright (c) 2024, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amapi.h"
#include "access/heapam.h"
#include "access/hio.h"
#include "access/tableam.h"
#include "access/multixact.h"
#include "access/heaptoast.h"
#include "access/xloginsert.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/heap.h"
#include "catalog/pg_am.h"
#include "catalog/storage.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "lib/dshash.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/inval.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "tcop/utility.h"
#include "pgstat.h"

#include "road_am.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(road_am_handler);

#define FEATURE_NOT_SUPPORTED() \
	ereport(ERROR, \
		errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
			errmsg("%s is not supported", __func__))

typedef struct RoadInsertStateData
{
	Oid		oid;

	char	chunk_page[ROAD_CHUNK_PAGE_SIZE];

} RoadInsertStateData;

typedef struct RoadStateData
{
	HTAB	*insert_states;

	/*
	 * Flags if ALTER TABLE ... SET ACCESS METHOD or CREATE TABLE ... AS
	 * is called. This is necessary to control whether we can allow
	 * insertion to the ROAD table.
	 *
	 * XXX: if we find a better way to distinguish how road_insert_tuple
	 * is called, we don't need both variables (as well as probably
	 * ProcessUtility hook).
	 */
	bool	called_in_atsam;
	bool	called_in_ctas;
} RoadStateData;
RoadStateData RoadState = {0};

static void RoadXactCallback(XactEvent event, void *arg);
static void RoadSubXactCallback(SubXactEvent event, SubTransactionId mySubid,
								SubTransactionId parentSubid, void *arg);
static void road_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
								bool readOnlyTree,
								ProcessUtilityContext context, ParamListInfo params,
								QueryEnvironment *queryEnv,
								DestReceiver *dest, QueryCompletion *qc);

static ProcessUtility_hook_type prev_ProcessUtility = NULL;

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	RegisterXactCallback(RoadXactCallback, NULL);
    RegisterSubXactCallback(RoadSubXactCallback, NULL);

	/* Install hooks */
	prev_ProcessUtility = ProcessUtility_hook ?
		ProcessUtility_hook : standard_ProcessUtility;
    ProcessUtility_hook = road_ProcessUtility;
}

static void
RoadXactCallback(XactEvent event, void *arg)
{
	/* Reset the global state */
	RoadState.called_in_atsam = false;
	RoadState.called_in_ctas = false;
}

static void
RoadSubXactCallback(SubXactEvent event, SubTransactionId mySubid,
					SubTransactionId parentSubid, void *arg)
{
	/* XXX */
}

/*
 * Check if CTAS or ALTER TABLE ... SET ACCESS METHOD is called.
 */
static void road_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
								bool readOnlyTree,
								ProcessUtilityContext context, ParamListInfo params,
								QueryEnvironment *queryEnv,
								DestReceiver *dest, QueryCompletion *qc)
{
	NodeTag tag = nodeTag(pstmt->utilityStmt);

	if (tag == T_CreateTableAsStmt)
	{
		RoadState.called_in_ctas = true;
		Assert(!RoadState.called_in_atsam);
	}
	else if (tag == T_AlterTableStmt)
	{
		AlterTableStmt *atstmt = (AlterTableStmt *) pstmt->utilityStmt;
		ListCell   *cell;

		foreach(cell, atstmt->cmds)
		{
			AlterTableCmd *cmd = (AlterTableCmd *) lfirst(cell);

			if (cmd->subtype == AT_SetAccessMethod)
			{
				RoadState.called_in_atsam = true;
				break;
			}
		}
		Assert(!RoadState.called_in_ctas);
	}

	prev_ProcessUtility(pstmt, queryString, false, context,
						params, queryEnv, dest, qc);
}

void
d(const char *msg, char *data, int len)
{
	StringInfoData buf;

	initStringInfo(&buf);
	for (int i = 0; i < len; i++)
		appendStringInfo(&buf, "%02X ", (uint8) data[i]);

	ROAD_DEBUG_LOG("%s:\n%s", msg, buf.data);
	pfree(buf.data);
}

static const TupleTableSlotOps *
road_slot_callbacks(Relation relation)
{
	/*
	 * Here you would most likely want to invent your own set of
	 * slot callbacks for your AM.
	 */
	return &TTSOpsVirtual;
}

static TableScanDesc
road_scan_begin(Relation relation, Snapshot snapshot,
				   int nkeys, ScanKey key,
				   ParallelTableScanDesc parallel_scan,
				   uint32 flags)
{
	return NULL;
}

static void
road_scan_end(TableScanDesc sscan)
{
}

static void
road_scan_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
					bool allow_strat, bool allow_sync, bool allow_pagemode)
{
}


static bool
road_scan_getnextslot(TableScanDesc sscan, ScanDirection direction,
						 TupleTableSlot *slot)
{
	return false;
}

static IndexFetchTableData *
road_index_fetch_begin(Relation rel)
{
	return NULL;
}

static void
road_index_fetch_reset(IndexFetchTableData *scan)
{
}

static void
road_index_fetch_end(IndexFetchTableData *scan)
{
}

static bool
road_index_fetch_tuple(struct IndexFetchTableData *scan,
							ItemPointer tid,
							Snapshot snapshot,
							TupleTableSlot *slot,
							bool *call_again, bool *all_dead)
{
	return false;
}

static bool
road_fetch_row_version(Relation relation,
							ItemPointer tid,
							Snapshot snapshot,
							TupleTableSlot *slot)
{
	/* nothing to do */
	return false;
}

static void
road_get_latest_tid(TableScanDesc sscan,
					   ItemPointer tid)
{
	/* nothing to do */
}

static bool
road_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	return false;
}

static bool
road_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
								  Snapshot snapshot)
{
	return false;
}

static TransactionId
road_index_delete_tuples(Relation rel,
							TM_IndexDeleteOp *delstate)
{
	return InvalidTransactionId;
}

static void
road_tuple_insert(Relation relation, TupleTableSlot *slot,
				  CommandId cid, int options, BulkInsertState bistate)
{
	elog(LOG, "called oid %u in ctas %d atsam %d",
		 RelationGetRelid(relation),
		 RoadState.called_in_ctas,
		 RoadState.called_in_atsam);
}

static void
road_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
								 CommandId cid, int options,
								 BulkInsertState bistate,
								 uint32 specToken)
{
	FEATURE_NOT_SUPPORTED();
}

static void
road_tuple_complete_speculative(Relation relation, TupleTableSlot *slot,
								   uint32 spekToken, bool succeeded)
{
	FEATURE_NOT_SUPPORTED();
}

static void
road_multi_insert(Relation relation, TupleTableSlot **slots,
					 int ntuples, CommandId cid, int options,
					 BulkInsertState bistate)
{
	FEATURE_NOT_SUPPORTED();
}

static TM_Result
road_tuple_delete(Relation relation, ItemPointer tid, CommandId cid,
					 Snapshot snapshot, Snapshot crosscheck, bool wait,
					 TM_FailureData *tmfd, bool changingPart)
{
	FEATURE_NOT_SUPPORTED();
}


static TM_Result
road_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
					CommandId cid, Snapshot snapshot, Snapshot crosscheck,
					bool wait, TM_FailureData *tmfd,
					LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes)
{
	FEATURE_NOT_SUPPORTED();
}

static TM_Result
road_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot,
					 TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
					 LockWaitPolicy wait_policy, uint8 flags,
					 TM_FailureData *tmfd)
{
	FEATURE_NOT_SUPPORTED();
}

static void
road_vacuum(Relation onerel, VacuumParams *params, BufferAccessStrategy bstrategy)
{
}

static void
road_finish_bulk_insert(Relation relation, int options)
{
}

static void
road_relation_set_new_filelocator(Relation rel,
									 const RelFileLocator *newrlocator,
									 char persistence,
									 TransactionId *freezeXid,
									 MultiXactId *minmulti)
{
	SMgrRelation	srel;
	PageHeader		hdr;
	RoadMetaPage	metap;
	PGIOAlignedBlock block;
	Page page = block.data;

	if (persistence == RELPERSISTENCE_UNLOGGED)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("unlogged road tables are not supported")));

	*freezeXid = RecentXmin;
	*minmulti = GetOldestMultiXactId();

	/* Create the storage for the road table */
	srel = RelationCreateStorage(*newrlocator, persistence, true);

	PageInit(page, BLCKSZ, 0);
	hdr = (PageHeader) page;

	/* initialize the meta page */
	metap.version = ROAD_META_VERSION;
	ItemPointerSet(&(metap.reserved_tid), 0, FirstOffsetNumber);

	memcpy(page + hdr->pd_lower, (char *) &metap, sizeof(RoadMetaPage));

	PageSetChecksumInplace(page, ROAD_META_BLOCKNO);
	smgrwrite(srel, MAIN_FORKNUM, ROAD_META_BLOCKNO, page, true);
	log_newpage(&srel->smgr_rlocator.locator, MAIN_FORKNUM,
				ROAD_META_BLOCKNO, page, true);

	smgrimmedsync(srel, MAIN_FORKNUM);
}

static void
road_relation_nontransactional_truncate(Relation rel)
{
	FEATURE_NOT_SUPPORTED();
}

static void
road_copy_data(Relation rel, const RelFileLocator *newrlocator)
{
	FEATURE_NOT_SUPPORTED();
}

static void
road_copy_for_cluster(Relation OldTable, Relation NewTable,
						   Relation OldIndex, bool use_sort,
						   TransactionId OldestXmin,
						   TransactionId *xid_cutoff,
						   MultiXactId *multi_cutoff,
						   double *num_tuples,
						   double *tups_vacuumed,
						   double *tups_recently_dead)
{
	FEATURE_NOT_SUPPORTED();
}

static bool
road_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
								  BufferAccessStrategy bstrategy)
{
	FEATURE_NOT_SUPPORTED();
}

static bool
road_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
								  double *liverows, double *deadrows,
								  TupleTableSlot *slot)
{
	FEATURE_NOT_SUPPORTED();
}

static double
road_index_build_range_scan(Relation tableRelation,
							   Relation indexRelation,
							   IndexInfo *indexInfo,
							   bool allow_sync,
							   bool anyvisible,
							   bool progress,
							   BlockNumber start_blockno,
							   BlockNumber numblocks,
							   IndexBuildCallback callback,
							   void *callback_state,
							   TableScanDesc scan)
{
	return 0;
}

static void
road_index_validate_scan(Relation tableRelation,
							  Relation indexRelation,
							  IndexInfo *indexInfo,
							  Snapshot snapshot,
							  ValidateIndexState *state)
{
	FEATURE_NOT_SUPPORTED();
}

static uint64
road_relation_size(Relation rel, ForkNumber forkNumber)
{
	if (forkNumber != MAIN_FORKNUM)
		return 0;

	return smgrnblocks(RelationGetSmgr(rel), forkNumber) * BLCKSZ;
}

/*
 * Check to see whether the table needs a TOAST table.
 */
static bool
road_relation_needs_toast_table(Relation rel)
{
    int32       data_length = 0;
    bool        maxlength_unknown = false;
    bool        has_toastable_attrs = false;
    TupleDesc   tupdesc = rel->rd_att;
    int32       tuple_length;
    int         i;

    for (i = 0; i < tupdesc->natts; i++)
    {
        Form_pg_attribute att = TupleDescAttr(tupdesc, i);

        if (att->attisdropped)
            continue;
        data_length = att_align_nominal(data_length, att->attalign);
        if (att->attlen > 0)
        {
            /* Fixed-length types are never toastable */
            data_length += att->attlen;
        }
        else
        {
            int32       maxlen = type_maximum_size(att->atttypid,
                                                   att->atttypmod);

            if (maxlen < 0)
                maxlength_unknown = true;
            else
                data_length += maxlen;
            if (att->attstorage != TYPSTORAGE_PLAIN)
                has_toastable_attrs = true;
        }
    }

	if (!has_toastable_attrs)
        return false;           /* nothing to toast? */
    if (maxlength_unknown)
        return true;            /* any unlimited-length attrs? */
    tuple_length = MAXALIGN(SizeofHeapTupleHeader +
                            BITMAPLEN(tupdesc->natts)) +
        MAXALIGN(data_length);
    return (tuple_length > TOAST_TUPLE_THRESHOLD);
}

static Oid
road_relation_toast_am(Relation rel)
{
	return HEAP_TABLE_AM_OID;
}

static void
road_estimate_rel_size(Relation rel, int32 *attr_widths,
							BlockNumber *pages, double *tuples,
							double *allvisfrac)
{
	/* no data available */
	if (attr_widths)
		*attr_widths = 0;
	if (pages)
		*pages = 0;
	if (tuples)
		*tuples = 0;
	if (allvisfrac)
		*allvisfrac = 0;
}

static bool
road_scan_bitmap_next_block(TableScanDesc scan,
								 TBMIterateResult *tbmres)
{
	FEATURE_NOT_SUPPORTED();
}

static bool
road_scan_bitmap_next_tuple(TableScanDesc scan,
								 TBMIterateResult *tbmres,
								 TupleTableSlot *slot)
{
	FEATURE_NOT_SUPPORTED();
}

static bool
road_scan_sample_next_block(TableScanDesc scan,
								 SampleScanState *scanstate)
{
	FEATURE_NOT_SUPPORTED();
}

static bool
road_scan_sample_next_tuple(TableScanDesc scan,
								 SampleScanState *scanstate,
								 TupleTableSlot *slot)
{
	FEATURE_NOT_SUPPORTED();
}

static const TableAmRoutine road_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = road_slot_callbacks,

	.scan_begin = road_scan_begin,
	.scan_end = road_scan_end,
	.scan_rescan = road_scan_rescan,
	.scan_getnextslot = road_scan_getnextslot,

	/* these are common helper functions */
	.parallelscan_estimate = table_block_parallelscan_estimate,
	.parallelscan_initialize = table_block_parallelscan_initialize,
	.parallelscan_reinitialize = table_block_parallelscan_reinitialize,

	.index_fetch_begin = road_index_fetch_begin,
	.index_fetch_reset = road_index_fetch_reset,
	.index_fetch_end = road_index_fetch_end,
	.index_fetch_tuple = road_index_fetch_tuple,

	.tuple_insert = road_tuple_insert,
	.tuple_insert_speculative = road_tuple_insert_speculative,
	.tuple_complete_speculative = road_tuple_complete_speculative,
	.multi_insert = road_multi_insert,
	.tuple_delete = road_tuple_delete,
	.tuple_update = road_tuple_update,
	.tuple_lock = road_tuple_lock,
	.finish_bulk_insert = road_finish_bulk_insert,

	.tuple_fetch_row_version = road_fetch_row_version,
	.tuple_get_latest_tid = road_get_latest_tid,
	.tuple_tid_valid = road_tuple_tid_valid,
	.tuple_satisfies_snapshot = road_tuple_satisfies_snapshot,
	.index_delete_tuples = road_index_delete_tuples,

	.relation_set_new_filelocator = road_relation_set_new_filelocator,
	.relation_nontransactional_truncate = road_relation_nontransactional_truncate,
	.relation_copy_data = road_copy_data,
	.relation_copy_for_cluster = road_copy_for_cluster,
	.relation_vacuum = road_vacuum,
	.scan_analyze_next_block = road_scan_analyze_next_block,
	.scan_analyze_next_tuple = road_scan_analyze_next_tuple,
	.index_build_range_scan = road_index_build_range_scan,
	.index_validate_scan = road_index_validate_scan,

	.relation_size = road_relation_size,
	.relation_toast_am = road_relation_toast_am,
	.relation_needs_toast_table = road_relation_needs_toast_table,
	.relation_fetch_toast_slice = heap_fetch_toast_slice,

	.relation_estimate_size = road_estimate_rel_size,

	.scan_bitmap_next_block = road_scan_bitmap_next_block,
	.scan_bitmap_next_tuple = road_scan_bitmap_next_tuple,
	.scan_sample_next_block = road_scan_sample_next_block,
	.scan_sample_next_tuple = road_scan_sample_next_tuple
};

Datum
road_am_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&road_methods);
}
