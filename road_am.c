/*-------------------------------------------------------------------------
 *
 * road_am.c
 *
 * Portions Copyright (c) 2024, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_LZ4
#include <lz4.h>
#endif

#include "access/amapi.h"
#include "access/generic_xlog.h"
#include "access/heapam.h"
#include "access/hio.h"
#include "access/tableam.h"
#include "access/multixact.h"
#include "access/heaptoast.h"
#include "access/xlog_internal.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/heap.h"
#include "catalog/pg_am.h"
#include "catalog/storage.h"
#include "commands/vacuum.h"
#include "common/pg_lzcompress.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
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
#include "skiplist_table.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(road_am_handler);

#define FEATURE_NOT_SUPPORTED() \
	ereport(ERROR, \
		errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
			errmsg("%s is not supported", __func__))

/*
 * Struct for all information required by inserting tuples to a ROAD table.
 * Since a ROAD table is created during either ALTER TABLE ... SET ACCESS
 * METHOD or CREATE TABLE ... AS, both of which take an AccessExclusiveLock
 * on the original table, and we don't allow these operations to be called
 * in a transaction block, we insert tuples to one ROAD table in a transaction.
 */
typedef struct RoadInsertStateData
{
	Oid			relid;
	Oid			skip_relid;

	/* The first rownumber in the current chunk page */
	RowNumber	first_rownum;

	/* The current rownumber */
	RowNumber	cur_rownum;

	/*
	 * Flags if ALTER TABLE ... SET ACCESS METHOD or CREATE TABLE ... AS is
	 * called. This is necessary to control whether we can allow insertion to
	 * the ROAD table.
	 *
	 * XXX: if we find a better way to distinguish how road_insert_tuple is
	 * called, we don't need both variables (as well as probably
	 * ProcessUtility hook).
	 */
	bool		called_in_atsam;
	bool		called_in_ctas;

	/*
	 * In ALTER TABLE ... SET ACCESS METHOD, road_tuple_insert() and
	 * road_relation_set_new_filelocator() don't know the original table's
	 * OID. If we create the skiplist table for the new (transient) relation,
	 * the skiplist able will be removed altogether. In our process utility
	 * hook, we set the original relation's OID to atsam_relid. So it's a
	 * valid OID only during ALTER TABLE ... SET ACCESS METHOD execution.
	 */
	Oid			atsam_relid;

	/* Work buffer to collect tuples */
	char		chunk_page[ROAD_CHUNK_PAGE_SIZE];

	int			compression_method;
} RoadInsertStateData;
static RoadInsertStateData RoadInsertState = {0};

/*
 * SeqScan descriptor for a ROAD table.
 */
typedef struct RoadScanDescData
{
	TableScanDescData base;

	bool		inited;

	/*
	 * The current buffer, block, and position in a ROAD table page.
	 */
	Buffer		curbuf;
	BlockNumber curblk;
	int64		curpos;

	/* The current and max offset number within the chunk page */
	OffsetNumber curoff;
	OffsetNumber maxoff;

	/* The current and max chunk number */
	int64		curchunk;
	int64		maxchunk;

	HeapTupleData tuple;
	int			compression_method;

	/* Working buffer for a chunk page */
	char		chunk_page[ROAD_CHUNK_PAGE_SIZE];
} RoadScanDescData;
typedef struct RoadScanDescData *RoadScanDesc;

/* A scan descriptor for index fetching */
typedef struct RoadIndexFetchData
{
	IndexFetchTableData base;

	bool		inited;

	int			compression_method;

	/* Working buffer for a chunk page */
	char		chunk_page[ROAD_CHUNK_PAGE_SIZE];

	/*
	 * The rownumbers of the first and last tuples in
	 * the cached chunk_page.
	 */
	RowNumber	first_rownum;
	RowNumber	last_rownum;
} RoadIndexFetchData;
typedef struct RoadIndexFetchData *RoadIndexFetch;

typedef enum
{
	ROAD_PGLZ_COMPRESSION = 0,
	ROAD_LZ4_COMPRESSION,
	/* ROAD_ZSTD_COMPRESSION, */
} RoadCompressionMethod;

static const struct config_enum_entry compression_options[] =
{
	{"pglz", ROAD_PGLZ_COMPRESSION, false},
#ifdef USE_LZ4
	{"lz4", ROAD_LZ4_COMPRESSION, false},
#endif

	/*
	 * XXX: not supported yet.
	 * #ifdef USE_ZSTD {"zstd", ROAD_ZSTD_COMPRESSION, false}, #endif
	 */
	{NULL, 0, false},
};
static int	road_compression_method = ROAD_PGLZ_COMPRESSION;

static void RoadXactCallback(XactEvent event, void *arg);
static void RoadSubXactCallback(SubXactEvent event, SubTransactionId mySubid,
								SubTransactionId parentSubid, void *arg);
static void road_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
								bool readOnlyTree,
								ProcessUtilityContext context, ParamListInfo params,
								QueryEnvironment *queryEnv,
								DestReceiver *dest, QueryCompletion *qc);

static void road_flush_insert_state(RoadInsertStateData * state);
static void road_insert_compressed_chunk_page(RoadInsertStateData * state,
											  char *c_buffer, int c_len);

/* chunk page functions */
static int32 chunk_page_compress(RoadInsertStateData * state, Page chunkpage,
								 ChunkHeader cdata);
static void chunk_page_init(Page page);

static ProcessUtility_hook_type prev_ProcessUtility = NULL;

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errmsg("cannot load \"%s\" after startup", "pgroad"),
				 errdetail("\"%s\" must be loaded with shared_preload_libraries.",
						   "pgroad")));

	DefineCustomEnumVariable("pgroad.compression",
							 "compression method for ROAD table",
							 NULL,
							 &road_compression_method,
							 ROAD_PGLZ_COMPRESSION,
							 compression_options,
							 PGC_SUSET,
							 0,
							 NULL, NULL, NULL);

	MarkGUCPrefixReserved("pgroad");

	/* Register xact callbacks */
	RegisterXactCallback(RoadXactCallback, NULL);
	RegisterSubXactCallback(RoadSubXactCallback, NULL);

	/* Install hooks */
	prev_ProcessUtility = ProcessUtility_hook ?
		ProcessUtility_hook : standard_ProcessUtility;
	ProcessUtility_hook = road_ProcessUtility;
}

/* Reset all fields of RoadInsertStateData */
static void
road_insert_state_init(RoadInsertStateData * state)
{
	MemSet(state, 0, sizeof(RoadInsertStateData));
	chunk_page_init((Page) state->chunk_page);
}

static void
RoadXactCallback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
		case XACT_EVENT_PREPARE:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_PREPARE:
			{
				road_insert_state_init(&RoadInsertState);
				break;
			}
	}
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
static void
road_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
					bool readOnlyTree,
					ProcessUtilityContext context, ParamListInfo params,
					QueryEnvironment *queryEnv,
					DestReceiver *dest, QueryCompletion *qc)
{
	NodeTag		tag = nodeTag(pstmt->utilityStmt);

	if (tag == T_CreateTableAsStmt)
	{
		RoadInsertState.called_in_ctas = true;
		Assert(!RoadInsertState.called_in_atsam);
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
				Relation	rel = relation_openrv(atstmt->relation, ShareLock);

				ROAD_DEBUG_LOG("ProcessUtility: rel %s am %s",
							   RelationGetRelationName(rel), cmd->name);

				/*
				 * Are we about to change the access method of the relation to
				 * ROAD table AM?
				 */
				if (strcmp(cmd->name, "road") == 0)
				{
					/* Remember the original table's OID */
					RoadInsertState.atsam_relid = RelationGetRelid(rel);

					RoadInsertState.called_in_atsam = true;
				}

				RelationClose(rel);

				break;
			}
		}
		Assert(!RoadInsertState.called_in_ctas);
	}

	prev_ProcessUtility(pstmt, queryString, false, context,
						params, queryEnv, dest, qc);
}

static inline ItemPointerData
rownumber_to_tid(uint64 rownumber)
{
	ItemPointerData ret = {0};

	ItemPointerSetBlockNumber(&ret, rownumber / MaxHeapTuplesPerPage);

	/*
	 * We cannot use offset number more than MaxHeapTuplesPerPage, see
	 * tidbitmap.
	 */
	ItemPointerSetOffsetNumber(&ret, rownumber % MaxHeapTuplesPerPage +
							   FirstOffsetNumber);

	return ret;
}

static inline uint64
tid_to_rownumber(ItemPointer tid)
{
	return (uint64) (ItemPointerGetBlockNumber(tid) * MaxHeapTuplesPerPage +
					 ItemPointerGetOffsetNumber(tid) - FirstOffsetNumber);
}

static void
chunk_page_init(Page page)
{
	Size		pageSize = ROAD_CHUNK_PAGE_SIZE;
	Size		specialSize = 0;
	PageHeader	p = (PageHeader) page;

	specialSize = MAXALIGN(specialSize);

	Assert(pageSize > specialSize + SizeOfPageHeaderData);

	/* Make sure all fields of page are zero, as well as unused space */
	MemSet(p, 0, pageSize);

	p->pd_flags = 0;
	p->pd_lower = SizeOfPageHeaderData;
	p->pd_upper = pageSize - specialSize;
	p->pd_special = pageSize - specialSize;
	PageSetPageSizeAndVersion(page, pageSize, PG_PAGE_LAYOUT_VERSION);
}

static Size
chunk_page_free_space(Page page)
{
	return PageGetFreeSpace(page);
}

static uint16
chunk_page_get_max_offsetnumber(Page page)
{
	return (uint16) PageGetMaxOffsetNumber(page);
}

static OffsetNumber
chunk_page_add_item(Page page, Item item, Size size, OffsetNumber offsetNumber)
{
	PageHeader	phdr = (PageHeader) page;
	Size		alignedSize;
	int			lower;
	int			upper;
	ItemId		itemId;
	OffsetNumber limit;
	bool		needshuffle = false;

	/*
	 * Be wary about corrupted page pointers
	 */
	if (phdr->pd_lower < SizeOfPageHeaderData ||
		phdr->pd_lower > phdr->pd_upper ||
		phdr->pd_upper > phdr->pd_special ||
		phdr->pd_special > ROAD_CHUNK_PAGE_SIZE)
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("corrupted page pointers: lower = %u, upper = %u, special = %u",
						phdr->pd_lower, phdr->pd_upper, phdr->pd_special)));

	/*
	 * Select offsetNumber to place the new item at
	 */
	limit = OffsetNumberNext(PageGetMaxOffsetNumber(page));

	/* was offsetNumber passed in? */
	if (OffsetNumberIsValid(offsetNumber))
	{
		if (offsetNumber < limit)
			needshuffle = true; /* need to move existing linp's */
	}
	else
	{
		/* offsetNumber was not passed in, so find a free slot */
		/* if no free slot, we'll put it at limit (1st open slot) */
		if (PageHasFreeLinePointers(page))
		{
			/*
			 * Scan line pointer array to locate a "recyclable" (unused)
			 * ItemId.
			 *
			 * Always use earlier items first.  PageTruncateLinePointerArray
			 * can only truncate unused items when they appear as a contiguous
			 * group at the end of the line pointer array.
			 */
			for (offsetNumber = FirstOffsetNumber;
				 offsetNumber < limit;	/* limit is maxoff+1 */
				 offsetNumber++)
			{
				itemId = PageGetItemId(page, offsetNumber);

				/*
				 * We check for no storage as well, just to be paranoid;
				 * unused items should never have storage.  Assert() that the
				 * invariant is respected too.
				 */
				Assert(ItemIdIsUsed(itemId) || !ItemIdHasStorage(itemId));

				if (!ItemIdIsUsed(itemId) && !ItemIdHasStorage(itemId))
					break;
			}
			if (offsetNumber >= limit)
			{
				/* the hint is wrong, so reset it */
				PageClearHasFreeLinePointers(page);
			}
		}
		else
		{
			/* don't bother searching if hint says there's no free slot */
			offsetNumber = limit;
		}
	}

	if (offsetNumber > limit)
	{
		elog(WARNING, "specified item offset is too large");
		return InvalidOffsetNumber;
	}

	/* Reject placing items beyond heap boundary, if heap */
	if (offsetNumber > MaxHeapTuplesPerChunkPage)
	{
		elog(WARNING, "can't put more than MaxHeapTuplesPerPage items in a heap page");
		return InvalidOffsetNumber;
	}

	/*
	 * Compute new lower and upper pointers for page, see if it'll fit.
	 *
	 * Note: do arithmetic as signed ints, to avoid mistakes if, say,
	 * alignedSize > pd_upper.
	 */
	if (offsetNumber == limit || needshuffle)
		lower = phdr->pd_lower + sizeof(ItemIdData);
	else
		lower = phdr->pd_lower;

	alignedSize = MAXALIGN(size);

	upper = (int) phdr->pd_upper - (int) alignedSize;

	if (lower > upper)
		return InvalidOffsetNumber;

	/*
	 * OK to insert the item.  First, shuffle the existing pointers if needed.
	 */
	itemId = PageGetItemId(page, offsetNumber);

	if (needshuffle)
		memmove(itemId + 1, itemId,
				(limit - offsetNumber) * sizeof(ItemIdData));

	/* set the line pointer */
	ItemIdSetNormal(itemId, upper, size);

	/*
	 * Items normally contain no uninitialized bytes.  Core bufpage consumers
	 * conform, but this is not a necessary coding rule; a new index AM could
	 * opt to depart from it.  However, data type input functions and other
	 * C-language functions that synthesize datums should initialize all
	 * bytes; datumIsEqual() relies on this.  Testing here, along with the
	 * similar check in printtup(), helps to catch such mistakes.
	 *
	 * Values of the "name" type retrieved via index-only scans may contain
	 * uninitialized bytes; see comment in btrescan().  Valgrind will report
	 * this as an error, but it is safe to ignore.
	 */
	/* VALGRIND_CHECK_MEM_IS_DEFINED(item, size); */

	/* copy the item's data onto the page */
	memcpy((char *) page + upper, item, size);

	/* adjust page header */
	phdr->pd_lower = (LocationIndex) lower;
	phdr->pd_upper = (LocationIndex) upper;

	return offsetNumber;
}

static void
chunk_page_decompress(RoadCompressionMethod method, ChunkHeader chunk, char *src, char *dest)
{
	char		tmp[ROAD_CHUNK_PAGE_SIZE];
	int			len = 0;

	if (method == ROAD_PGLZ_COMPRESSION)
	{
		len = pglz_decompress(src, chunk->c_len, tmp,
							  ROAD_CHUNK_PAGE_SIZE - chunk->c_hole_len, false);
	}
#ifdef USE_LZ4
	else if (method == ROAD_LZ4_COMPRESSION)
	{
		len = LZ4_decompress_safe(src, tmp, chunk->c_len, ROAD_CHUNK_PAGE_SIZE);
	}
#endif

	if (len < 0)
		elog(ERROR, "could not decompress chunk page data");

	if (chunk->c_hole_len == 0)
		memcpy(dest, tmp, ROAD_CHUNK_PAGE_SIZE);
	else
	{
		memcpy(dest, tmp, chunk->c_hole_off);
		MemSet(dest + chunk->c_hole_off, 0, chunk->c_hole_len);
		memcpy(dest + (chunk->c_hole_off + chunk->c_hole_len),
			   tmp + chunk->c_hole_off,
			   ROAD_CHUNK_PAGE_SIZE - (chunk->c_hole_off + chunk->c_hole_len));
	}

	ROAD_DEBUG_LOG("DECOMPRESS: c_len %u c_hole_off %u c_hole_len %u decompressed len %d method %d",
				   chunk->c_len, chunk->c_hole_off,
				   chunk->c_hole_len, len, method);
}

/*
 * Compress the given chunkpage into cdata. Return the total length
 * including the compressed chunkpage and chunk header size.
 */
static int32
chunk_page_compress(RoadInsertStateData * state, Page chunkpage, ChunkHeader cdata)
{
	uint16		lower = ((PageHeader) chunkpage)->pd_lower;
	uint16		upper = ((PageHeader) chunkpage)->pd_upper;
	int			orig_len,
				dest_len = 0;
	int			hole_offset,
				hole_len = 0;
	char		tmp[ROAD_CHUNK_PAGE_SIZE];
	char	   *source,
			   *dest;

	if (lower >= SizeOfPageHeaderData && upper > lower && upper <= ROAD_CHUNK_PAGE_SIZE)
	{
		hole_offset = lower;
		hole_len = upper - lower;
	}

	if (hole_len > 0)
	{
		source = tmp;
		memcpy(source, chunkpage, hole_offset);
		memcpy(source + hole_offset,
			   chunkpage + (hole_offset + hole_len),
			   ROAD_CHUNK_PAGE_SIZE - (hole_len + hole_offset));
	}
	else
		source = chunkpage;

	orig_len = ROAD_CHUNK_PAGE_SIZE - hole_len;
	dest = ((char *) cdata) + SizeOfChunkHeaderData;

	if (state->compression_method == ROAD_PGLZ_COMPRESSION)
	{
		dest_len = pglz_compress(source, orig_len, dest, PGLZ_strategy_always);
	}
#ifdef USE_LZ4
	else if (state->compression_method == ROAD_LZ4_COMPRESSION)
	{
		dest_len = LZ4_compress_default(source, dest, orig_len, ROAD_CHUNK_PAGE_SIZE);
	}
#endif

	if (dest_len < 0)
		elog(ERROR, "could not compress chunk page data");

	cdata->c_len = dest_len;
	cdata->c_hole_off = hole_offset;
	cdata->c_hole_len = hole_len;

	ROAD_DEBUG_LOG("COMPRESS: c_len %u c_hole_off %u c_hole_len %u total len %lu method %d",
				   cdata->c_len, cdata->c_hole_off,
				   cdata->c_hole_len, dest_len + SizeOfChunkHeaderData,
				   state->compression_method);

	return dest_len + SizeOfChunkHeaderData;
}

static void
road_page_init(Page page)
{
	Assert(PageIsNew(page));
	PageInit(page, BLCKSZ, 0);
	((PageHeader) page)->pd_lower = MAXALIGN(SizeOfPageHeaderData);
}

static int
road_page_get_freespace(Page page)
{
	/*
	 * We don't use PageGetFreeSpace() for road pages since the road page
	 * doesn't use ItemId while the function subtract the space by one ItemId.
	 */
	return (int) ((PageHeader) page)->pd_upper -
		(int) ((PageHeader) page)->pd_lower;
}

static Buffer
road_get_meta_page(Relation rel)
{
	Buffer		buffer;

	buffer = ReadBuffer(rel, ROAD_META_BLOCKNO);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);

	return buffer;
}

/*
 * Compress and insert the chunk page to the table, and update the
 * corresponding skiplist entry.
 *
 * c_buffer is the compressed chunk page including the header, and
 * c_len is the total length of c_buffer.
 */
static void
road_flush_insert_state(RoadInsertStateData * state)
{
	char		tmp[ROAD_CHUNK_PAGE_SIZE] = {0};
	int			len;

	ROAD_DEBUG_LOG("FLUSH: start oid %u chunk page ntuples %u freespace %zu first_rownum %lu cur_rownum %lu",
				   state->relid,
				   chunk_page_get_max_offsetnumber(state->chunk_page),
				   chunk_page_free_space(state->chunk_page),
				   state->first_rownum,
				   state->cur_rownum);

	ROAD_DEBUG_LOG("FLUSH: step-1: start compress");
	len = chunk_page_compress(state, state->chunk_page, (ChunkHeader) tmp);

	ROAD_DEBUG_LOG("FLUSH: step-2: insert compressed page");
	road_insert_compressed_chunk_page(state, tmp, len);

	state->first_rownum = state->cur_rownum;
	chunk_page_init((Page) state->chunk_page);

	ROAD_DEBUG_LOG("FLUSH: step-3: finish, update the insert state next-first %lu next-cur %lu",
				   state->first_rownum,
				   state->cur_rownum);
}

static void
road_write_data(Relation rel, Buffer metabuffer, Buffer *buffers, int nbuffers,
				char *src, int src_len)
{
	int		idx;
	int		written;
	int		nbuffers_remain = MAX_GENERIC_XLOG_PAGES;
	Page	page;
	RoadMetaPage *metap;
	GenericXLogState *state = GenericXLogStart(rel);

	/* update the meta page */
	page = GenericXLogRegisterBuffer(state, metabuffer,
									 GENERIC_XLOG_FULL_IMAGE);
	nbuffers_remain--;
	metap = (RoadMetaPage *) PageGetContents(page);
	metap->nchunks += 1;

	written = idx = 0;
	while (written < src_len)
	{
		Buffer	buffer = buffers[idx];
		uint32	len;
		Size	freespace;
		PageHeader phdr;

		/*
		 * Generic XLOG infrastructure has the limit of the maximum number
		 * of buffers that can be registered, MAX_GENERIC_XLOG_PAGES, 4 as of
		 * now. So if the total number of buffers that we're writing the
		 * compressed chunk pages across to exceeds the limit, we have to
		 * split WAL records.
		 */
		if (nbuffers_remain == 0)
		{
			/* Write the previously collected data changes */
			GenericXLogFinish(state);

			/* Start a new WAL insertion */
			state = GenericXLogStart(rel);
			nbuffers_remain = MAX_GENERIC_XLOG_PAGES;
		}

		/* Register a new buffer and get the working page */
		page = GenericXLogRegisterBuffer(state, buffer, GENERIC_XLOG_FULL_IMAGE);
		nbuffers_remain--;

		phdr = (PageHeader) page;
		if (PageIsNew(page))
			road_page_init(page);

		freespace = road_page_get_freespace(page);
		len = Min(freespace, src_len - written);

		ROAD_DEBUG_LOG("FLUSH: [%d] blk %u pd_lower %u -> %u, c_buf pos %u (total %u), write %u",
					   idx, BufferGetBlockNumber(buffer),
					   phdr->pd_lower,
					   phdr->pd_lower + len,
					   written, src_len,
					   len);

		Assert(phdr->pd_lower + len <= BLCKSZ);
		memcpy((char *) page + phdr->pd_lower,
			   (char *) src + written,
			   len);
		phdr->pd_lower += len;

		written += len;
		idx++;
	}

	GenericXLogFinish(state);
}

/*
 * Insert the given compressed chunk page into the table. Also, insert the
 * corresponding skiplist table entry.
 */
static void
road_insert_compressed_chunk_page(RoadInsertStateData * state, char *c_buffer,
								  int c_len)
{
	Relation	rel = table_open(state->relid, NoLock);
	BlockNumber blkno;
	Buffer	   *buffers;
	Buffer		metabuffer;
	int			num_buffers;
	Buffer		buffer;
	Page		page;
	Size		freespace;
	uint64		skipent_offset;

	Assert(CheckRelationLockedByMe(rel, AccessExclusiveLock, true));

	metabuffer = ReadBuffer(rel, ROAD_META_BLOCKNO);
	LockBuffer(metabuffer, BUFFER_LOCK_EXCLUSIVE);

	/* road table has meta page at block 0 */
	blkno = RelationGetNumberOfBlocks(rel) - 1;

	if (blkno == ROAD_META_BLOCKNO)
		blkno = P_NEW;

	buffer = ReadBuffer(rel, blkno);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

	page = (Page) BufferGetPage(buffer);
	if (PageIsNew(page))
		road_page_init(page);

	freespace = road_page_get_freespace(page);
	if (freespace >= c_len)
	{
		ROAD_DEBUG_LOG("FLUSH: tail page %u has enough space %lu for chunk size %u",
					   BufferGetBlockNumber(buffer), freespace, c_len);

		/* easy case, no need to extend the table */
		buffers = palloc(sizeof(Buffer));
		buffers[0] = buffer;
		num_buffers = 1;

		skipent_offset = (BufferGetBlockNumber(buffers[0]) * BLCKSZ) +
			(PageIsNew(page) ?
			 MAXALIGN(SizeOfPageHeaderData) :
			 ((PageHeader) BufferGetPage(buffer))->pd_lower);
	}
	else if (freespace >= SizeOfChunkHeaderData)
	{
		uint32		extend_by;
		uint32		extended_by;

		/*
		 * The tail page doesn't have enough space to store the whole
		 * compressed page, but can have at least the chunk header. So we keep
		 * hold this buffer to use, and extend the cc-table for the remaining
		 * data.
		 */
		extend_by = ((c_len - freespace) / (BLCKSZ - sizeof(PageHeaderData))) + 1;
		num_buffers = extend_by + 1;
		buffers = palloc(sizeof(Buffer) * num_buffers);
		buffers[0] = buffer;

		ExtendBufferedRelBy(BMR_REL(rel), MAIN_FORKNUM,
							NULL,
							0,
							extend_by,
							&(buffers[1]),
							&extended_by);

		ROAD_DEBUG_LOG("FLUSH: tail page %u has space %lu only header %lu (c_len %u), extend by %u",
					   BufferGetBlockNumber(buffer),
					   freespace, SizeOfChunkHeaderData,
					   c_len,
					   extend_by);

		for (int i = 1; i < num_buffers; i++)
			LockBuffer(buffers[i], BUFFER_LOCK_EXCLUSIVE);

		skipent_offset = (BufferGetBlockNumber(buffers[0]) * BLCKSZ) +
			(PageIsNew(page) ?
			 MAXALIGN(SizeOfPageHeaderData) :
			 ((PageHeader) BufferGetPage(buffer))->pd_lower);
	}
	else
	{
		uint32		extend_by;
		uint32		extended_by;

		ROAD_DEBUG_LOG("FLUSH: tail page %u doesn't have space %lu (cc %u), extend by %u",
					   BufferGetBlockNumber(buffer),
					   freespace,
					   c_len,
					   (uint32) ((c_len / (BLCKSZ - sizeof(PageHeaderData))) + 1));

		/*
		 * The tail page doesn't have enough space even for the chunk header.
		 * We extend the cc-table and store the compressed page from the next
		 * page.
		 */
		UnlockReleaseBuffer(buffer);

		extend_by = (c_len / (BLCKSZ - sizeof(PageHeaderData))) + 1;
		num_buffers = extend_by;
		buffers = palloc(sizeof(Buffer) * num_buffers);

		ExtendBufferedRelBy(BMR_REL(rel), MAIN_FORKNUM,
							NULL,
							0,
							extend_by,
							buffers,
							&extended_by);

		for (int i = 0; i < num_buffers; i++)
			LockBuffer(buffers[i], BUFFER_LOCK_EXCLUSIVE);

		skipent_offset = (BufferGetBlockNumber(buffers[0]) * BLCKSZ) +
			MAXALIGN(SizeOfPageHeaderData);
	}

	/*
	 * Insert a new entry to skiplist table.
	 *
	 * Inserting the compressed chunk is non-transactional since all tuples
	 * inside the chunk page don't have any visibility information and
	 * considered as visible anyway, whereas inserting the skiplist table
	 * entry is transactional since the skiplist table is a heap table.
	 * Therefore, we insert the skiplist table entry first and then insert the
	 * compressed chunk page. Note that we write a separate WAL for skiplist
	 * entry insertion from WAL for compressed page insertion.
	 *
	 * If the server crashes immediately after inserting a skiplist entry,
	 * nothing update will be visible thanks to a heap table (and CLOG).
	 */
	Assert(OidIsValid(RoadInsertState.skip_relid));
	skiplist_insert_entry(RoadInsertState.skip_relid, state->first_rownum,
						  skipent_offset);

	/* Write the compressed page to (multiple) blocks */
	road_write_data(rel, metabuffer, buffers, num_buffers, c_buffer, c_len);

	UnlockReleaseBuffer(metabuffer);
	for (int i = 0; i < num_buffers; i++)
		UnlockReleaseBuffer(buffers[i]);

	table_close(rel, NoLock);
}

static void
chunk_page_put_tuple(RoadInsertStateData * state, HeapTuple tuple)
{
	if (tuple->t_len > chunk_page_free_space(state->chunk_page))
	{
		road_flush_insert_state(state);

		Assert(tuple->t_len <= chunk_page_free_space(state->chunk_page));
	}

	/* XXX: use HeapTuple for now */
	chunk_page_add_item(state->chunk_page, (Item) tuple->t_data, tuple->t_len,
						InvalidOffsetNumber);
}

static RoadInsertStateData *
road_get_insert_state(Relation rel)
{
	if (!OidIsValid(RoadInsertState.relid))
	{
		Buffer		metabuf;
		RoadMetaPage *metap;


		metabuf = road_get_meta_page(rel);
		metap = (RoadMetaPage *) PageGetContents(BufferGetPage(metabuf));
		RoadInsertState.compression_method = metap->compression_method;
		UnlockReleaseBuffer(metabuf);

		RoadInsertState.relid = RelationGetRelid(rel);
		chunk_page_init((Page) RoadInsertState.chunk_page);

		ROAD_DEBUG_LOG("INSERT: initialize insert state relid %u skip_relid %u comp_method %d",
					   RoadInsertState.relid, RoadInsertState.skip_relid,
					   RoadInsertState.compression_method);
	}

	return &(RoadInsertState);
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
	 * Here you would most likely want to invent your own set of slot
	 * callbacks for your AM.
	 */
	return &TTSOpsVirtual;
}

static void
road_initscan(RoadScanDesc scan)
{
	scan->inited = false;
	scan->curbuf = InvalidBuffer;
	MemSet(scan->chunk_page, 0, ROAD_CHUNK_PAGE_SIZE);
}

static TableScanDesc
road_scan_begin(Relation relation, Snapshot snapshot,
				int nkeys, ScanKey key,
				ParallelTableScanDesc parallel_scan,
				uint32 flags)
{
	RoadScanDesc scan;

	scan = palloc0(sizeof(RoadScanDescData));

	scan->base.rs_rd = relation;
	scan->base.rs_snapshot = snapshot;
	scan->base.rs_nkeys = nkeys;
	scan->base.rs_flags = flags;
	scan->base.rs_parallel = parallel_scan;

	/*
	 * Disable page-at-a-time mode if it's not a MVCC-safe snapshot.
	 */
	if (!(snapshot && IsMVCCSnapshot(snapshot)))
	{
		scan->base.rs_flags &= ~SO_ALLOW_PAGEMODE;
	}

	if (nkeys > 0)
		scan->base.rs_key = (ScanKey) palloc(sizeof(ScanKeyData) * nkeys);
	else
		scan->base.rs_key = NULL;

	road_initscan(scan);

	return (TableScanDesc) scan;
}

static void
road_scan_end(TableScanDesc sscan)
{
	RoadScanDesc scan = (RoadScanDesc) sscan;

	if (BufferIsValid(scan->curbuf))
		ReleaseBuffer(scan->curbuf);

	if (scan->base.rs_key)
		pfree(scan->base.rs_key);

	pfree(scan);
}

static void
road_scan_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
				 bool allow_strat, bool allow_sync, bool allow_pagemode)
{
	RoadScanDesc scan = (RoadScanDesc) sscan;

	if (BufferIsValid(scan->curbuf))
		ReleaseBuffer(scan->curbuf);

	road_initscan(scan);
}

static void
road_read_chunk_page(Relation rel, Buffer *buf_p, BlockNumber *blk_p,
					 int64 *pos_p, int total_len, char *dest)
{
	BlockNumber blk = *blk_p;
	Buffer		buffer = *buf_p;
	int32		pos = *pos_p;
	char	   *ptr = dest;
	int			written = 0;

	for (;;)
	{
		int			len;
		Page		page = BufferGetPage(buffer);

		len = Min(((PageHeader) page)->pd_upper - pos, total_len - written);

		if (len == 0)
			goto next_blk;

		memcpy(ptr, (char *) page + pos, len);

		ROAD_DEBUG_LOG("SCAN: restore scan blk %u len %u pos %u written %u -> %u total_len %u",
					   BufferGetBlockNumber(buffer),
					   len, pos, written, written + len, total_len);

		written += len;
		ptr += len;
		pos += len;

		/*
		 * We hold the buffer content lock on the last block until we can all
		 * tuples on the chunk page.
		 */
		if (written >= total_len)
			break;

next_blk:
		UnlockReleaseBuffer(buffer);

		blk += 1;
		buffer = ReadBuffer(rel, blk);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		pos = MAXALIGN(SizeOfPageHeaderData);
	}

	/*
	 * Retrieved all data of compressed chunk page. Update the scan
	 * description for the next scan. Note that scan->curbuf is unlocked but
	 * pinned.
	 */
	*buf_p = buffer;
	*blk_p = blk;
	*pos_p = pos;
}

static Page
road_get_chunk_page(RoadScanDesc scan)
{
	Page		page;
	char		tmp[ROAD_CHUNK_PAGE_SIZE];
	ChunkHeader cdata;

	/* Adjust the current position to the beginning of the next block */
	if (scan->curpos + SizeOfChunkHeaderData > BLCKSZ)
	{
		UnlockReleaseBuffer(scan->curbuf);

		scan->curblk++;
		scan->curbuf = ReadBuffer(scan->base.rs_rd, scan->curblk);
		LockBuffer(scan->curbuf, BUFFER_LOCK_SHARE);
		scan->curpos = MAXALIGN(SizeOfPageHeaderData);

		ROAD_DEBUG_LOG("SCAN: adjust ptr because of page end, curblk %u curpos %lu",
					   scan->curblk, scan->curpos);
	}

	page = BufferGetPage(scan->curbuf);

	/*
	 * Advance the current position. Now it points to the first byte of the
	 * compressed chunk page.
	 */
	cdata = (ChunkHeader) ((char *) page + scan->curpos);
	scan->curpos += SizeOfChunkHeaderData;

	ROAD_DEBUG_LOG("SCAN: get chunk c_len %u c_hole_len %u c_hole_off %u: read blk %u ptr %lu ptr+header %lu",
				   cdata->c_len, cdata->c_hole_len, cdata->c_hole_off,
				   scan->curblk,
				   scan->curpos - SizeOfChunkHeaderData,
				   scan->curpos);

	/* Read the compressed chunk page from (possibly multiple) pages */
	road_read_chunk_page(scan->base.rs_rd, &scan->curbuf, &scan->curblk,
						 &scan->curpos, cdata->c_len, tmp);

	chunk_page_decompress(scan->compression_method, cdata, tmp,
						  scan->chunk_page);

	return (Page) scan->chunk_page;
}

static bool
road_scan_getnextslot(TableScanDesc sscan, ScanDirection direction,
					  TupleTableSlot *slot)
{
	RoadScanDesc scan = (RoadScanDesc) sscan;
	HeapTuple	tuple = &(scan->tuple);
	Page		chunk_page;
	OffsetNumber off;

	if (unlikely(!scan->inited))
	{
		Buffer		metabuffer;
		RoadMetaPage *metap;

		scan->inited = true;
		scan->tuple.t_tableOid = RelationGetRelid(scan->base.rs_rd);
		scan->curblk = 1;		/* block 0 is the meta page */
		scan->curpos = MAXALIGN(SizeOfPageHeaderData);	/* skip the page header */
		scan->curchunk = 0;

		/* Get the total number of chunks */
		metabuffer = road_get_meta_page(scan->base.rs_rd);
		metap = (RoadMetaPage *) PageGetContents(BufferGetPage(metabuffer));
		scan->maxchunk = metap->nchunks;
		scan->compression_method = metap->compression_method;
		UnlockReleaseBuffer(metabuffer);

		ROAD_DEBUG_LOG("SCAN: started from blk %u pos %lu maxchunks %lu",
					   scan->curblk, scan->curpos, scan->maxchunk);
	}
	else
	{
		off = scan->curoff;
		chunk_page = scan->chunk_page;

		goto continue_chunk_page;
	}

	while (scan->curchunk < scan->maxchunk)
	{
		CHECK_FOR_INTERRUPTS();

		if (BufferIsValid(scan->curbuf))
			ReleaseBuffer(scan->curbuf);

		scan->curbuf = ReadBuffer(scan->base.rs_rd, scan->curblk);
		LockBuffer(scan->curbuf, BUFFER_LOCK_SHARE);

		/* get new chunk */
		chunk_page = road_get_chunk_page(scan);
		off = FirstOffsetNumber;
		scan->maxoff = chunk_page_get_max_offsetnumber(chunk_page);

		ROAD_DEBUG_LOG("SCAN: start new chunk page max off %u", scan->maxoff);

continue_chunk_page:
		for (; off <= scan->maxoff; off = OffsetNumberNext(off))
		{
			ItemId		lpp = PageGetItemId(chunk_page, off);

			Assert(ItemIdIsNormal(lpp));

			tuple->t_data = (HeapTupleHeader) PageGetItem(chunk_page, lpp);
			tuple->t_len = ItemIdGetLength(lpp);
			ItemPointerSet(&(tuple->t_self), InvalidBlockNumber, off);

			scan->curoff = OffsetNumberNext(off);

			ExecForceStoreHeapTuple(&scan->tuple, slot, false);

			return true;
		}

		ROAD_DEBUG_LOG("SCAN: finished to scan on the %lu chunk (max chunk %lu)",
					   scan->curchunk, scan->maxchunk);

		LockBuffer(scan->curbuf, BUFFER_LOCK_UNLOCK);
		scan->curchunk++;
	}

	if (BufferIsValid(scan->curbuf))
		ReleaseBuffer(scan->curbuf);

	/* end of the scan */
	scan->curbuf = InvalidBuffer;

	return false;
}

static IndexFetchTableData *
road_index_fetch_begin(Relation rel)
{
	RoadIndexFetchData *rscan = palloc0(sizeof(RoadIndexFetchData));

	rscan->base.rel = rel;
	rscan->inited = false;
	rscan->first_rownum = InvalidRowNumber;
	rscan->last_rownum = InvalidRowNumber;

	return &rscan->base;
}

static void
road_index_fetch_reset(IndexFetchTableData *scan)
{
	RoadIndexFetchData *rscan = (RoadIndexFetchData *) scan;

	rscan->inited = false;
	rscan->first_rownum = InvalidRowNumber;
	rscan->last_rownum = InvalidRowNumber;
}

static void
road_index_fetch_end(IndexFetchTableData *scan)
{
	road_index_fetch_reset(scan);

	pfree(scan);
}

/*
 * Return true if we find the tuple with given (RowNumber-converted) tid.
 * Otherwise return false.
 */
static bool
road_scan_tuple_in_page(RoadIndexFetchData * rscan, ItemPointer tid,
						Page page, TupleTableSlot *slot)
{
	for (OffsetNumber off = FirstOffsetNumber;
		 off <= chunk_page_get_max_offsetnumber(page);
		 off = OffsetNumberNext(off))
	{
		ItemId		lp = PageGetItemId(page, off);
		HeapTupleHeader htup;

		Assert(ItemIdIsUsed(lp));
		htup = (HeapTupleHeader) PageGetItem(page, lp);

		if (ItemPointerEquals(&(htup->t_ctid), tid))
		{
			HeapTupleData tuple;

			tuple.t_data = htup;
			tuple.t_len = ItemIdGetLength(lp);
			tuple.t_tableOid = RelationGetRelid(rscan->base.rel);
			ItemPointerCopy(tid, &(tuple.t_self));

			ExecForceStoreHeapTuple(&tuple, slot, false);

			return true;
		}
	}

	return false;
}

static bool
road_index_fetch_tuple(struct IndexFetchTableData *scan,
					   ItemPointer tid,
					   Snapshot snapshot,
					   TupleTableSlot *slot,
					   bool *call_again, bool *all_dead)
{
	Relation	rel = scan->rel;
	RoadIndexFetchData *rscan = (RoadIndexFetchData *) scan;
	RowNumber	rownum;
	Page		page;
	bool		got_tuple;

	if (!rscan->inited)
	{
		Buffer		metabuffer;
		RoadMetaPage *metap;

		/* Get the compression method */
		metabuffer = road_get_meta_page(scan->rel);
		metap = (RoadMetaPage *) PageGetContents(BufferGetPage(metabuffer));
		rscan->compression_method = metap->compression_method;
		UnlockReleaseBuffer(metabuffer);

		rscan->inited = true;
	}

	rownum = tid_to_rownumber(tid);

	if (rscan->first_rownum != InvalidRowNumber &&
		rscan->last_rownum != InvalidRowNumber &&
		rscan->first_rownum <= rownum && rownum <= rscan->last_rownum)
	{
		/* We can use the cached chunk page */
		page = (Page) rscan->chunk_page;

		/*
		ROAD_DEBUG_LOG("IDXSCAN: use the cached page %lu <= %lu <= %lu",
					   rscan->first_rownum,
					   rownum,
					   rscan->last_rownum);
		*/
	}
	else
	{
		uint64		offset;
		RowNumber	first_rownum;
		ChunkHeader cdata;
		BlockNumber blkno;
		int64		blkoff;
		Buffer		buffer;
		char		tmp[ROAD_CHUNK_PAGE_SIZE];
		ItemId		lp;
		HeapTupleHeader htup;

		/*
		 * Find the offset of chunk page that potentially has the rownum.
		 */
		offset = skiplist_get_chunk_offset_for_rownum(rel, rownum,
													  snapshot, &first_rownum);

		ROAD_DEBUG_LOG("IDXSCAN: get offset %lu first_rownum %lu for rownum %lu",
					   offset, first_rownum, rownum);

		blkno = offset / BLCKSZ;
		blkoff = offset % BLCKSZ;

		buffer = ReadBuffer(rel, blkno);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);

		/* Get the chunk header data */
		page = BufferGetPage(buffer);
		cdata = (ChunkHeader) ((char *) page + blkoff);
		Assert(cdata->c_len > 0);
		blkoff += SizeOfChunkHeaderData;

		ROAD_DEBUG_LOG("IDXSCAN: get cheader len %u hole_len %u hole_off %u",
					   cdata->c_len, cdata->c_hole_len, cdata->c_hole_off);

		/* Read the compressed chunk page */
		road_read_chunk_page(rel, &buffer, &blkno, &blkoff, cdata->c_len, tmp);

		UnlockReleaseBuffer(buffer);

		/* Restore the chunk page into rscan->chunk_page */
		chunk_page_decompress(rscan->compression_method, cdata, tmp,
							  rscan->chunk_page);

		page = (Page) rscan->chunk_page;

		/* Cache the first and last rownumber in this chunk page */
		lp = PageGetItemId(page,
						   chunk_page_get_max_offsetnumber(page));
		htup = (HeapTupleHeader) PageGetItem(page, lp);

		rscan->first_rownum = first_rownum;
		rscan->last_rownum = tid_to_rownumber(&(htup->t_ctid));
		Assert(rscan->first_rownum != InvalidRowNumber);
		Assert(rscan->last_rownum != InvalidRowNumber);
	}

	/* Look for the tuple in the page by tid */
	got_tuple = road_scan_tuple_in_page(rscan, tid, page, slot);

	if (!got_tuple)
		*call_again = false;

	return got_tuple;
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
	RoadInsertStateData *state;
	ItemPointerData tid;
	RowNumber	rownum;
	bool		shouldFree;
	HeapTuple	tuple = ExecFetchSlotHeapTuple(slot, true, &shouldFree);

	state = road_get_insert_state(relation);

	if (!(state->called_in_atsam || state->called_in_ctas))
		ereport(ERROR,
				(errmsg("cannot insert tuple directly into a ROAD table"),
				 errhint("Use %s or %s to insert tuples",
						 "ALTER TABLE ... SET ACCESS METHOD",
						 "CREATE TABLE ... AS")));

	tuple->t_tableOid = slot->tts_tableOid;
	slot->tts_tableOid = RelationGetRelid(relation);

	/* Get the rownumber (and tid) of this tuple */
	rownum = state->cur_rownum;
	tid = rownumber_to_tid(rownum);

	/* Set its rownumber as tid */
	ItemPointerCopy(&tid, &(tuple->t_data->t_ctid));

	/* insert the tuple into the chunk page */
	chunk_page_put_tuple(state, tuple);

	/*
	 * Set the rownumber to return to the core. RowNumber will be the value of
	 * this tuple in indexes.
	 */
	ItemPointerCopy(&tid, &slot->tts_tid);

	if (rownum % 100 == 0)
		ROAD_DEBUG_LOG("INSERT: oid %u inserted tuple with rownum %lu tid (%u,%u)",
					   state->relid, rownum,
					   ItemPointerGetBlockNumber(&tuple->t_data->t_ctid),
					   ItemPointerGetOffsetNumber(&tuple->t_data->t_ctid));

	if (shouldFree)
		pfree(tuple);

	state->cur_rownum++;
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

/*
 * This is called at the end of bulk insertions such as CREATE TABLE ... AS,
 * COPY FROM, ALTER TABLE with table rewrite etc.
 */
static void
road_finish_bulk_insert(Relation relation, int options)
{
	RoadInsertStateData *state;

	state = road_get_insert_state(relation);

	if (state->first_rownum - state->cur_rownum == 0)
		return;

	road_flush_insert_state(state);

}

static void
road_relation_set_new_filelocator(Relation rel,
								  const RelFileLocator *newrlocator,
								  char persistence,
								  TransactionId *freezeXid,
								  MultiXactId *minmulti)
{
	SMgrRelation srel;
	PageHeader	hdr;
	RoadMetaPage metap;
	PGIOAlignedBlock block;
	Page		page = block.data;

	/*
	 * Cannot create a "road" table neither in a transaction block, a
	 * subtransaction, nor within a pipeline. This prevents the table from
	 * being read in the same transaction block, so we don't need to care
	 * about command id and subtransaction commit/rollback etc.
	 */
	if (IsTransactionBlock())
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("cannot create \"road\" table in a transaction block")));

	if (IsSubTransaction())
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("cannot create \"road\" table in a subtransaction")));

	if (MyXactFlags & XACT_FLAGS_PIPELINING)
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("cannot create \"road\" table within a pipeline")));

	if (persistence == RELPERSISTENCE_UNLOGGED)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("unlogged road tables are not supported")));

	*freezeXid = InvalidTransactionId;
	*minmulti = InvalidTransactionId;

	/* Create the storage for the road table */
	srel = RelationCreateStorage(*newrlocator, persistence, true);

	/*
	 * Create skiplist-table and insert the initial entry. If we're creating a
	 * ROAD table while ALTER TABLE ... SET ACCESS METHOD, we need to create a
	 * skiplist table for *the original table* rather than the given 'rel', a
	 * transient table. Otherwise, the skiplist table will be removed
	 * altogether.
	 */
	if (RoadInsertState.called_in_atsam)
	{
		Relation	oldrel;

		Assert(OidIsValid(RoadInsertState.atsam_relid));
		oldrel = table_open(RoadInsertState.atsam_relid, AccessExclusiveLock);
		RoadInsertState.skip_relid = skiplist_create_for_rel(oldrel);

		table_close(oldrel, NoLock);
	}
	else
		RoadInsertState.skip_relid = skiplist_create_for_rel(rel);

	PageInit(page, BLCKSZ, 0);
	hdr = (PageHeader) page;

	/* initialize the meta page */
	metap.version = ROAD_META_VERSION;
	metap.nchunks = 0;
	metap.compression_method = road_compression_method;

	memcpy(page + hdr->pd_lower, (char *) &metap, sizeof(RoadMetaPage));
	hdr->pd_lower += sizeof(RoadMetaPage);

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
road_scan_analyze_next_block(TableScanDesc scan, ReadStream *stream)
{
	return false;
}

static bool
road_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
							 double *liverows, double *deadrows,
							 TupleTableSlot *slot)
{
	return false;
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
	RoadScanDesc rscan;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	double		reltuples;
	EState	   *estate;
	ExprState  *predicate;
	ExprContext *econtext;
	TupleTableSlot *slot;

	/* XXX: need to support */
	if (start_blockno != 0 || numblocks != InvalidBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("BRIN indexes on archive tables are not supported")));

	if (scan)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("parallel index build on archive tables is not supported")));

	/* system catalog cannot use the road AM */
	Assert(!IsSystemRelation(tableRelation));

	/*
	 * Need an EState for evaluation of index expressions and partial-index
	 * predicates. Also a slot to hold the current tuple.
	 */
	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	slot = table_slot_create(tableRelation, NULL);

	econtext->ecxt_scantuple = slot;
	predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

	scan = table_beginscan_strat(tableRelation, /* relation */
								 SnapshotAny,	/* snapshot */
								 0, /* number of keys */
								 NULL,	/* scan key */
								 true,	/* buffer access strategy OK */
								 allow_sync);	/* syncscan OK? */

	rscan = (RoadScanDesc) scan;

	reltuples = 0;
	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		HeapTuple	tuple;

		CHECK_FOR_INTERRUPTS();

		/*
		 * Since all tuples in a road table are always visible, we don't need
		 * any visibility check here. A road table can be created only while
		 * the caller takes an AccessExclusiveLock, we don't need to care
		 * about concurrent insertions and updates too.
		 */
		tuple = (&rscan->tuple);
		reltuples += 1;

		MemoryContextReset(econtext->ecxt_per_tuple_memory);

		/* Set up for predicate or expression evaluation */
		ExecForceStoreHeapTuple(tuple, slot, false);

		/*
		 * In a partial index, discard tuples that don't satisfy the
		 * predicate.
		 */
		if (predicate != NULL && !ExecQual(predicate, econtext))
			continue;

		FormIndexDatum(indexInfo, slot, estate, values, isnull);

		/* Call the AM's callback routine to process the tuple */
		callback(indexRelation, &(tuple->t_data->t_ctid), values,
				 isnull, true, callback_state);
	}

	/* XXX: progress update here */

	table_endscan(scan);

	/* Clean up */
	ExecDropSingleTupleTableSlot(slot);
	FreeExecutorState(estate);

	/* There may have been pointing to the now-gone estate */
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_PredicateState = NULL;

	return reltuples;
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
	int32		data_length = 0;
	bool		maxlength_unknown = false;
	bool		has_toastable_attrs = false;
	TupleDesc	tupdesc = rel->rd_att;
	int32		tuple_length;
	int			i;

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
			int32		maxlen = type_maximum_size(att->atttypid,
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
		return false;			/* nothing to toast? */
	if (maxlength_unknown)
		return true;			/* any unlimited-length attrs? */
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
