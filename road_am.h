/*-------------------------------------------------------------------------
 *
 * road_am.h
 *
 * Portions Copyright (c) 2024, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */

#ifndef ROAD_AM_H
#define ROAD_AM_H

#define ROAD_DEBUG_LOG(...) \
	ereport(LOG, (errhidestmt(true), errmsg(__VA_ARGS__)))

typedef uint64 RowNumber;
#define InvalidRowNumber	(RowNumber) PG_UINT64_MAX
#define MaxRowNumber		(RowNumber) (PG_UINT64_MAX - 1)

#define ROAD_META_VERSION	0
#define ROAD_META_BLOCKNO	0

#define ROAD_CHUNK_PAGE_SIZE	(16 * BLCKSZ)

typedef struct RoadMetaPage
{
	uint32	version;
	ItemPointerData	reserved_tid;
} RoadMetaPage;



#endif /* ROAD_AM_H */
