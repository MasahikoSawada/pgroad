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
	ereport(DEBUG1, (errhidestmt(true), errmsg(__VA_ARGS__)))

typedef uint64 RowNumber;
#define InvalidRowNumber	(RowNumber) PG_UINT64_MAX
#define MaxRowNumber		(RowNumber) (PG_UINT64_MAX - 1)

#define ROAD_META_VERSION	0
#define ROAD_META_BLOCKNO	0

#define ROAD_CHUNK_PAGE_SIZE	(4 * BLCKSZ)

typedef struct RoadMetaPage
{
	uint32		version;

	int64		nchunks;
	int			compression_method;
} RoadMetaPage;


/* Header data of a compressed chunk page */
typedef struct ChunkHeaderData
{
	uint32		c_len;
	uint32		c_hole_off;
	uint32		c_hole_len;
} ChunkHeaderData;
typedef ChunkHeaderData * ChunkHeader;
#define SizeOfChunkHeaderData MAXALIGN(sizeof(ChunkHeaderData))

#define MaxHeapTuplesPerChunkPage    \
    ((int) ((ROAD_CHUNK_PAGE_SIZE - SizeOfPageHeaderData) / \
            (MAXALIGN(SizeofHeapTupleHeader) + sizeof(ItemIdData))))

/*
 * XXX: we will use ArchiteTupleHeaderData for tuples inserted to a chunk
 * page. But not used for now.
 */
#if 0
typedef struct ArchiveTupleHeaderData
{
	RowNumber	t_rownum;
	uint16		t_infomask;
	uint8		t_hoff;
	bits8		t_bits[FLEXIBLE_ARRAY_MEMBER];	/* bitmap of NULLs */

	/* MORE DATA FOLLOWS AT END OF STRUCT */
} ArchiveTupleHeaderData;
typedef struct ArchiveTupleHeaderData *ArchiveTupleHeader;

/* flags for t_infomask */
#define ARCHIVE_NATTS_MASK		0x07FF	/* 11 bits for number of attributes */
#define ARCHIVE_HASNULL         0x0800	/* has null attribute(s) */
#define ARCHIVE_HASVARWIDTH     0x1000	/* has variable-width attribute(s) */
#define ARCHIVE_HASEXTERNAL     0x2000	/* has external stored attribute(s) */

#define ArchiveTupleHeaderGetNatts(tup) \
    ((tup)->t_infomask & ARCHIVE_NATTS_MASK)
#endif

extern void d(const char *msg, char *data, int len);

#endif							/* ROAD_AM_H */
