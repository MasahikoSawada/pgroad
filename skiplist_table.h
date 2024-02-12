#ifndef SKIPLIST_H
#define SKIPLIST_H

extern Oid	skiplist_create_for_rel(Relation rel);
extern Relation skiplist_table_open(Relation rel, LOCKMODE lockmode);
extern void skiplist_insert_entry(Oid skip_relid, RowNumber first_rownum,
								  uint64 offset);
extern int64 skiplist_get_chunk_offset_for_rownum(Relation rel, RowNumber rownum,
												  Snapshot snapshot,
												  RowNumber * first_rownum);

#endif							/* SKIPLIST_H */
