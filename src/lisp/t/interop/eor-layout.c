/* Pinned C header oracle for xl_end_of_recovery's semantic field layout. */
#include "postgres.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include <stddef.h>
#include <stdio.h>
#include <string.h>

#undef printf

int
main(int argc, char **argv)
{
	xl_end_of_recovery record;
	FILE *output;

	if (argc != 2 || sizeof(record) != 24 ||
		offsetof(xl_end_of_recovery, end_time) != 0 ||
		offsetof(xl_end_of_recovery, ThisTimeLineID) != 8 ||
		offsetof(xl_end_of_recovery, PrevTimeLineID) != 12 ||
		offsetof(xl_end_of_recovery, wal_level) != 16 || WAL_LEVEL_LOGICAL != 2)
		return 1;
	/* C's writer does not promise cleared ABI padding; this fixture clears it. */
	memset(&record, 0, sizeof(record));
	record.end_time = -1;
	record.ThisTimeLineID = 5;
	record.PrevTimeLineID = 2;
	record.wal_level = WAL_LEVEL_LOGICAL;
	output = fopen(argv[1], "wb");
	if (output == NULL)
		return 2;
	if (fwrite(&record, sizeof(record), 1, output) != 1 || fclose(output) != 0)
		return 3;
	printf("eor-layout: size=24 offsets=0,8,12,16 wal_level=2 PASS\n");
	return 0;
}
