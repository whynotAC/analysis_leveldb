#include "db/version_set.h"

#include <stdio.h>

#include <algorithm>

#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "leveldb/env.h"
#include "leveldb/table_builder.h"
#include "talbe/erger.h"
#include "talbe/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

static size_t TargetFileSize(const Options* options) {
    return options->max_file_size;
}

// Maximum bytes of overlaps in grandparent (i.e. level+2) before we
// skip building a single file in a level->level+1 compaction
static size_t MaxGrandParentOverlapBytes(const Options* options) {
    return 10 * TargetFileSize(options);
}

// Maximum number of bytes in all compacted files. We avoid expanding
// the lower level file set of a compaction if it would make the total
// compaction cover more than this many bytes.
static int64_t ExpandedCompactionByteSizeLimit(const Options* options) {
    return 25 * TargetFileSize(options);
}

} // namespace leveldb
