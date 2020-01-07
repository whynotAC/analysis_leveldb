#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>

#include "db/dbformat.h"

namespace leveldb {

class VersionSet;

struct FileMetaData {
    FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) {}

    int refs;
    int allowed_seeks;      // Seeks allowed until compaction
    uint64_t number;
    uint64_t file_size;     // File Size in byres
    InternalKey smallest;   // Smallest internal key served by table
    InternalKey largest;    // Largest internal key served by table
};

class VersionEdit {
  public:
    VersionEdit() { Clear(); }
    ~VersionEdit() {}

    void Clear();

    void SetComparatorName(const Slice& name) {
        has_comparator_ = true;
        comparator_ = name.ToString();
    }
    void SetLogNumber(uint64_t num) {
        has_log_number_ = true;
        log_number_ = num;
    }
    void SetPrevLogNumber(uint64_t num) {
        has_prev_log_number_ = true;
        prev_log_number_ = num;
    }
    void SetNextFile(uint64_t num) {
        has_next_file_number_ = true;
        next_file_number_ = num;
    }
    void SetLastSequence(SequenceNumber seq) {
        has_last_sequence_ = true;
        last_sequence_ = seq;
    }
    void SetCompactPointer(int level, const InternalKey& key) {
        compact_pointers_.push_back(std::make_pair(level, key));
    }

};

} // namespace leveldb

#endif
