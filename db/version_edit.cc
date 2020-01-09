#include "db/version_edit.h"

#include "db/version_set.h"
#include "util/coding.h"

namespace leveldb {

// Tag numbers for serialized VersionEdit. These numbers are written to 
// disk and should not be changed.
enum Tag {
    kComparator = 1,
    kLogNumber = 2,
    kNextFileNumber = 3,
    kLastSequence = 4,
    kCompactPointer = 5,
    kDeletedFile = 6,
    kNewFile = 7,
    // 8 was used for large value refs
    kPrevLogNumber = 9
};

void VersionEdit::Clear() {
    comparator_.clear();
    log_number_ = 0;
    prev_log_number_ = 0;
    last_sequence_ = 0;
    next_file_number_ = 0;
    has_comparator_ = false;
    has_log_number_ = false;
    has_prev_log_number_ = false;
    has_next_file_number_ = false;
    has_last_sequence_ = false;
    deleted_files_.clear();
    new_files_.clear();
}

void VersionEdit::EncodeTo(std::string* dst) const {
    if (has_comparator_) {
        PutVarint32(dst, kComparator);
        PutLengthPrefixedSlice(dst, comparator_);
    }
    if (has_log_number_) {
        PutVarint32(dst, kLogNumber);
        PutVarint64(dst, log_number_);
    }
    if (has_prev_log_number_) {
        PutVarint32(dst, kPrevLogNumber);
        PutVarint64(dst, prev_log_number_);
    }
    if (has_next_file_number_) {
        PutVarint32(dst, kNextFileNumber);
        PutVarint64(dst, next_file_number_);
    }
    if (has_last_sequence_) {
        PutVarint32(dst, kLastSequence);
        PutVarint64(dst, last_sequence_);
    }

    for (size_t i = 0; i < compact_pointers_.size(); i++) {
        PutVarint32(dst, kCompactPointer);
        PutVarint32(dst, compact_pointers_[i].first);   // level
        PutLengthPrefixedSlice(dst, compact_pointers_[i].second.Encode());
    }

    for (DeletedFileSet::const_iterator iter = deleted_files_.begin();
            iter != deleted_files_.end(); ++iter) {
        PutVarint32(dst, kDeletedFile);
        PutVarint32(dst, iter->first);  // level
        PutVarint64(dst, iter->second); // file number
    }

    for (size_t i = 0; i < new_files_.size(); i++) {
        const FileMetaData& f = new_files_[i].second;
        PutVarint32(dst, kNewFile);
        PutVarint32(dst, new_files_[i].first);  // level
        PutVarint64(dst, f.number);
        PutVarint64(dst, f.file_size);
        PutLengthPrefixedSlice(dst, f.smallest.Encode());
        PutLengthPrefixedSlice(dst, f.largest.Encode());
    }
}

static bool Get

}
