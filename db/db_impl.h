#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include <atomic>
#include <deque>
#include <set>
#include <string>

#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

class DBImpl : public DB {
public:
    DBImpl(const Options& options, const std::string& dbname);

    DBImpl(const DBImpl&) = delete;
    DBImpl& operator=(const DBImpl&) = delete;

    virtual ~DBImpl();

    // Implementations of the DB interface
    virtual Status Put(const WriteOptions&, const Slice& key, const Slice& value);
    virtual Status Delete(const WriteOptions&, const Slice& key);
    virtual Status Write(const WriteOptions& options, WriteBatch* updates);
    virtual Status Get(const ReadOptions& options, const Slice& key,
                        std::string* value);
    virtual Iterator* NewIterator(const ReadOptions&);
    virtual const Snapshot* GetSnapshot();
    virtual void ReleaseSnapshot(const Snapshot* snapshot);
    virtual bool GetProperty(const Slice& property, std::string* value);
    virtual void GetApproximateSizes(const Range* range, int n, uint64_t* sizes);
    virtual void CompactRange(const Slice* begin, const Slice* end);

    // Extra methods (for testing) that are not in the public DB interface
    
    // Compact any files in the named level that overlap [*begin, *end]
    void TEST_CompactRange(int level, const Slice* begin, const Slice* end);

    // Force current memtable contents to be compacted.
    Status TEST_CompactMemTable();

    // Return an internal iterator over the current state of the database.
    // The keys of this iterator are internal keys (see format.h)
    // The returned iterator should be deleted when no longer needed.
    Iterator* TEST_NewInternalIterator();

    // Return the maximum overlapping data (in bytes) at next level for any
    // file at a level >= 1.
    int64_t TEST_MaxNextLevelOverlappingBytes();

    // Record a smaple of bytes read at the specified internal key.
    // Samples are taken approximately once every config::kReadBytesPeriod
    // bytes.
    void RecordReadSample(Slice key);

private:
    friend class DB;
    struct CompactionState;
    struct Writer;

    // Information for a manual compaction
    struct ManualCompaction {
        int level;
        bool done;
        const InternalKey* begin;       // null means beginning of key range
        const InternalKey* end;         // null means end of key range
        InternalKey tmp_storage;        // Used to keep track of compaction progress
    };

    // Per level compaction stats. stats_[level] stores the stats for
    // compactions that produced data for the specified "level".
    struct CompactionStats {
        CompactionStats() : micros(0), bytes_read(0), bytes_written(0) {}

        void Add(const CompactionStats& c) {
            this->micros += c.micros;
            this->bytes_read += c.bytes_read;
            this->bytes_written += c.bytes_written;
        }

        int64_t micros;
        int64_t bytes_read;
        int64_t bytes_written;
    };

    Iterator* NewInternalIterator(const ReadOptions&,
                                    SequenceNumber* latest_snapshot,
                                    uint32_t* seed);

    Status NewDB();

    // Recover the descriptor from persistent storage. May do a significant
    // amount of work to recover recently logged updates. Any changes to
    // be made to the descriptor are added to *edit
    Status Recover(VersionEdit* edit, bool* save_mainfest)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    void MaybeIgnoreError(Status* s) const;

    // Delete any unneeded files and stale in-memory entries.
    void DeleteObsoleteFiles() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    // Compact the in-memory write buffer to disk. Switches to a new
    // log-file/memtable and writes a new descriptor iff successful.
    // Errors are recorded in bg_error_.
    void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    Status RecoverLogFile(uint64_t log_number, bool last_log, bool* save_manifest,
                            VersionEdit* edit, SequenceNumber* max_sequence)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    Status WriteLevel0Table(MemTable* mem, VersionEdit* eidt, Version* base)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    Status MakeRoomForWrite(bool force /* compact even if there is room? */)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    WriteBatch* BuildBatchGroup(Writer** last_writer)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    void RecordBackgroundError(const Status& s);

    void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    static void BGWork(void* db);
    void BackgroundCall();
    void BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    void CleanupCompaction(CompactionState* compact)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    Status DoCompactionWrok(CompactionState* compact)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    Status OpenCompactionOutputFile(CompactionState* compact);
    Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
    Status InstallCompactionResults(CompactionState* compact)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    const Comparator* user_comparator() const {
        return internal_comparator_.user_comparator();
    }

    // Constant after construction
    Env* const env_;
    const InternalKeyComparator internal_comparator_;
    const InternalFilterPolicy internal_filter_policy_;
    const Options options_; // options_.comparator == &internal_.comparator_
    const bool owns_info_log_;
    const bool owns_cache_;
    const std::string dbname_;


};

} // namespace leveldb

#endif
