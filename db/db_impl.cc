#include "db/db_impl.h"

#include <stdint.h>
#include <stdio.h>

#include <algorithm>
#include <atomic>
#include <set>
#include <string>
#include <vector>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct DBImpl::Writer {
    explicit Writer(port::Mutex* mu)
        : batch(nullptr), sync(false), done(false), cv(mu) {}

    Status status;
    WriteBatch* batch;
    bool sync;
    bool done;
    port::CondVar cv;
};

struct DBImpl::CompactionState {
    // Files produced by compaction
    struct Output {
        uint64_t number;
        uint64_t file_size;
        InternalKey smallest, largest;
    };

    Output* current_output() { return &outputs[outputs.size() - 1]; }

    explicit CompactionState(Compaction* c)
        : compaction(c),
          smallest_snapshot(0),
          outfile(nullptr),
          builder(nullptr),
          total_bytes(0) {}

    Compaction* const compaction;

    // Sequence numbers < smallest_snapshot are not significant since we
    // will never have to service a snapshot below smallest_snapshot
    // Therebefore if we have seen a sequence number 5 <= smallest_snapshot,
    // we can drop all entries for the same key with sequence numbers < S.
    SequenceNumber smallest_snapshot;

    std::vector<Output> outputs;

    // State kept for output being generated
    WritableFile* outfile;
    TableBuilder* builder;

    uint64_t total_bytes;
};

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
    if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
    if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
    Options result = src;
    result.comparator = icmp;
    result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
    ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);
    ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
    ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);
    ClipToRange(&result.block_size, 1 << 10, 4 << 20);
    if (result.info_log == nullptr) {
        // Open a log file in the same directory as the db
        src.env->CreateDir(dbname);     // In case it does not exist
        src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
        Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
        if (!s.ok()) {
            // No place suitable for logging
            result.info_log = nullptr;
        }
    }
    if (result.block_cache == nullptr) {
        result.block_cache = NewLRUCache(8 << 20);
    }
    return result;
}

static int TableCacheSize(const Options& sanitized_options) {
    // Reserve ten files or so far other uses and give the rest to TableCache.
    return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                                &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
      db_lock_(nullptr),
      shutting_down_(false),
      background_work_finished_signal_(&mutex_),
      mem_(nullptr),
      imm_(nullptr),
      has_imm_(nullptr),
      logfile_(nullptr),
      logfile_number_(0),
      log_(nullptr),
      seed_(0),
      tmp_batch_(new WriteBatch),
      background_compaction_scheduled_(false),
      manual_compaction_(nullptr),
      versions_(new VersionSet(dbname_, &options_, table_cache_,
                                &internal_comparator_)) {}

DBImpl::~DBImpl() {
    // Wait for background work to finish.
    mutex_.Lock();
    shutting_down_.store(true, std::memory_order_release);
    while (background_compaction_scheduled_) {
        background_work_finished_signal_.Wait();
    }
    mutex_.Unlock();

    if (db_lock_ != nullptr) {
        env->UnlockFile(db_lock_);
    }

    delete version_;
    if (mem_ != nullptr) mem_->Unref();
    if (imm_ != nullptr) imm_->Unref();
    delete tmp_batch_;
    delete log_;
    delete logfile_;
    delete table_cache_;

    if (owns_info_log_) {
        delete options.info_log;
    }
    if (owns_cache_) {
        delete options.block_cache;
    }
}

Status DBImpl::NewDB() {
    VersionEdit new_db;
    new_db.SetComparatorName(user_comparator()->Name());
    new_db.SetLogNumber(0);
    new_db.SetNextFile(2);
    new_db.SetLastSequence(0);

    const std::string manifest = DescriptorFileName(dbname_, 1);
    WritableFile* file;
    Status s = env_->NewWritableFile(manifest, &file);
    if (!s.ok()) {
        return s;
    }
    {
        log::Writer log(file);
        std::string record;
        new_db.EncodeTo(&record);
        s = log.AddRecord(record);
        if (s.ok()) {
            s = file->Close();
        }
    }
    delete file;
    if (s.ok()) {
        // Make "CURRENT" file that points to the new manifest file.
        s = SetCurrentFile(env_, dbname_, 1);
    } else {
        env_->DeleteFile(manifest);
    }
    return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
    if (s->ok() || options_paranoid_checks) {
        // No change needed
    } else {
        Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
        *s = Status::OK();
    }
}

void DBImpl::DeleteObsoleteFiles() {
    mutex_.AssertHeld();

    if (!bg_error_.ok()) {
        // After a background error, we don't know whether a new version may
        // or may not have been committed, so we cannot safely garbage collect.
        return;
    }

    // Make a set of all of the live files
    std::set<uint64_t> live = pending_outputs_;
    versions_->AddLiveFiles(&live);

    std::vector<std::string> filenames;
    env_->GetChildren(dbname_, &filenames);     // Ignoring errors on purpose
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
        if (ParseFileName(filenames[i], &number, &type)) {
            bool keep = true;
            switch (type) {
                case kLogFile:
                    keep = ((number >= versions_->LogNumber()) ||
                            (number == version_->PrevLogNumber()));
                    break;
                case kDescriptorFile:
                    // keep my manifest file, and any newer incarnations'
                    // (in case these is a race that allows other incarnations)
                    keep = (number >= versions_->ManifestFileNumber());
                    break;
                case  
            }
        }
    }
}

} // namespace leveldb
