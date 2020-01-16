#ifndef STORAGE_LEVELDB_DB_LOG_READER_H_
#define STORAGE_LEVELDB_DB_LOG_READER_H_

#include <stdint.h>

#include "db/log_format.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

class SequentialFile;

namespace log {

class Reader {
  public:
    // Interface for reporting errors
    class Reporter {
    public:
        virtual ~Reporter();

        // Some corruption was detected. "size" is the approximate number
        // of bytes dropped due to the corruption
        virtual void Corruption(size_t bytes, const Status& status) = 0;
    };

    // Create a reader that will return log records for "*file".
    // "*file" msut remain live while this Reader is in use.
    //
    // If "reporter" is non-null, it is notified whenever some data is
    // dropped due to a detected corruption. "*reporter" must remain
    // live while this Reader is in use.
    //
    // If "checksum" is true, verify checksums if available.
    //
    // The Reader will start reading at the first record located at physical
    // position >= initial_offset within the file.
    Reader(SequentialFile* file, Reporter* reporter, bool checksum,
            uint64_t initial_offset);

    Reader(const Reader&) = delete;
    Reader& operator=(const Reader&) = delete;

    ~Reader();

    // Read the next record into *record. Returns true if read
    // successfully, false if we hit end of the input. May use
    // "*scratch" as temporary storage. The contents filled in *record
    // will only be valid until the next mutating operation on this
    // reader or the next mutation to *scratch.
    bool ReadRecord(Slice* record, std::string* scratch);

    // Returns the physical offset of the last record returned by ReadRecord.
    //
    // Undefined before the first call to ReadRecord.
    uint64_t LastRecordOffset();

  private:
    // Extend record types with the following special values
    enum {
        kEof = kMaxRecordType + 1,
        // Returned whenever we find an invalid physical record.
        // Currently there are three situations in which this happens:
        // * The record has an invalid CRC (ReadPhysicalRecord reports a drop)
        // * The record is a 0-length record (No drop is reported)
        // * The record is below constructor's initial_offset (No drop is
        // reported)
        kBadRecord = kMaxRecordType + 2
    };

    // Skips all blocks that are completely before "initial_offset_".
    //
    // Returns true on success. Handles reporting.
    bool SkipToInitialBlock();


};

} // namespace log

} // namespace leveldb

#endif
