#include "util/coding.h"

namespace leveldb {

void EncodeFixed32(char* dst, uint32_t value) {
    if (port::kLittleEndian) {
        memcpy(dst, &value, sizeof(value));
    } else {
        dst[0] = value & 0xff;
        dst[1] = (value >> 8) & 0xff;
        dst[2] = (value >> 16) & 0xff;
        dst[3] = (value >> 24) & 0xff;
    }
}

void EncodeFixed64(char* dst, uint64_t value) {
    if (port::kLittleEndian) {
        memcpy(dst, &value, sizeof(value));
    } else {
        dst[0] = value & 0xff;
        dst[1] = (value >> 8) & 0xff;
        dst[2] = (value >> 16) & 0xff;
        dst[3] = (value >> 24) & 0xff;
        dst[4] = (value >> 32) & 0xff;
        dst[5] = (value >> 40) & 0xff;
        dst[6] = (value >> 48) & 0xff;
        dst[7] = (value >> 56) & 0xff;
    }
}

void PutFixed32(std::string* dst, uint32_t value) {
    char buf[sizeof(value)];
    EncodeFixed32(buf, value);
    dst->append(buf, sizeof(buf));
}

void PutFixed64(std::string* dst, uint64_t value) {
    char buf[sizeof(value)];
    EncodeFixed64(buf, value);
    dst->append(buf, sizeof(buf));
}

char* EncodeVarint32(char* dst, uint32_t v) {
    // Operate on characters as unsigneds
    unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
    static const int B = 128;
    if (v < (1 << 7)) {
        *(ptr++) = v;
    } else if (v < (1 << 14)) {
        *(ptr++) = v | B;
        *(ptr++) = v >> 7;
    } else if (v < (1 << 21)) {
        *(ptr++) = v | B;
        *(ptr++) = (v >> 7) | B;
        *(ptr++) = v >> 14;
    } else if (v < (1 << 28)) {
        *(ptr++) = 
    }
}

} // namespace leveldb
