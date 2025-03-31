// Copyright 2005 and onwards Google Inc.
// Modified for header-only implementation

#ifndef MINIPARQUET_SNAPPY_COMMON_H_
#define MINIPARQUET_SNAPPY_COMMON_H_

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

namespace snappy {

// Constants for Snappy format
static const int kBlockLog = 16;
static const size_t kBlockSize = 1 << kBlockLog;
static const int kMaxHashTableBits = 14;
static const size_t kMaxHashTableSize = 1 << kMaxHashTableBits;

// Mapping from i in range [0,4] to a mask to extract the bottom 8*i bits
static const uint32_t wordmask[] = {
  0u, 0xffu, 0xffffu, 0xffffffu, 0xffffffffu
};

// Data stored per entry in lookup table:
//      Range   Bits-used       Description
//      ------------------------------------
//      [0..64)  6             Literal/copy length encoded in opcode byte
//      [64..128) 7            Copy offset encoded in opcode byte / literal
//      [128..192) 8           Copy offset encoded in opcode byte / copy
//      [192..256) 8           Copy length encoded in opcode byte
enum {
  LITERAL = 0,
  COPY_1_BYTE_OFFSET = 1,  // 3 bit length + 3 bits of offset in opcode
  COPY_2_BYTE_OFFSET = 2,
  COPY_4_BYTE_OFFSET = 3
};

// Copy "len" bytes from "src" to "op", one byte at a time.  Used for
// handling COPY operations where the input and output regions may
// overlap.  For example, suppose:
//    src    == "ab"
//    op     == src + 2
//    len    == 20
// After IncrementalCopy(src, op, len), the result will have
// eleven copies of "ab"
//    ababababababababababab
// Note that this does not match the semantics of either memcpy()
// or memmove().
inline void IncrementalCopy(const char* src, char* op, size_t len) {
  do {
    *op++ = *src++;
  } while (--len > 0);
}

// Equivalent to IncrementalCopy except that it can write up to ten extra
// bytes after the end of the copy, and that it is faster.
//
// The main part of this loop is a simple copy of eight bytes at a time until
// we've copied (at least) the requested amount of bytes.  However, if op and
// src are less than eight bytes apart (indicating a repeating pattern of
// length < 8), we first need to expand the pattern in order to get the correct
// results. For instance, if the buffer looks like this, with the eight-byte
// <src> and <op> patterns marked as intervals:
//
//    abxxxxxxxxxxxx
//    [------]           src
//      [------]         op
//
// a single eight-byte copy from <src> to <op> will repeat the pattern once,
// after which we can move <op> two bytes without moving <src>:
//
//    ababxxxxxxxxxx
//    [------]           src
//        [------]       op
//
// and repeat the exercise until the two no longer overlap.
//
// This allows us to do very well in the special case of one single byte
// repeated many times, without taking a big hit for more general cases.
//
// The worst case of extra writing past the end of the match occurs when
// op - src == 1 and len == 1; the last copy will read from byte positions
// [0..7] and write to [1..8].
inline void IncrementalCopyFastPath(const char* src, char* op, size_t len) {
  while (op - src < 8) {
    // This could be more efficient, but it's hardly worth optimizing for
    // given that this condition is rare.
    *op++ = *src++;
    if (--len == 0) return;
  }

  // Copy from src to op until we've copied at least len bytes, or until
  // op catches up with src (in which case we're done).
  do {
    std::memcpy(op, src, 8);
    src += 8;
    op += 8;
    len -= 8;
  } while (len > 0);
}

} // namespace snappy

#endif // MINIPARQUET_SNAPPY_COMMON_H_
