// Copyright 2005 and onwards Google Inc.
// Modified for header-only implementation

#ifndef MINIPARQUET_SNAPPY_DECOMPRESSION_H_
#define MINIPARQUET_SNAPPY_DECOMPRESSION_H_

#include "snappy_common.h"

namespace snappy {

// Reads the uncompressed length from a snappy compressed buffer.
// Returns true if the length was successfully read.
inline bool GetUncompressedLength(const char* compressed, size_t compressed_length,
                                 size_t* result) {
  if (compressed_length < 1) return false;
  
  // The first byte encodes the varint length
  const unsigned char* src = reinterpret_cast<const unsigned char*>(compressed);
  const unsigned char* src_end = src + compressed_length;
  
  // Read the varint
  size_t len = 0;
  int shift = 0;
  while (true) {
    if (src >= src_end) return false;
    unsigned char c = *src++;
    len |= static_cast<size_t>(c & 0x7f) << shift;
    if (!(c & 0x80)) break;
    shift += 7;
    if (shift > 63) return false; // Too big
  }
  
  *result = len;
  return true;
}

// Decompresses the snappy compressed buffer into the uncompressed buffer.
// Returns true if the decompression was successful.
inline bool RawUncompress(const char* compressed, size_t compressed_length,
                         char* uncompressed) {
  if (compressed_length == 0) return false;
  
  const unsigned char* src = reinterpret_cast<const unsigned char*>(compressed);
  const unsigned char* src_end = src + compressed_length;
  char* op = uncompressed;
  
  // Read the uncompressed length
  size_t uncompressed_len = 0;
  int shift = 0;
  while (true) {
    if (src >= src_end) return false;
    unsigned char c = *src++;
    uncompressed_len |= static_cast<size_t>(c & 0x7f) << shift;
    if (!(c & 0x80)) break;
    shift += 7;
    if (shift > 63) return false; // Too big
  }
  
  char* const op_limit = op + uncompressed_len;
  
  // Main decompression loop
  while (src < src_end) {
    unsigned char tag = *src++;
    unsigned int literal_length = tag >> 2;
    if (literal_length <= 60) {
      // Short literal
      if (src + literal_length > src_end) return false;
      if (op + literal_length > op_limit) return false;
      std::memcpy(op, src, literal_length);
      op += literal_length;
      src += literal_length;
    } else {
      // Long literal
      unsigned int len_bytes = literal_length - 60;
      if (src + len_bytes > src_end) return false;
      literal_length = 0;
      for (unsigned int i = 0; i < len_bytes; i++) {
        literal_length |= (*src++) << (i * 8);
      }
      if (src + literal_length > src_end) return false;
      if (op + literal_length > op_limit) return false;
      std::memcpy(op, src, literal_length);
      op += literal_length;
      src += literal_length;
    }
    
    // Copy operation
    if (src < src_end) {
      unsigned int copy_offset = 0;
      unsigned int copy_length = (tag & 3) << 2;
      
      // Extract copy offset
      if ((tag & 3) == 0) {
        // COPY_1_BYTE_OFFSET
        if (src >= src_end) return false;
        copy_offset = *src++;
        copy_length += 4;
      } else if ((tag & 3) == 1) {
        // COPY_2_BYTE_OFFSET
        if (src + 1 >= src_end) return false;
        copy_offset = *src | (*(src + 1) << 8);
        src += 2;
        copy_length += 1;
      } else {
        // COPY_4_BYTE_OFFSET
        if (src + 3 >= src_end) return false;
        copy_offset = *src | (*(src + 1) << 8) | (*(src + 2) << 16) | (*(src + 3) << 24);
        src += 4;
      }
      
      // Extract copy length for COPY_4_BYTE_OFFSET
      if ((tag & 3) == 3) {
        if (src >= src_end) return false;
        copy_length += *src++;
      }
      
      // Validate copy operation
      if (copy_offset == 0 || copy_offset > static_cast<unsigned int>(op - uncompressed)) {
        return false;
      }
      
      if (op + copy_length > op_limit) return false;
      
      // Perform copy operation
      const char* copy_src = op - copy_offset;
      if (copy_offset < copy_length) {
        // Slow path - overlapping copy
        IncrementalCopy(copy_src, op, copy_length);
      } else {
        // Fast path - non-overlapping copy
        IncrementalCopyFastPath(copy_src, op, copy_length);
      }
      op += copy_length;
    }
  }
  
  return op == op_limit;
}

} // namespace snappy

#endif // MINIPARQUET_SNAPPY_DECOMPRESSION_H_
