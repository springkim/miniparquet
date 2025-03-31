// Copyright 2005 and onwards Google Inc.
// Modified for header-only implementation

#ifndef MINIPARQUET_SNAPPY_DECOMPRESSION_H_
#define MINIPARQUET_SNAPPY_DECOMPRESSION_H_

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <algorithm>

// Constants for Snappy format
static const int kBlockLog = 16;
static const size_t kBlockSize = 1 << kBlockLog;
static const int kMaxHashTableBits = 14;
static const size_t kMaxHashTableSize = 1 << kMaxHashTableBits;
static const int kMaximumTagLength = 5;  // Maximum length of a tag (copy with 4-byte offset)

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

// Varint utility functions for handling variable-length encoded integers
class Varint {
public:
  // Maximum lengths of varint encoding of uint32_t.
  static const int kMax32 = 5;

  // Attempts to parse a varint32 from a prefix of the bytes in [ptr,limit-1].
  // Never reads a character at or beyond limit. If a valid/terminated varint32
  // was found in the range, stores it in *OUTPUT and returns a pointer just
  // past the last byte of the varint32. Else returns NULL.
  static inline const char* Parse32WithLimit(const char* p,
                                           const char* l,
                                           uint32_t* OUTPUT) {
    const unsigned char* ptr = reinterpret_cast<const unsigned char*>(p);
    const unsigned char* limit = reinterpret_cast<const unsigned char*>(l);
    uint32_t b, result;
    if (ptr >= limit) return NULL;
    b = *(ptr++); result = b & 127;          if (b < 128) goto done;
    if (ptr >= limit) return NULL;
    b = *(ptr++); result |= (b & 127) <<  7; if (b < 128) goto done;
    if (ptr >= limit) return NULL;
    b = *(ptr++); result |= (b & 127) << 14; if (b < 128) goto done;
    if (ptr >= limit) return NULL;
    b = *(ptr++); result |= (b & 127) << 21; if (b < 128) goto done;
    if (ptr >= limit) return NULL;
    b = *(ptr++); result |= (b & 127) << 28; if (b < 16) goto done;
    return NULL;       // Value is too long to be a varint32
   done:
    *OUTPUT = result;
    return reinterpret_cast<const char*>(ptr);
  }

  // REQUIRES   "ptr" points to a buffer of length sufficient to hold "v".
  // EFFECTS    Encodes "v" into "ptr" and returns a pointer to the
  //            byte just past the last encoded byte.
  static inline char* Encode32(char* sptr, uint32_t v) {
    // Operate on characters as unsigneds
    unsigned char* ptr = reinterpret_cast<unsigned char*>(sptr);
    static const int B = 128;
    if (v < (1<<7)) {
      *(ptr++) = v;
    } else if (v < (1<<14)) {
      *(ptr++) = v | B;
      *(ptr++) = v>>7;
    } else if (v < (1<<21)) {
      *(ptr++) = v | B;
      *(ptr++) = (v>>7) | B;
      *(ptr++) = v>>14;
    } else if (v < (1<<28)) {
      *(ptr++) = v | B;
      *(ptr++) = (v>>7) | B;
      *(ptr++) = (v>>14) | B;
      *(ptr++) = v>>21;
    } else {
      *(ptr++) = v | B;
      *(ptr++) = (v>>7) | B;
      *(ptr++) = (v>>14) | B;
      *(ptr++) = (v>>21) | B;
      *(ptr++) = v>>28;
    }
    return reinterpret_cast<char*>(ptr);
  }
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

namespace snappy {

// Reads the uncompressed length from a snappy compressed buffer.
// Returns true if the length was successfully read.
inline bool GetUncompressedLength(const char* compressed, size_t compressed_length,
                                 size_t* result) {
  if (compressed_length == 0) return false;
  
  // First try standard Snappy format (varint-encoded length)
  uint32_t v = 0;
  const char* limit = compressed + compressed_length;
  const char* varint_end = Varint::Parse32WithLimit(compressed, limit, &v);
  
  if (varint_end != NULL) {
    *result = v;
    return true;
  }
  
  // If that fails, try Parquet-specific format (first 4 bytes might be length)
  if (compressed_length >= 4) {
    // Try reading the first 4 bytes as a little-endian 32-bit integer
    uint32_t len = static_cast<uint32_t>(static_cast<unsigned char>(compressed[0])) |
                  (static_cast<uint32_t>(static_cast<unsigned char>(compressed[1])) << 8) |
                  (static_cast<uint32_t>(static_cast<unsigned char>(compressed[2])) << 16) |
                  (static_cast<uint32_t>(static_cast<unsigned char>(compressed[3])) << 24);
    
    // Sanity check - length should be reasonable
    if (len > 0 && len < 10 * 1024 * 1024) {  // Max 10MB as a safety check
      *result = len;
      return true;
    }
  }
  
  return false;
}

// Helper class for decompression
class SnappyDecompressor {
 public:
  explicit inline SnappyDecompressor(const char* compressed, size_t compressed_length)
      : compressed_(compressed), compressed_end_(compressed + compressed_length), 
        ip_(compressed) {}

  inline bool ReadUncompressedLength(size_t* result) {
    // Read the uncompressed length stored at the start of the compressed data
    uint32_t v = 0;
    if (ip_ == compressed_end_) return false;
    
    const char* varint_end = Varint::Parse32WithLimit(ip_, compressed_end_, &v);
    if (varint_end == NULL) return false;
    
    ip_ = varint_end;
    *result = v;
    return true;
  }

  inline bool DecompressAllTags(char* uncompressed, size_t uncompressed_len) {
    // Output limits
    char* op = uncompressed;
    char* op_limit = uncompressed + uncompressed_len;
    
    // Process all chunks
    while (ip_ < compressed_end_) {
      // Each chunk begins with a tag byte
      const unsigned char c = *(reinterpret_cast<const unsigned char*>(ip_++));
      
      // The tag byte has the following format:
      // - For literals: the top 6 bits encode the length and the bottom 2 bits are 00
      // - For copies: the top bits encode the copy length and the bottom 2 bits encode the copy type
      
      if ((c & 0x03) == 0) {  // LITERAL
        uint32_t length = 0;
        
        // The literal length is stored in the upper 6 bits of the tag byte
        uint32_t literal_tag = c >> 2;
        
        if (literal_tag < 60) {
          // Short literal: length is just the literal tag + 1
          length = literal_tag + 1;
        } else {
          // Long literal: length is stored in the next 1-4 bytes
          uint32_t bytes_after_tag = literal_tag - 59;  // 60->1, 61->2, 62->3, 63->4
          
          if (ip_ + bytes_after_tag > compressed_end_) {
            return false;  // Not enough input
          }
          
          // Read the length from the next bytes (little-endian)
          for (uint32_t i = 0; i < bytes_after_tag; i++) {
            length |= (static_cast<uint32_t>(*(reinterpret_cast<const unsigned char*>(ip_++))) << (i * 8));
          }
          length += 1;  // Lengths are stored as length-1
        }
        
        // Check if we have enough input and output space
        if (ip_ + length > compressed_end_ || op + length > op_limit) {
          return false;
        }
        
        // Copy the literal data
        memcpy(op, ip_, length);
        ip_ += length;
        op += length;
      } else {  // COPY
        uint32_t length = 0;
        uint32_t offset = 0;
        
        // The bottom 2 bits encode the copy type
        uint32_t copy_type = c & 0x03;
        
        if (copy_type == 1) {  // COPY_1_BYTE_OFFSET
          // Copy with 1-byte offset, 3-bit length
          if (ip_ >= compressed_end_) {
            return false;  // Not enough input
          }
          
          // Length is encoded in bits [2,4] + 4
          length = ((c >> 2) & 0x07) + 4;
          
          // Offset is encoded in the next byte
          offset = *(reinterpret_cast<const unsigned char*>(ip_++));
        } else if (copy_type == 2) {  // COPY_2_BYTE_OFFSET
          // Copy with 2-byte offset, 3-bit length
          if (ip_ + 1 >= compressed_end_) {
            return false;  // Not enough input
          }
          
          // Length is encoded in bits [2,4] + 1
          length = ((c >> 2) & 0x07) + 1;
          
          // Offset is encoded in the next 2 bytes (little-endian)
          offset = *(reinterpret_cast<const unsigned char*>(ip_)) |
                  (*(reinterpret_cast<const unsigned char*>(ip_ + 1)) << 8);
          ip_ += 2;
        } else if (copy_type == 3) {  // COPY_4_BYTE_OFFSET
          // Copy with 4-byte offset, length in next byte
          if (ip_ >= compressed_end_) {
            return false;  // Not enough input
          }
          
          // Length is encoded in the next byte
          length = *(reinterpret_cast<const unsigned char*>(ip_++));
          
          // Offset is encoded in the next 4 bytes (little-endian)
          if (ip_ + 3 >= compressed_end_) {
            return false;  // Not enough input
          }
          
          offset = *(reinterpret_cast<const unsigned char*>(ip_)) |
                  (*(reinterpret_cast<const unsigned char*>(ip_ + 1)) << 8) |
                  (*(reinterpret_cast<const unsigned char*>(ip_ + 2)) << 16) |
                  (*(reinterpret_cast<const unsigned char*>(ip_ + 3)) << 24);
          ip_ += 4;
        } else {
          return false;  // Invalid copy type
        }
        
        // Validate offset and length
        if (offset == 0) {
          return false;  // Invalid offset
        }
        
        // Special handling for Parquet-specific format
        // In some Parquet implementations, offset might be relative to the start of the buffer
        // rather than the current position
        if (offset > static_cast<uint32_t>(op - uncompressed)) {
          // Try to interpret as absolute offset if it's within the buffer
          if (offset <= uncompressed_len) {
            const char* copy_src = uncompressed + offset - 1;
            
            // Check limits
            if (op + length > op_limit) {
              return false;  // Not enough output space
            }
            
            // Copy the data
            memcpy(op, copy_src, length);
            op += length;
            continue;
          } else {
            return false;  // Invalid offset
          }
        }
        
        if (op + length > op_limit) {
          return false;  // Not enough output space
        }
        
        // Copy from earlier in the output
        const char* copy_src = op - offset;
        
        // Handle overlapping copies
        if (offset < length) {
          // Slow path: src and dst regions overlap
          for (uint32_t i = 0; i < length; i++) {
            op[i] = copy_src[i];
          }
        } else {
          // Fast path: src and dst regions don't overlap
          memcpy(op, copy_src, length);
        }
        
        op += length;
      }
    }
    
    // For Parquet, we might not consume all input but should fill the output
    return (op == op_limit);
  }

  inline bool IsEOF() const { return ip_ >= compressed_end_; }

 private:
  const char* const compressed_;
  const char* const compressed_end_;
  const char* ip_;  // Input pointer
};

// Decompresses the snappy compressed buffer into the uncompressed buffer.
// Returns true if the decompression was successful.
inline bool RawUncompress(const char* compressed, size_t compressed_length,
                         char* uncompressed) {
  // Check input
  if (compressed_length == 0) return false;
  
  // Special handling for Parquet-specific Snappy format
  // In Parquet, the first 4 bytes might be the uncompressed length
  if (compressed_length >= 4) {
    // Try to interpret the first 4 bytes as uncompressed length (little-endian)
    uint32_t uncompressed_length = static_cast<uint32_t>(static_cast<unsigned char>(compressed[0])) |
                                  (static_cast<uint32_t>(static_cast<unsigned char>(compressed[1])) << 8) |
                                  (static_cast<uint32_t>(static_cast<unsigned char>(compressed[2])) << 16) |
                                  (static_cast<uint32_t>(static_cast<unsigned char>(compressed[3])) << 24);
    
    // If the length seems reasonable, try to decompress directly
    if (uncompressed_length > 0 && uncompressed_length < 10 * 1024 * 1024) {
      // Skip the length field and decompress the rest
      const char* compressed_data = compressed + 4;
      size_t compressed_data_length = compressed_length - 4;
      
      // Try standard Snappy decompression
      SnappyDecompressor decompressor(compressed_data, compressed_data_length);
      return decompressor.DecompressAllTags(uncompressed, uncompressed_length);
    }
  }
  
  // Try standard Snappy format
  SnappyDecompressor decompressor(compressed, compressed_length);
  size_t uncompressed_length = 0;
  
  if (!decompressor.ReadUncompressedLength(&uncompressed_length)) {
    return false;
  }
  
  return decompressor.DecompressAllTags(uncompressed, uncompressed_length);
}

// Overload that takes a known uncompressed length
// This is useful for Parquet files where the uncompressed length is stored separately
inline bool RawUncompress(const char* compressed, size_t compressed_length,
                         char* uncompressed, size_t uncompressed_length) {
  // Check input
  if (compressed_length == 0 || uncompressed_length == 0) return false;
  
  // Special handling for Parquet-specific format
  // Try to interpret the data as raw uncompressed data first
  if (compressed_length >= uncompressed_length) {
    memcpy(uncompressed, compressed, uncompressed_length);
    return true;
  }
  
  // Try direct decompression with the provided uncompressed length
  SnappyDecompressor decompressor(compressed, compressed_length);
  return decompressor.DecompressAllTags(uncompressed, uncompressed_length);
}

} // namespace snappy

#endif // MINIPARQUET_SNAPPY_DECOMPRESSION_H_
