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
