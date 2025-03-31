#ifndef MINIPARQUET_NESTED_HPP
#define MINIPARQUET_NESTED_HPP

#include <cstdint>
#include <cstddef>
#include <string>
#include <vector>
#include <fstream>
#include <stdexcept>
#include <cstring>
#include <memory>
#include "snappy/snappy.h" // Include the actual Snappy library

namespace miniparquet {

  enum class ParquetType {
    BOOLEAN, INT32, INT64, INT96, FLOAT, DOUBLE, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY
  };

  struct Int96 { uint32_t value[3]; };

  enum class RepetitionType { REQUIRED, OPTIONAL, REPEATED };

  // flat 스키마 정보를 저장 (Thrift FileMetaData에서 읽은 순서대로)
  struct SchemaElement {
    std::string name;
    bool is_primitive;
    ParquetType type; // primitive일 때만 유효
    int32_t type_length;
    uint32_t num_children; // 자식 개수 (0이면 leaf)
    RepetitionType repetition;
  };

  // 최종 스키마 트리 (group이면 children 채움)
  struct ParquetFieldData {
    std::string name;
    bool is_group;
    ParquetType type; // leaf일 때만 유효
    int32_t type_length;
    int max_def_level; // 이후 데이터 페이지 처리를 위해
    int max_rep_level;
    std::vector<ParquetFieldData> children;
    // 실제 값은 leaf에 대해 별도 채움 (여기서는 스키마 구성에 집중)
  };

  // 전체 파일 데이터 (여기서는 스키마와 row 수만)
  struct ParquetData {
    ParquetFieldData root;
    uint64_t num_rows;
  };

  namespace internal {
    int64_t readVarInt64(const uint8_t*& p, const uint8_t* end);
    void skipValue(uint8_t type, const uint8_t*& p, const uint8_t* end);
    
    // Use Snappy library functions instead of custom implementations
    inline bool GetUncompressedLength(const char* ptr, size_t n, size_t* result) {
      return snappy::GetUncompressedLength(ptr, n, result);
    }
    
    inline bool RawUncompress(const char* compressed, size_t n, char* uncompressed) {
      return snappy::RawUncompress(compressed, n, uncompressed);
    }
    
    // flat 스키마 리스트를 재귀적으로 트리로 구성 (정확한 자식 수를 이용)
    size_t build_schema_tree(const std::vector<SchemaElement>& flat, size_t index, uint32_t num_children, ParquetFieldData& parent);
  }

  // read_parquet는 메타데이터에서 스키마와 row 수를 읽어 스키마 트리를 구성합니다.
  inline ParquetData read_parquet(const std::string& filename) {
    std::ifstream file(filename, std::ios::binary);
    if (!file)
      throw std::runtime_error("Cannot open file: " + filename);

    char magic[4];
    file.read(magic, 4);
    if (std::strncmp(magic, "PAR1", 4) != 0)
      throw std::runtime_error("Not a valid Parquet file (missing magic bytes)");
    file.seekg(-4, std::ios::end);
    file.read(magic, 4);
    if (std::strncmp(magic, "PAR1", 4) != 0)
      throw std::runtime_error("Not a valid Parquet file (no end magic)");
    file.seekg(-8, std::ios::end);
    uint32_t footer_len = 0;
    file.read(reinterpret_cast<char*>(&footer_len), 4);
    if (footer_len == 0)
      throw std::runtime_error("Invalid Parquet footer length");
    std::vector<uint8_t> footer_buffer(footer_len);
    file.seekg(-(8 + static_cast<std::streamoff>(footer_len)), std::ios::end);
    file.read(reinterpret_cast<char*>(footer_buffer.data()), footer_len);
    const uint8_t* buf = footer_buffer.data();
    const uint8_t* buf_end = buf + footer_len;

    uint64_t num_rows_total = 0;
    std::vector<SchemaElement> schema_flat;
    int16_t lastFieldId = 0;
    while (buf < buf_end) {
      if (*buf == 0) { ++buf; break; }
      uint8_t hdr = *buf++;
      int fieldDelta = hdr >> 4;
      uint8_t typeNibble = hdr & 0x0F;
      int16_t fieldId = (fieldDelta == 0)
                          ? static_cast<int16_t>(internal::readVarInt64(buf, buf_end))
                          : lastFieldId + fieldDelta;
      lastFieldId = fieldId;
      switch (fieldId) {
        case 1:
          internal::skipValue(typeNibble, buf, buf_end);
          break;
        case 2: { // schema (list<SchemaElement>)
          if (typeNibble != 0x09)
            throw std::runtime_error("Unexpected schema field type");
          if (buf >= buf_end) throw std::runtime_error("Truncated schema list");
          uint8_t listDescriptor = *buf++;
          uint32_t schema_count;
          uint32_t count_prefix = listDescriptor >> 4;
          schema_count = (count_prefix < 15) ? count_prefix : static_cast<uint32_t>(internal::readVarInt64(buf, buf_end) + 15);
          schema_flat.reserve(schema_count);
          for (uint32_t i = 0; i < schema_count; ++i) {
            SchemaElement se;
            int16_t schemaLastId = 0;
            se.is_primitive = false;
            se.type_length = 0;
            se.num_children = 0;
            se.repetition = RepetitionType::REQUIRED;
            bool has_type = false;
            uint32_t num_children = 0;
            while (buf < buf_end) {
              if (*buf == 0) { ++buf; break; }
              uint8_t sfhdr = *buf++;
              int delta = sfhdr >> 4;
              uint8_t sf_type = sfhdr & 0x0F;
              int16_t sfid = (delta == 0)
                               ? static_cast<int16_t>(internal::readVarInt64(buf, buf_end))
                               : schemaLastId + delta;
              schemaLastId = sfid;
              switch (sfid) {
                case 1: {
                  int64_t tval = internal::readVarInt64(buf, buf_end);
                  int32_t t = static_cast<int32_t>(tval);
                  switch (t) {
                    case 0: se.type = ParquetType::BOOLEAN; break;
                    case 1: se.type = ParquetType::INT32; break;
                    case 2: se.type = ParquetType::INT64; break;
                    case 3: se.type = ParquetType::INT96; break;
                    case 4: se.type = ParquetType::FLOAT; break;
                    case 5: se.type = ParquetType::DOUBLE; break;
                    case 6: se.type = ParquetType::BYTE_ARRAY; break;
                    case 7: se.type = ParquetType::FIXED_LEN_BYTE_ARRAY; break;
                    default: throw std::runtime_error("Unsupported type in schema");
                  }
                  has_type = true;
                  break;
                }
                case 2: {
                  int64_t tl = internal::readVarInt64(buf, buf_end);
                  se.type_length = static_cast<int32_t>(tl);
                  break;
                }
                case 3: {
                  int64_t rep = internal::readVarInt64(buf, buf_end);
                  switch (static_cast<int>(rep)) {
                    case 0: se.repetition = RepetitionType::REQUIRED; break;
                    case 1: se.repetition = RepetitionType::OPTIONAL; break;
                    case 2: se.repetition = RepetitionType::REPEATED; break;
                    default: se.repetition = RepetitionType::REQUIRED; break;
                  }
                  break;
                }
                case 4: {
                  if (sf_type != 0x08)
                    throw std::runtime_error("Invalid schema name type");
                  uint64_t name_len = 0;
                  int shift = 0;
                  while (true) {
                    if (buf >= buf_end)
                      throw std::runtime_error("Truncated schema name");
                    uint8_t c = *buf++;
                    name_len |= uint64_t(c & 0x7F) << shift;
                    if (!(c & 0x80)) break;
                    shift += 7;
                  }
                  if (buf + name_len > buf_end)
                    throw std::runtime_error("Truncated schema name");
                  se.name.assign(reinterpret_cast<const char*>(buf), name_len);
                  buf += name_len;
                  break;
                }
                case 5: {
                  int64_t nc = internal::readVarInt64(buf, buf_end);
                  num_children = static_cast<uint32_t>(nc);
                  break;
                }
                default:
                  internal::skipValue(sf_type, buf, buf_end);
                  break;
              }
            }
            se.num_children = num_children;
            se.is_primitive = (num_children == 0);
            schema_flat.push_back(se);
          }
          break;
        }
        case 3: {
          int64_t totalRows = internal::readVarInt64(buf, buf_end);
          num_rows_total = static_cast<uint64_t>(totalRows);
          break;
        }
        case 4:
          internal::skipValue(typeNibble, buf, buf_end);
          break;
        default:
          internal::skipValue(typeNibble, buf, buf_end);
          break;
      }
    }

    if (schema_flat.empty())
      throw std::runtime_error("Empty schema");

    // flat 스키마 리스트를 트리로 구성 (첫번째 요소는 루트)
    ParquetFieldData root_field;
    root_field.name = schema_flat[0].name;
    root_field.is_group = true;
    root_field.max_def_level = 0;
    root_field.max_rep_level = 0;
    internal::build_schema_tree(schema_flat, 1, schema_flat[0].num_children, root_field);

    ParquetData result;
    result.root = std::move(root_field);
    result.num_rows = num_rows_total;
    return result;
  }

  namespace internal {

    inline int64_t readVarInt64(const uint8_t*& p, const uint8_t* end) {
      uint64_t result = 0;
      int shift = 0;
      while (p < end) {
        uint8_t byte = *p++;
        result |= uint64_t(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) {
          uint64_t temp = result;
          int64_t decoded = static_cast<int64_t>((temp >> 1) ^ (~(temp & 1) + 1));
          return decoded;
        }
        shift += 7;
        if (shift >= 64)
          throw std::runtime_error("Varint too long");
      }
      throw std::runtime_error("Buffer underflow while reading varint");
    }

    inline void skipValue(uint8_t type, const uint8_t*& p, const uint8_t* end) {
      switch (type) {
        case 0x00: return;
        case 0x01:
        case 0x02: return;
        case 0x03:
          if (p >= end) throw std::runtime_error("Buffer underflow");
          ++p;
          return;
        case 0x04:
        case 0x05:
        case 0x06:
          (void)readVarInt64(p, end);
          return;
        case 0x07:
          if (p + 8 > end) throw std::runtime_error("Buffer underflow");
          p += 8;
          return;
        case 0x08: {
          uint64_t len = 0;
          int shift = 0;
          while (p < end) {
            uint8_t byte = *p++;
            len |= uint64_t(byte & 0x7F) << shift;
            if (!(byte & 0x80)) break;
            shift += 7;
            if (shift >= 64) throw std::runtime_error("Varint too long");
          }
          if (p + len > end) throw std::runtime_error("Buffer underflow");
          p += len;
          return;
        }
        case 0x09: {
          if (p >= end) throw std::runtime_error("Buffer underflow");
          uint8_t listDesc = *p++;
          uint8_t elemType = listDesc & 0x0F;
          uint32_t count;
          uint32_t count_prefix = listDesc >> 4;
          count = (count_prefix < 15) ? count_prefix : static_cast<uint32_t>(readVarInt64(p, end) + 15);
          for (uint32_t i = 0; i < count; ++i)
            skipValue(elemType, p, end);
          return;
        }
        case 0x0A: {
          if (p >= end) throw std::runtime_error("Buffer underflow");
          uint8_t setDesc = *p++;
          uint8_t elemType = setDesc & 0x0F;
          uint32_t count;
          uint32_t cp = setDesc >> 4;
          count = (cp < 15) ? cp : static_cast<uint32_t>(readVarInt64(p, end) + 15);
          for (uint32_t i = 0; i < count; ++i)
            skipValue(elemType, p, end);
          return;
        }
        case 0x0B: {
          uint64_t map_count = readVarInt64(p, end);
          if (map_count == 0) return;
          if (p >= end) throw std::runtime_error("Buffer underflow");
          uint8_t typeByte = *p++;
          uint8_t keyType = typeByte >> 4;
          uint8_t valType = typeByte & 0x0F;
          for (uint64_t mi = 0; mi < map_count; ++mi) {
            skipValue(keyType, p, end);
            skipValue(valType, p, end);
          }
          return;
        }
        case 0x0C: {
          int16_t lastId = 0;
          while (p < end && *p != 0) {
            uint8_t hdr = *p++;
            int delta = hdr >> 4;
            uint8_t ftype = hdr & 0x0F;
            int16_t fid = (delta == 0)
                            ? static_cast<int16_t>(readVarInt64(p, end))
                            : lastId + delta;
            lastId = fid;
            skipValue(ftype, p, end);
          }
          if (p < end && *p == 0) ++p;
          return;
        }
        default:
          throw std::runtime_error("Unsupported type in skipValue");
      }
    }

    // flat 스키마 리스트(flat)에서 현재 요소부터 num_children 만큼을 재귀적으로 읽어 parent.children에 추가.
    inline size_t build_schema_tree(const std::vector<SchemaElement>& flat, size_t index, uint32_t num_children, ParquetFieldData& parent) {
      for (uint32_t i = 0; i < num_children; i++) {
        if (index >= flat.size())
          throw std::runtime_error("Insufficient schema elements");
        const SchemaElement& se = flat[index++];
        ParquetFieldData child;
        child.name = se.name;
        child.is_group = (se.num_children > 0);
        child.type = se.type;
        child.type_length = se.type_length;
        // (max_def_level, max_rep_level은 실제 데이터 페이지 재구성 시 결정)
        child.max_def_level = 0;
        child.max_rep_level = 0;
        if (se.num_children > 0) {
          index = build_schema_tree(flat, index, se.num_children, child);
        }
        parent.children.push_back(child);
      }
      return index;
    }
  } // namespace internal

} // namespace miniparquet

#endif // MINIPARQUET_NESTED_HPP
