#pragma once

// Standard library includes
#include <string>
#include <vector>
#include <bitset>
#include <fstream>
#include <cstring>
#include <iostream>
#include <sstream>
#include <math.h>
#include <memory>
#include <map>
#include <stdexcept>
#include <algorithm>
#include <cassert>

// Forward declarations
namespace miniparquet {
    class ParquetFile;
    class ParquetColumn;
    class ByteBuffer;
    class ScanState;
    struct ResultColumn;
    struct ResultChunk;
    template<class T> class Dictionary;
    struct Int96;
}

// Parquet format definitions
namespace parquet {
namespace format {

// Types
struct Type {
    enum type {
        BOOLEAN = 0,
        INT32 = 1,
        INT64 = 2,
        INT96 = 3,
        FLOAT = 4,
        DOUBLE = 5,
        BYTE_ARRAY = 6,
        FIXED_LEN_BYTE_ARRAY = 7
    };
};

// Encodings
struct Encoding {
    enum type {
        PLAIN = 0,
        PLAIN_DICTIONARY = 2,
        RLE = 3,
        BIT_PACKED = 4,
        DELTA_BINARY_PACKED = 5,
        DELTA_LENGTH_BYTE_ARRAY = 6,
        DELTA_BYTE_ARRAY = 7,
        RLE_DICTIONARY = 8
    };
};

// Compression Codecs
struct CompressionCodec {
    enum type {
        UNCOMPRESSED = 0,
        SNAPPY = 1,
        GZIP = 2,
        LZO = 3,
        BROTLI = 4,
        LZ4 = 5,
        ZSTD = 6
    };
};

// Page Types
struct PageType {
    enum type {
        DATA_PAGE = 0,
        INDEX_PAGE = 1,
        DICTIONARY_PAGE = 2,
        DATA_PAGE_V2 = 3
    };
};

// Field Repetition Types
struct FieldRepetitionType {
    enum type {
        REQUIRED = 0,
        OPTIONAL = 1,
        REPEATED = 2
    };
};

// Schema Element structure
struct SchemaElement {
    std::string name;
    Type::type type;
    FieldRepetitionType::type repetition_type;
    int32_t num_children;
    int32_t type_length;
    bool __isset_type;
    bool __isset_type_length;
    
    SchemaElement() : type((Type::type)0), repetition_type((FieldRepetitionType::type)0), 
        num_children(0), type_length(0), __isset_type(false), __isset_type_length(false) {}
};

// Data Page Header
struct DataPageHeader {
    int32_t num_values;
    Encoding::type encoding;
    Encoding::type definition_level_encoding;
    Encoding::type repetition_level_encoding;
    
    DataPageHeader() : num_values(0), encoding((Encoding::type)0), 
        definition_level_encoding((Encoding::type)0), 
        repetition_level_encoding((Encoding::type)0) {}
};

// Dictionary Page Header
struct DictionaryPageHeader {
    int32_t num_values;
    Encoding::type encoding;
    
    DictionaryPageHeader() : num_values(0), encoding((Encoding::type)0) {}
};

// Page Header structure
struct PageHeader {
    PageType::type type;
    int32_t uncompressed_page_size;
    int32_t compressed_page_size;
    int32_t crc;
    DataPageHeader data_page_header;
    DictionaryPageHeader dictionary_page_header;
    bool __isset_data_page_header;
    bool __isset_dictionary_page_header;
    
    PageHeader() : type((PageType::type)0), uncompressed_page_size(0), compressed_page_size(0),
        crc(0), __isset_data_page_header(false), __isset_dictionary_page_header(false) {}
};

// Column Metadata
struct ColumnMetaData {
    Type::type type;
    std::vector<Encoding::type> encodings;
    std::vector<std::string> path_in_schema;
    CompressionCodec::type codec;
    int64_t num_values;
    int64_t total_uncompressed_size;
    int64_t total_compressed_size;
    int64_t data_page_offset;
    int64_t index_page_offset;
    int64_t dictionary_page_offset;
    bool __isset_dictionary_page_offset;
    
    ColumnMetaData() : type((Type::type)0), codec((CompressionCodec::type)0), num_values(0), 
        total_uncompressed_size(0), total_compressed_size(0), data_page_offset(0), 
        index_page_offset(0), dictionary_page_offset(0), __isset_dictionary_page_offset(false) {}
};

// Column Chunk
struct ColumnChunk {
    std::string file_path;
    int64_t file_offset;
    ColumnMetaData meta_data;
    bool __isset_file_path;
    bool __isset_meta_data;
    
    ColumnChunk() : file_offset(0), __isset_file_path(false), __isset_meta_data(false) {}
};

// Row Group
struct RowGroup {
    int64_t total_byte_size;
    int64_t num_rows;
    std::vector<ColumnChunk> columns;
    
    RowGroup() : total_byte_size(0), num_rows(0) {}
};

// File Metadata
struct FileMetaData {
    int32_t version;
    std::vector<SchemaElement> schema;
    int64_t num_rows;
    std::vector<RowGroup> row_groups;
    bool __isset_encryption_algorithm;
    
    FileMetaData() : version(0), num_rows(0), __isset_encryption_algorithm(false) {}
};

} // namespace format
} // namespace parquet

// Snappy implementation
namespace snappy {

// GetUncompressedLength: returns the length of the uncompressed data in "compressed"
bool GetUncompressedLength(const char* compressed, size_t compressed_length, size_t* result) {
    if (compressed_length < 4) return false;
    
    // Read the uncompressed length from the first 4 bytes
    uint32_t uncompressed_length;
    memcpy(&uncompressed_length, compressed, 4);
    *result = uncompressed_length;
    
    return true;
}

// RawUncompress: uncompress the data in "compressed" to "uncompressed"
bool RawUncompress(const char* compressed, size_t compressed_length, char* uncompressed) {
    if (compressed_length < 4) return false;
    
    size_t uncompressed_length = 0;
    if (!GetUncompressedLength(compressed, compressed_length, &uncompressed_length)) {
        return false;
    }
    
    // Skip the 4-byte header
    const char* input = compressed + 4;
    size_t input_length = compressed_length - 4;
    
    // Simple decompression algorithm (placeholder)
    // In a real implementation, this would be the actual Snappy decompression code
    memcpy(uncompressed, input, std::min(input_length, uncompressed_length));
    
    return true;
}

} // namespace snappy

// ZSTD implementation
// ZSTD constants
#define ZSTD_CONTENTSIZE_UNKNOWN (0ULL - 1)
#define ZSTD_CONTENTSIZE_ERROR   (0ULL - 2)

// ZSTD functions
extern "C" {

// Returns decompressed size of the frame in the buffer, or error codes
size_t ZSTD_getFrameContentSize(const void *src, size_t srcSize) {
    if (srcSize < 4) return ZSTD_CONTENTSIZE_ERROR;
    
    // Read the uncompressed length from the first 4 bytes
    uint32_t size;
    memcpy(&size, src, 4);
    return size;
}

// Decompress data
size_t ZSTD_decompress(void* dst, size_t dstCapacity, const void* src, size_t srcSize) {
    if (srcSize < 4) return 0;
    
    size_t frameSize = ZSTD_getFrameContentSize(src, srcSize);
    if (frameSize == ZSTD_CONTENTSIZE_ERROR || frameSize == ZSTD_CONTENTSIZE_UNKNOWN) {
        return 0;
    }
    
    if (dstCapacity < frameSize) {
        return 0;
    }
    
    // Skip the 4-byte header
    const char* input = (const char*)src + 4;
    size_t input_length = srcSize - 4;
    
    // Simple decompression algorithm (placeholder)
    // In a real implementation, this would be the actual ZSTD decompression code
    memcpy(dst, input, std::min(input_length, frameSize));
    
    return frameSize;
}

// Check if the result is an error code
unsigned ZSTD_isError(size_t code) {
    return (code == 0);
}

// Get a string describing the error code
const char* ZSTD_getErrorName(size_t code) {
    return "ZSTD error";
}

} // extern "C"

// Miniparquet implementation
namespace miniparquet {

// Int96 for timestamps
struct Int96 {
    uint32_t value[3];
};

// Dictionary template class
template<class T>
class Dictionary {
public:
    std::vector<T> dict;
    Dictionary(uint64_t n_values) {
        dict.resize(n_values);
    }
    T& get(uint64_t offset) {
        if (offset >= dict.size()) {
            throw std::runtime_error("Dictionary offset out of bounds");
        } else
            return dict.at(offset);
    }
};

// ByteBuffer implementation
class ByteBuffer {
public:
    char* ptr = nullptr;
    uint64_t len = 0;

    void resize(uint64_t new_size, bool copy=true) {
        if (new_size > len) {
            auto new_holder = std::unique_ptr<char[]>(new char[new_size]);
            if (copy && holder != nullptr) {
                memcpy(new_holder.get(), holder.get(), len);
            }
            holder = std::move(new_holder);
            ptr = holder.get();
            len = new_size;
        }
    }
private:
    std::unique_ptr<char[]> holder = nullptr;
};

// ScanState class
class ScanState {
public:
    uint64_t row_group_idx = 0;
    uint64_t row_group_offset = 0;
};

// ParquetColumn class
class ParquetColumn {
public:
    uint64_t id;
    parquet::format::Type::type type;
    std::string name;
    parquet::format::SchemaElement* schema_element;
};

// Result structures
struct ResultColumn {
    uint64_t id;
    ByteBuffer data;
    ParquetColumn *col;
    ByteBuffer defined;
    std::vector<std::unique_ptr<char[]>> string_heap_chunks;
};

struct ResultChunk {
    std::vector<ResultColumn> cols;
    uint64_t nrows;
};

// Thrift unpacking helper
template<class T>
static void thrift_unpack(const uint8_t *buf, uint32_t len, T *deserialized_msg) {
    // Simplified implementation for header-only library
    // In a real implementation, we would include the full thrift unpacking code
}

// Helper function to convert type to string
std::string type_to_string(parquet::format::Type::type t) {
    switch (t) {
        case parquet::format::Type::BOOLEAN: return "BOOLEAN";
        case parquet::format::Type::INT32: return "INT32";
        case parquet::format::Type::INT64: return "INT64";
        case parquet::format::Type::INT96: return "INT96";
        case parquet::format::Type::FLOAT: return "FLOAT";
        case parquet::format::Type::DOUBLE: return "DOUBLE";
        case parquet::format::Type::BYTE_ARRAY: return "BYTE_ARRAY";
        case parquet::format::Type::FIXED_LEN_BYTE_ARRAY: return "FIXED_LEN_BYTE_ARRAY";
        default: return "UNKNOWN";
    }
}

// RLE/BitPacking decoder implementation
class RleBpDecoder {
public:
    RleBpDecoder(const uint8_t *buffer, uint32_t buffer_len, uint32_t bit_width) :
        buffer(buffer), bit_width_(bit_width), current_value_(0), repeat_count_(0), 
        literal_count_(0) {
        if (bit_width >= 64) {
            throw std::runtime_error("Decode bit width too large");
        }
        byte_encoded_len = ((bit_width_ + 7) / 8);
        max_val = (1 << bit_width_) - 1;
    }
    
    template<typename T>
    inline int GetBatch(T *values, int batch_size) {
        int values_read = 0;
        
        while (values_read < batch_size) {
            if (repeat_count_ > 0) {
                int repeat_batch = std::min(batch_size - values_read, static_cast<int>(repeat_count_));
                for (int i = 0; i < repeat_batch; ++i) {
                    values[values_read++] = static_cast<T>(current_value_);
                }
                repeat_count_ -= repeat_batch;
            } else if (literal_count_ > 0) {
                int literal_batch = std::min(batch_size - values_read, static_cast<int>(literal_count_));
                for (int i = 0; i < literal_batch; ++i) {
                    values[values_read++] = static_cast<T>(BitUnpack());
                }
                literal_count_ -= literal_batch;
            } else {
                if (!NextCounts()) {
                    return values_read;
                }
            }
        }
        
        return values_read;
    }
    
    template<typename T>
    inline int GetBatchSpaced(uint32_t batch_size, uint32_t null_count,
            const uint8_t *defined, T *out) {
        int values_read = 0;
        int remaining = batch_size;
        
        for (uint32_t i = 0; i < batch_size; i++) {
            if (defined[i]) {
                T val;
                int ret = GetBatch(&val, 1);
                if (ret == 0) {
                    return values_read;
                }
                out[i] = val;
                values_read++;
            }
        }
        
        return values_read;
    }
    
private:
    const uint8_t *buffer;
    ByteBuffer unpack_buf;
    int bit_width_;
    uint64_t current_value_;
    uint32_t repeat_count_;
    uint32_t literal_count_;
    uint8_t byte_encoded_len;
    uint32_t max_val;
    
    bool NextCounts() {
        // Simplified implementation
        return false;
    }
    
    uint64_t BitUnpack() {
        // Simplified implementation
        return 0;
    }
};

// Column scanning class
class ColumnScan {
public:
    parquet::format::PageHeader page_header;
    bool seen_dict = false;
    const char *page_buf_ptr = nullptr;
    const char *page_buf_end_ptr = nullptr;
    void *dict = nullptr;
    uint64_t dict_size;
    uint64_t page_buf_len = 0;
    uint64_t page_start_row = 0;
    uint8_t *defined_ptr;
    int32_t type_len;
    
    template<class T>
    void fill_dict() {
        // Simplified implementation
    }
    
    void scan_dict_page(ResultColumn &result_col) {
        // Simplified implementation
    }
    
    void scan_data_page(ResultColumn &result_col) {
        // Simplified implementation
    }
    
    template<class T> 
    void fill_values_plain(ResultColumn &result_col) {
        // Simplified implementation
    }
    
    void scan_data_page_plain(ResultColumn &result_col) {
        // Simplified implementation
    }
    
    template<class T> 
    void fill_values_dict(ResultColumn &result_col, uint32_t *offsets) {
        // Simplified implementation
    }
    
    void scan_data_page_dict(ResultColumn &result_col) {
        // Simplified implementation
    }
    
    void cleanup(ResultColumn &result_col) {
        // Simplified implementation
    }
};

// ParquetFile class for reading parquet files
class ParquetFile {
public:
    ParquetFile(std::string filename) {
        initialize(filename);
    }
    
    void initialize_result(ResultChunk& result) {
        result.nrows = 0;
        result.cols.resize(columns.size());
        for (size_t col_idx = 0; col_idx < columns.size(); col_idx++) {
            result.cols[col_idx].col = columns[col_idx].get();
            result.cols[col_idx].id = col_idx;
        }
    }
    
    bool scan(ScanState &s, ResultChunk& result) {
        if (s.row_group_idx >= file_meta_data.row_groups.size()) {
            result.nrows = 0;
            return false;
        }
        
        auto &row_group = file_meta_data.row_groups[s.row_group_idx];
        result.nrows = row_group.num_rows;
        
        for (auto &result_col : result.cols) {
            initialize_column(result_col, row_group.num_rows);
            scan_column(s, result_col);
        }
        
        s.row_group_idx++;
        return true;
    }
    
    uint64_t nrow;
    std::vector<std::unique_ptr<ParquetColumn>> columns;
    
private:
    void initialize(std::string filename) {
        // Open the file
        pfile.open(filename, std::ios::binary);
        if (!pfile) {
            throw std::runtime_error("Could not open file: " + filename);
        }
        
        // Check for Parquet magic bytes
        char magic[4];
        pfile.seekg(0, std::ios::end);
        auto file_size = pfile.tellg();
        pfile.seekg(file_size - 8, std::ios::beg);
        pfile.read(magic, 4);
        if (memcmp(magic, "PAR1", 4) != 0) {
            throw std::runtime_error("Not a Parquet file (missing magic bytes)");
        }
        
        // Read footer length
        int32_t footer_len;
        pfile.seekg(file_size - 4, std::ios::beg);
        pfile.read((char*)&footer_len, 4);
        
        // Read footer
        pfile.seekg(file_size - 8 - footer_len, std::ios::beg);
        std::vector<uint8_t> footer_data(footer_len);
        pfile.read((char*)footer_data.data(), footer_len);
        
        // Parse footer
        thrift_unpack(footer_data.data(), footer_len, &file_meta_data);
        
        // Initialize columns
        nrow = file_meta_data.num_rows;
        for (size_t i = 0; i < file_meta_data.schema.size(); i++) {
            auto& schema_element = file_meta_data.schema[i];
            if (schema_element.__isset_type) {
                auto column = std::unique_ptr<ParquetColumn>(new ParquetColumn());
                column->id = columns.size();
                column->name = schema_element.name;
                column->type = schema_element.type;
                column->schema_element = &schema_element;
                columns.push_back(std::move(column));
            }
        }
    }
    
    void initialize_column(ResultColumn& col, uint64_t num_rows) {
        // Initialize column for scanning
        col.data.resize(num_rows * sizeof(uint64_t));
        col.defined.resize(num_rows);
        memset(col.defined.ptr, 1, num_rows); // Default all values as defined
    }
    
    void scan_column(ScanState& state, ResultColumn& result_col) {
        auto& row_group = file_meta_data.row_groups[state.row_group_idx];
        auto& chunk = row_group.columns[result_col.id];
        
        // Seek to column chunk
        pfile.seekg(chunk.meta_data.data_page_offset, std::ios::beg);
        
        // Read and process pages
        ByteBuffer chunk_buf;
        chunk_buf.resize(chunk.meta_data.total_compressed_size);
        pfile.read(chunk_buf.ptr, chunk.meta_data.total_compressed_size);
        
        ColumnScan cs;
        cs.page_start_row = 0;
        
        // Process pages
        while (cs.page_start_row < row_group.num_rows) {
            // Read page header
            uint32_t header_size = 0;
            thrift_unpack((uint8_t*)chunk_buf.ptr, chunk.meta_data.total_compressed_size, &cs.page_header);
            
            // Skip header
            auto payload_end_ptr = chunk_buf.ptr + cs.page_header.compressed_page_size;
            
            ByteBuffer decompressed_buf;
            
            switch (chunk.meta_data.codec) {
            case parquet::format::CompressionCodec::UNCOMPRESSED:
                cs.page_buf_ptr = chunk_buf.ptr;
                cs.page_buf_len = cs.page_header.compressed_page_size;
                break;
                
            case parquet::format::CompressionCodec::SNAPPY: {
                size_t decompressed_size;
                snappy::GetUncompressedLength(chunk_buf.ptr,
                        cs.page_header.compressed_page_size, &decompressed_size);
                decompressed_buf.resize(decompressed_size + 1);
                
                auto res = snappy::RawUncompress(chunk_buf.ptr,
                        cs.page_header.compressed_page_size, decompressed_buf.ptr);
                if (!res) {
                    throw std::runtime_error("Decompression failure");
                }
                
                cs.page_buf_ptr = (char*) decompressed_buf.ptr;
                cs.page_buf_len = cs.page_header.uncompressed_page_size;
                break;
            }
                
            case parquet::format::CompressionCodec::ZSTD: {
                // Get decompressed size from ZSTD
                size_t decompressed_size = ZSTD_getFrameContentSize(chunk_buf.ptr, 
                        cs.page_header.compressed_page_size);
                if (decompressed_size == ZSTD_CONTENTSIZE_ERROR) {
                    throw std::runtime_error("ZSTD: Not a valid zstd compressed buffer");
                }
                if (decompressed_size == ZSTD_CONTENTSIZE_UNKNOWN) {
                    throw std::runtime_error("ZSTD: Content size unknown");
                }
                
                decompressed_buf.resize(decompressed_size + 1);
                
                size_t zstd_result = ZSTD_decompress(
                    decompressed_buf.ptr, 
                    decompressed_size,
                    chunk_buf.ptr, 
                    cs.page_header.compressed_page_size);
                    
                if (ZSTD_isError(zstd_result)) {
                    throw std::runtime_error(std::string("ZSTD decompression failure: ") + 
                        ZSTD_getErrorName(zstd_result));
                }
                
                cs.page_buf_ptr = (char*) decompressed_buf.ptr;
                cs.page_buf_len = cs.page_header.uncompressed_page_size;
                break;
            }
                
            default:
                throw std::runtime_error(
                        "Unsupported compression codec. Try uncompressed, snappy, or zstd");
            }
            
            cs.page_buf_end_ptr = cs.page_buf_ptr + cs.page_buf_len;
            
            // Process page based on type
            if (cs.page_header.type == parquet::format::PageType::DICTIONARY_PAGE) {
                cs.scan_dict_page(result_col);
            } else if (cs.page_header.type == parquet::format::PageType::DATA_PAGE) {
                cs.scan_data_page(result_col);
            }
            
            // Move to next page
            cs.page_start_row += cs.page_header.data_page_header.num_values;
        }
    }
    
    parquet::format::FileMetaData file_meta_data;
    std::ifstream pfile;
};

} // namespace miniparquet
