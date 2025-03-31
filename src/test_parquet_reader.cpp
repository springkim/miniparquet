#include <iostream>
#include <string>
#include <fstream>
#include <vector>
#include "parquet_reader.h"

bool test_snappy_decompression(const std::string& filename) {
    std::ifstream file(filename, std::ios::binary);
    if (!file) {
        std::cerr << "Cannot open file for Snappy test: " << filename << std::endl;
        return false;
    }
    
    char magic[4];
    file.read(magic, 4);
    if (std::strncmp(magic, "PAR1", 4) != 0) {
        std::cerr << "Not a valid Parquet file (missing magic bytes)" << std::endl;
        return false;
    }
    
    std::vector<char> compressed_data(8192);
    file.read(compressed_data.data(), compressed_data.size());
    size_t bytes_read = file.gcount();
    
    std::cout << "Read " << bytes_read << " bytes for Snappy test" << std::endl;
    
    bool found_snappy = false;
    size_t offset = 0;
    
    for (size_t i = 0; i < bytes_read - 10; i++) {
        if ((unsigned char)compressed_data[i] == 0x73 && 
            (unsigned char)compressed_data[i+1] == 0x4E && 
            (unsigned char)compressed_data[i+2] == 0x61 && 
            (unsigned char)compressed_data[i+3] == 0x50) {
            offset = i;
            found_snappy = true;
            std::cout << "Found Snappy magic bytes at offset " << i << std::endl;
            break;
        }
        
        if ((compressed_data[i] & 0x03) == 0x00) {  // LITERAL tag
            uint8_t tag = (unsigned char)compressed_data[i];
            uint32_t literal_tag = tag >> 2;
            
            if (literal_tag < 60 && i + literal_tag + 1 < bytes_read) {
                offset = i;
                found_snappy = true;
                std::cout << "Found potential Snappy LITERAL tag at offset " << i << std::endl;
                break;
            }
        }
    }
    
    if (!found_snappy) {
        std::cerr << "Could not find Snappy data in file" << std::endl;
        
        std::cout << "First 16 bytes: ";
        for (int i = 0; i < 16 && i < bytes_read; i++) {
            printf("%02x ", (unsigned char)compressed_data[i]);
        }
        std::cout << std::endl;
        
        return false;
    }
    
    std::cout << "Testing Snappy decompression from offset " << offset << std::endl;
    
    const char* snappy_data = compressed_data.data() + offset;
    size_t snappy_length = bytes_read - offset;
    
    for (size_t test_size : {1024, 2048, 4096, 8192}) {
        std::cout << "Trying with uncompressed size: " << test_size << std::endl;
        
        std::vector<char> uncompressed_data(test_size);
        
        bool decompress_success = miniparquet::internal::RawUncompress(
            snappy_data, snappy_length, uncompressed_data.data(), test_size);
        
        if (decompress_success) {
            std::cout << "Successfully decompressed with size " << test_size << std::endl;
            
            std::cout << "First 16 bytes of decompressed data: ";
            for (int i = 0; i < 16 && i < test_size; i++) {
                printf("%02x ", (unsigned char)uncompressed_data[i]);
            }
            std::cout << std::endl;
            
            return true;
        }
    }
    
    size_t uncompressed_length = 0;
    bool length_success = miniparquet::internal::GetUncompressedLength(
        snappy_data, snappy_length, &uncompressed_length);
    
    if (!length_success) {
        std::cerr << "Failed to get uncompressed length" << std::endl;
        
        std::cout << "First 16 bytes of compressed data: ";
        for (int i = 0; i < 16 && i < snappy_length; i++) {
            printf("%02x ", (unsigned char)snappy_data[i]);
        }
        std::cout << std::endl;
        
        return false;
    }
    
    std::cout << "Detected uncompressed length: " << uncompressed_length << std::endl;
    
    std::vector<char> uncompressed_data(uncompressed_length);
    bool decompress_success = miniparquet::internal::RawUncompress(
        snappy_data, snappy_length, uncompressed_data.data());
    
    if (!decompress_success) {
        std::cerr << "Failed to decompress data with detected length" << std::endl;
        return false;
    }
    
    std::cout << "Successfully decompressed " << uncompressed_length << " bytes" << std::endl;
    
    std::cout << "First 16 bytes of decompressed data: ";
    for (int i = 0; i < 16 && i < uncompressed_length; i++) {
        printf("%02x ", (unsigned char)uncompressed_data[i]);
    }
    std::cout << std::endl;
    
    return true;
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <parquet_file>" << std::endl;
        return 1;
    }

    try {
        std::string filename = argv[1];
        std::cout << "Reading Parquet file: " << filename << std::endl;
        
        std::cout << "Testing Snappy decompression..." << std::endl;
        if (!test_snappy_decompression(filename)) {
            std::cerr << "Snappy decompression test failed" << std::endl;
        }
        
        miniparquet::ParquetData data = miniparquet::read_parquet(filename);
        
        std::cout << "Successfully read Parquet file!" << std::endl;
        std::cout << "Number of rows: " << data.num_rows << std::endl;
        std::cout << "Root schema name: " << data.root.name << std::endl;
        std::cout << "Number of columns: " << data.root.children.size() << std::endl;
        
        std::cout << "Columns:" << std::endl;
        for (const auto& col : data.root.children) {
            std::cout << "  - " << col.name << " (";
            if (col.is_group) {
                std::cout << "GROUP, children: " << col.children.size();
            } else {
                std::cout << "PRIMITIVE";
            }
            std::cout << ")" << std::endl;
        }
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}
