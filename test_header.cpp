#include "miniparquet.hpp"
#include <iostream>

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <parquet_file>" << std::endl;
        return 1;
    }
    
    try {
        miniparquet::ParquetFile file(argv[1]);
        std::cout << "Successfully opened parquet file with " 
                  << file.nrow << " rows" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}
