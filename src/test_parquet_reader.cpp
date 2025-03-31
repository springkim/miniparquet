#include <iostream>
#include <string>
#include "parquet_reader.h"

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <parquet_file>" << std::endl;
        return 1;
    }

    try {
        std::string filename = argv[1];
        std::cout << "Reading Parquet file: " << filename << std::endl;
        
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
