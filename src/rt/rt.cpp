#include "rt.hpp"

RelationalTable::RelationalTable(const std::string& file_name) : file_name_(file_name) {
    // if (!parseMetadata()) {
    //     std::cerr << "Error: Unable to parse metadata for table " << file_name << std::endl;
    // }
}

#include <iostream>