#ifndef _rt_h_
#define _rt_h_

#include <string>
#include <vector> 
#include <cstdint>
#include <fstream>
#include <iostream>

using namespace std;

// Class representing a relational table
class RelationalTable {
public:
    RelationalTable(const std::string& file_name);
    
    // Create a new table with the given column metadata
    RelationalTable(const std::string& file_name, const uint32_t num_columns);

    // Add a new row to the table
    void addRow(const std::vector<uint32_t>& row_data);

    // Retrieve a specific row by index
    std::vector<uint32_t> getRow(uint32_t row_index) const;

    // Perform a join operation with another table
    RelationalTable join(const RelationalTable& other) const;

    // Compress the table data
    void compressData();

    // Decompress the table data
    void decompressData();

private:
    std::string file_name_;                   // File path for the table
    uint32_t num_entries_;                    // Number of rows
    uint32_t num_columns_;                    // Number of columns
    std::vector<std::vector<uint32_t>> data_; // Entry data

    // Parse metadata and fill num_entries, num_columns
    bool parseMetadata();
    
    // Calculate row size from metadata in bytes
    uint32_t calculateRowSize() const;
};

#endif _rt_h_