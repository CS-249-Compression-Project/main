#ifndef _rt_h_
#define _rt_h_

#include <string>
#include <vector>
#include <cstdint>
#include <fstream>
#include <iostream>

using namespace std;

// Class representing a relational table
class ColumnarRelationalTable
{
public:
    ColumnarRelationalTable();

    ColumnarRelationalTable(const std::string &file_name);

    // Create a new table with the given column metadata
    ColumnarRelationalTable(const std::string &file_name, const uint32_t num_columns);

    // Print the table data
    // void printTable() const;

    // Add a new row to the table
    // void addRow_uint32_t(const std::vector<uint32_t> &row_data);
    // void addRow_float(const std::vector<float> &row_data);

    // Retrieve a specific row by index
    // std::vector<uint32_t> getRow_uint32_t(uint32_t row_index) const;
    // std::vector<float> getRow_float(uint32_t row_index) const;

    // Perform a join operation with another table and make the new file
    // ColumnarRelationalTable full_outer_join(const ColumnarRelationalTable &other, const std::string &new_table_file_name) const;
    // ColumnarRelationalTable inner_join(const ColumnarRelationalTable &other, const std::string &new_table_file_name, const std::vector<uint32_t> col1, const std::vector<uint32_t> col2) const;

    // Compress the table data
    // void compressData();

    // Decompress the table data
    // void decompressData();

    // Getters
    // uint32_t readNumEntries() const;
    // uint32_t readNumColumns() const;

protected:
    // std::string file_name_;                   // File path for the table
    // uint32_t num_entries_;                    // Number of rows
    // uint32_t num_columns_;                    // Number of columns
    // std::vector<std::vector<uint32_t>> data_; // Entry data

    // // Parse metadata and fill num_entries, num_columns
    // bool parseMetadata();

    // // Calculate row size from metadata in bytes
    // uint32_t calculateRowSize() const;

    // // Setters
    // bool writeMetadata(uint32_t num_entries, uint32_t num_columns);
    // bool writeNumEntries(uint32_t num_entries);
    // bool writeNumColumns(uint32_t num_columns);
};

#endif
