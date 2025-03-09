#ifndef _rt_h_
#define _rt_h_

#include <string>
#include <vector>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <cstdint>

using namespace std;

// Class representing a relational table
class ColumnarRelationalTable
{
public:
    uint32_t num_entries_;                    // Number of rows
    uint32_t num_columns_;                    // Number of columns
    ColumnarRelationalTable();

    ColumnarRelationalTable(const std::string &file_name);

    // Create a new table with the given column metadata
    ColumnarRelationalTable(const std::string &file_name, const uint32_t num_columns);

    // Print the table data
    void printTable() const;

    // Add a new row to the table
    // void addRow_uint32_t(const std::vector<uint32_t> &row_data);
    // void addRow_float(const std::vector<float> &row_data);

    // Retrieve a specific row by index
    // std::vector<uint32_t> getRow_uint32_t(uint32_t row_index) const;
    // std::vector<float> getRow_float(uint32_t row_index) const;

    // Perform a join operation with another table and make the new file
    ColumnarRelationalTable full_outer_join(const ColumnarRelationalTable &other, const string &new_table_file_name);
    ColumnarRelationalTable inner_join(const ColumnarRelationalTable &other, const string &new_table_file_name, const std::vector<uint32_t> col1, const std::vector<uint32_t> col2);

    // Compress the table data
    // void compressData();

    // Decompress the table data
    // void decompressData();

    // Getters
    vector<vector<uint32_t>> ReadRowGroup_uint32(ifstream &file, const uint32_t num_columns) const;
    uint32_t readNumEntries() const;
    uint32_t readNumColumns() const;
    vector<vector<uint32_t>> readColumns(string filename, uint32_t columns) const;

    //Adds rows
    void writeRows(string filename, const vector<vector<uint32_t>> rows);

protected:
    string file_name_;                   // File path for the table
    vector<vector<uint32_t>> data_; // Entry data

    // // Parse metadata and fill num_entries, num_columns
    bool parseMetadata();

    // // Calculate row size from metadata in bytes
    // uint32_t calculateRowSize() const;

    // // Setters
    bool writeMetadata(uint32_t num_entries, uint32_t num_columns);
    void WriteRowGroupUncompressed_uint32(ofstream &file, const vector<vector<uint32_t>> rows);
    bool writeNumEntries(uint32_t num_entries);
    void setEntryCount(uint32_t new_count);
    // bool writeNumColumns(uint32_t num_columns);
};

#endif
