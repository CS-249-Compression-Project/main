#include "helper.hpp"
#include "columnar_rt.hpp"
#include <string>
#include <fstream>
#include <vector>
#include <cstdint>
using std::vector;

enum RepresentationKind : uint8_t
{
    Direct = 0,
};

ColumnarRelationalTable::ColumnarRelationalTable() {}

ColumnarRelationalTable::ColumnarRelationalTable(const std::string &file_name) : file_name_(file_name)
{
    if(!parseMetadata())
    {
        std::cerr << "Error: Unable to parse metadata for table " << file_name << std::endl;
    }
}

// Create a new table with the given column metadata
ColumnarRelationalTable::ColumnarRelationalTable(const std::string &file_name, const uint32_t num_columns) : file_name_(file_name)
{
    if(fileExists(file_name))
    {
        std::cerr << "Error: Table " << file_name << " already exists" << std::endl;
        return;
    }

    createFile(file_name);

    if(!writeMetadata(0, num_columns))
    {
        std::cerr << "Error: Unable to write metadata for table " << file_name << std::endl;
    }
    this->num_entries_ = 0;
    this->num_columns_ = num_columns;

    // We don't need to specify the type of our columns since we support only integers and floats, both 32 bits.
    // std::ofstream file(file_name, std::ios::binary | std::ios::in | std::ios::out);
    // if (!file.is_open())
    // {
    //     return;
    // }

    // uint32_t num_entries = 0;
    // file.write(reinterpret_cast<const char *>(&num_columns), sizeof(num_columns));
    // file.write(reinterpret_cast<const char *>(&num_entries), sizeof(num_entries));
    // file.close();
    // return true;
}

// The eventual generalization of this will be a WriteRowGroup_uint32 that takes another parameter specifying what
// encoding to use for each column.
void ColumnarRelationalTable::WriteRowGroupUncompressed_uint32(std::ofstream &file, const vector<vector<uint32_t>> rows)
{
    size_t num_entries = rows.size();
    size_t num_columns = rows.at(0).size();
    for (size_t i = 0; i < num_columns; i++)
    {
        const RepresentationKind columnRepresentation = RepresentationKind::Direct;
        // In the generalized version we will need to encode the column's values before we know
        // how many bytes it takes up - e.g. for run-length encoding.
        uint32_t bytes_used = num_entries * sizeof(uint32_t);
        file.write(reinterpret_cast<const char *>(&columnRepresentation), sizeof(columnRepresentation));
        file.write(reinterpret_cast<const char *>(&bytes_used), sizeof(bytes_used));
    }
    for (size_t column = 0; column < num_columns; column++)
    {
        vector<uint32_t> column_data(num_entries);
        for(size_t row = 0; row < num_entries; row++)
        {
            column_data[row] = rows[row][column];
        }
        file.write(reinterpret_cast<const char *>(column_data.data()), num_entries * sizeof(uint32_t));
        this->num_entries_++;
    }
    file.flush();
}

void ColumnarRelationalTable::writeRows(std::string filename, const vector<vector<uint32_t>> rows)
{
    std::ofstream file(filename, std::ios::binary | std::ios::app);
    if (!file.is_open())
    {
        std::cerr << "Error: Unable to open file " << filename << std::endl;
        return;
    }
    this->WriteRowGroupUncompressed_uint32(file, rows);
    file.close();
}

vector<vector<uint32_t>> ColumnarRelationalTable::readColumns(string filename, uint32_t columns) const {
    vector<vector<uint32_t>> result;
    std::ifstream file(filename, std::ios::binary | std::ios::app);
    if (!file.is_open())
    {
        std::cerr << "Error: Unable to open file " << filename << std::endl;
        return result;
    }
    result = this->ReadRowGroup_uint32(file, columns);
    file.close();
    return result;
}

vector<vector<uint32_t>> ColumnarRelationalTable::ReadRowGroup_uint32(std::ifstream &file, const uint32_t num_columns) const
{
    vector<RepresentationKind> columnRepresentations;
    vector<uint32_t> bytesUsed;
    file.seekg(8, std::ios::beg);
    for (uint32_t i = 0; i < num_columns; i++)
    {
        RepresentationKind rep;
        file.read(reinterpret_cast<char *>(&rep), sizeof(rep));
        columnRepresentations.push_back(rep);
        uint32_t byteCt;
        file.read(reinterpret_cast<char *>(&byteCt), sizeof(uint32_t));
        bytesUsed.push_back(byteCt);
    }

    vector<vector<uint32_t>> columnData(num_columns);
    for (uint32_t column = 0; column < num_columns; column++)
    {
        uint32_t num_elements = bytesUsed[column] / sizeof(uint32_t);
        if (bytesUsed[column] % sizeof(uint32_t) != 0)
        {
            throw std::runtime_error("Bad number of bytes for direct-represented uint32_ts");
        }
        columnData[column].resize(num_elements);
        file.read(reinterpret_cast<char*>(columnData[column].data()), num_elements * sizeof(uint32_t));
        if(file.eof()) break;
    }

    vector<vector<uint32_t>> rowData;
    size_t num_rows = columnData[0].size();
    for (size_t row = 0; row < num_rows; row++)
    {
        vector<uint32_t> entry;
        for (size_t column = 0; column < num_columns; column++)
        {
            if (columnData[column].size() != num_rows)
            {
                throw std::runtime_error("Columns have different row counts");
            }
            entry.push_back(columnData[column][row]);
        }
        rowData.push_back(std::move(entry));
    }
    return rowData;
}

ColumnarRelationalTable ColumnarRelationalTable::full_outer_join(const ColumnarRelationalTable &other, const std::string &new_table_file_name) 
{

    int new_entry_count = 0;

    // Read row information for both tables
    vector<vector<uint32_t>> left_rows = this->readColumns(file_name_, this->num_entries_);
    vector<vector<uint32_t>> right_rows = this->readColumns(other.file_name_, other.num_entries_);
    vector<vector<uint32_t>> new_rows;

    // Every entry, join left and right and add rows
    for (uint32_t entry_left = 0; entry_left < this->num_entries_; entry_left++)
    {
        for (uint32_t entry_right = 0; entry_right < other.num_entries_; entry_right++)
        {
            // Get the row data for the left table
            std::vector<uint32_t> row_data_left = left_rows[entry_left];

            // Get the row data for the right table
            std::vector<uint32_t> row_data_right = right_rows[entry_right];

            // Combine the row data
            std::vector<uint32_t> row_data_new = row_data_left;
            row_data_new.insert(row_data_new.end(), row_data_right.begin(), row_data_right.end());
            // Add the row to the new table
            new_rows.push_back(row_data_new);
            new_entry_count++;
        }
    }
    ColumnarRelationalTable toReturn = ColumnarRelationalTable(new_table_file_name, this->num_columns_);
    writeRows(new_table_file_name, new_rows);
    toReturn.setEntryCount(new_entry_count);
    return toReturn;
}

ColumnarRelationalTable ColumnarRelationalTable::inner_join(const ColumnarRelationalTable &other, const std::string &new_table_file_name, const std::vector<uint32_t> col1, const std::vector<uint32_t> col2)
{
    int new_entry_count = 0;

    // Read row information for both tables
    vector<vector<uint32_t>> left_rows = this->readColumns(file_name_, this->num_entries_);
    vector<vector<uint32_t>> right_rows = this->readColumns(other.file_name_, other.num_entries_);
    vector<vector<uint32_t>> new_rows;

    // Every entry, join left and right and add rows if the columns match
    for (uint32_t entry_left : col1)
    {
        for (uint32_t entry_right : col2)
        {
            // Get the row data for the left table
            std::vector<uint32_t> row_data_left = left_rows[entry_left];

            // Get the row data for the right table
            std::vector<uint32_t> row_data_right = right_rows[entry_right];

            // Combine the row data
            std::vector<uint32_t> row_data_new = row_data_left;
            row_data_new.insert(row_data_new.end(), row_data_right.begin(), row_data_right.end());

            // Add the row to the new table
            new_rows.push_back(row_data_new);
            new_entry_count++;
        }
    }

    std::ofstream new_table_file(new_table_file_name, std::ios::binary | std::ios::app);
    if (!new_table_file.is_open())
    {
        std::cerr << "Error: Unable to open file " << new_table_file_name << std::endl;
        return ColumnarRelationalTable();
    }
    ColumnarRelationalTable toReturn = ColumnarRelationalTable(new_table_file_name, this->num_columns_);
    writeRows(new_table_file_name, new_rows);
    toReturn.setEntryCount(new_entry_count);
    return toReturn;
}

bool ColumnarRelationalTable::writeMetadata(uint32_t num_entries, uint32_t num_columns)
{
    std::ofstream file(file_name_, std::ios::binary | std::ios::in | std::ios::out);
    if (!file.is_open())
    {
        return false;
    }

    file.write(reinterpret_cast<const char *>(&num_entries), sizeof(num_entries));
    file.write(reinterpret_cast<const char *>(&num_columns), sizeof(num_columns));
    file.close();
    return true;
}

bool ColumnarRelationalTable::parseMetadata()
{
    std::ifstream file(file_name_, std::ios::binary | std::ios::in);
    if (!file.is_open())
    {
        return false;
    }

    file.read(reinterpret_cast<char *>(&num_entries_), sizeof(num_entries_));
    file.read(reinterpret_cast<char *>(&num_columns_), sizeof(num_columns_));
    file.close();
    return true;
}

void ColumnarRelationalTable::printTable() const
{
    std::ifstream file(file_name_, std::ios::binary | std::ios::in);
    if (!file.is_open())
    {
        std::cerr << "Error: Unable to open file " << file_name_ << std::endl;
        return;
    }

    std::cout << "Table Name: " << file_name_ << std::endl;
    std::cout << "Number of entries: " << num_entries_ << std::endl;
    std::cout << "Number of columns: " << num_columns_ << std::endl;

    vector<vector<uint32_t>> contents = this->ReadRowGroup_uint32(file, num_columns_);
    for(uint32_t entry = 0; entry < num_entries_; entry++)
    {
        for(uint32_t item : contents[entry]) {
            std::cout << item << " ";
        }
        std::cout << std::endl;
    }
    file.close();
}

bool ColumnarRelationalTable::writeNumEntries(uint32_t num_entries)
{
    std::ofstream file(file_name_, std::ios::binary | std::ios::in | std::ios::out);
    if (!file.is_open())
    {
        return false;
    }

    file.seekp(0);
    file.write(reinterpret_cast<const char *>(&num_entries), sizeof(num_entries));
    file.close();
    return true;
}

void ColumnarRelationalTable::setEntryCount(uint32_t new_count)
{
    this->num_entries_ = new_count;
    writeNumEntries(new_count);
}

uint32_t ColumnarRelationalTable::readNumEntries() const
{
    std::ifstream file(file_name_, std::ios::binary | std::ios::in);
    if (!file.is_open())
    {
        return 0;
    }

    uint32_t num_entries;
    file.read(reinterpret_cast<char *>(&num_entries), sizeof(num_entries));
    file.close();
    return num_entries;
}

uint32_t ColumnarRelationalTable::readNumColumns() const
{
    std::ifstream file(file_name_, std::ios::binary | std::ios::in);
    if (!file.is_open())
    {
        return 0;
    }

    uint32_t num_columns;
    file.seekg(sizeof(num_entries_));
    file.read(reinterpret_cast<char *>(&num_columns), sizeof(num_columns));
    file.close();
    return num_columns;
}
