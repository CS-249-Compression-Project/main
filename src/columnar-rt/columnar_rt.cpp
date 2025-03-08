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
        std::cerr << "Error: Table " << file_name << "already exists" << std::endl;
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
void ColumnarRelationalTable::WriteRowGroupUncompressed_uint32(std::ofstream &file, const vector<vector<uint32_t>> rows) const
{
    for(vector<uint32_t> row : rows) {
        for(uint32_t item : row) {
            cout << item << " ";
        }
        cout << endl;
    }
    size_t num_entries = rows.size();
    size_t num_columns = rows.at(0).size();
    cout << rows.size() << " " << rows.at(0).size() << endl;
    for (size_t i = 0; i < num_columns; i++)
    {
        const RepresentationKind columnRepresentation = RepresentationKind::Direct;
        // In the generalized version we will need to encode the column's values before we know
        // how many bytes it takes up - e.g. for run-length encoding.
        const uint32_t bytes_used = num_entries * sizeof(uint32_t);
        file.write(reinterpret_cast<const char *>(&columnRepresentation), sizeof(columnRepresentation));
        file.write(reinterpret_cast<const char *>(&bytes_used), sizeof(bytes_used));
    }
    for (size_t column = 0; column < num_columns; column++)
    {
        for (const vector<uint32_t> &row : rows)
        {
            file.write(reinterpret_cast<const char *>(&row[column]), sizeof(row[column]));
        }
    }
}

void ColumnarRelationalTable::writeRows(std::string filename, const vector<vector<uint32_t>> rows) const
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

vector<vector<uint32_t>> ColumnarRelationalTable::readColumns(string filename, uint32_t columns) {
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
    char buffer[4];
    vector<RepresentationKind> columnRepresentations;
    vector<uint32_t> bytesUsed;
    for (uint32_t i = 0; i < num_columns; i++)
    {
        file.read(buffer, sizeof(RepresentationKind));
        columnRepresentations.push_back(*reinterpret_cast<RepresentationKind *>(&buffer));
        file.read(buffer, sizeof(uint32_t));
        bytesUsed.push_back(*reinterpret_cast<uint32_t *>(&buffer));
    }

    vector<vector<uint32_t>> columnData;
    for (uint32_t column = 0; column < num_columns; column++)
    {
        vector<uint32_t> thisColumn;
        switch (columnRepresentations[column])
        {
        case RepresentationKind::Direct:
            if (bytesUsed[column] % sizeof(uint32_t) != 0)
            {
                cout << "a" << endl;
                throw "Bad number of bytes for direct-represented uint32_ts";
            }
            for (uint32_t i = 0; i < bytesUsed[column]; i += sizeof(uint32_t))
            {
                // TODO this can probably be optimized by reading multiple uint32_ts at a time
                // and doing thisColumn.insert() for all of them.
                // Make sure to increase the size of buffer to do that.
                file.read(buffer, sizeof(uint32_t));
                thisColumn.push_back(*reinterpret_cast<uint32_t *>(&buffer));
            }
            break;
        default:
            throw "Unknown representation kind";
        }
        columnData.push_back(thisColumn);
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
                cout << "b" << endl;
                throw "Columns have different row counts";
            }
            entry.push_back(columnData[column][row]);
        }
        rowData.push_back(std::move(entry));
    }
    return rowData;
}

ColumnarRelationalTable ColumnarRelationalTable::full_outer_join(const ColumnarRelationalTable &other, const std::string &new_table_file_name) const 
{
    // Left Table Open
    ColumnarRelationalTable table_left = *this;
    ColumnarRelationalTable table_right = other;

    int new_entry_count = this->num_entries_;

    // Left Table Open
    std::ifstream file_left(file_name_, std::ios::binary | std::ios::in);
    if (!file_left.is_open())
    {
        std::cerr << "Error: Unable to open file " << file_name_ << std::endl;
        return ColumnarRelationalTable();
    }

    // Right Table Open
    std::ifstream file_right(other.file_name_, std::ios::binary | std::ios::in);
    if (!file_right.is_open())
    {
        std::cerr << "Error: Unable to open file " << other.file_name_ << std::endl;
        return ColumnarRelationalTable();
    }

    // Read row information for both tables
    vector<vector<uint32_t>> left_rows = this->ReadRowGroup_uint32(file_left, table_left.num_columns_);
    vector<vector<uint32_t>> right_rows = this->ReadRowGroup_uint32(file_right, table_right.num_columns_);
    vector<vector<uint32_t>> new_rows;

    // Every entry, join left and right and add rows
    for (uint32_t entry_left = 0; entry_left < table_left.num_entries_; entry_left++)
    {
        for (uint32_t entry_right = 0; entry_right < table_right.num_entries_; entry_right++)
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

    writeRows(new_table_file_name, new_rows);
    file_left.close();
    file_right.close();
    ColumnarRelationalTable toReturn = ColumnarRelationalTable(new_table_file_name);
    toReturn.setEntryCount(new_entry_count);
    return toReturn;
}

ColumnarRelationalTable ColumnarRelationalTable::inner_join(const ColumnarRelationalTable &other, const std::string &new_table_file_name, const std::vector<uint32_t> col1, const std::vector<uint32_t> col2) const
{
    // Left Table Open
    ColumnarRelationalTable table_left = *this;
    ColumnarRelationalTable table_right = other;

    int new_entry_count = this->num_entries_;

    // Left Table Open
    std::ifstream file_left(file_name_, std::ios::binary | std::ios::in);
    if (!file_left.is_open())
    {
        std::cerr << "Error: Unable to open file " << file_name_ << std::endl;
        return ColumnarRelationalTable();
    }

    // Right Table Open
    std::ifstream file_right(other.file_name_, std::ios::binary | std::ios::in);
    if (!file_right.is_open())
    {
        std::cerr << "Error: Unable to open file " << other.file_name_ << std::endl;
        return ColumnarRelationalTable();
    }

    // Read row information for both tables
    vector<vector<uint32_t>> left_rows = this->ReadRowGroup_uint32(file_left, table_left.num_columns_);
    vector<vector<uint32_t>> right_rows = this->ReadRowGroup_uint32(file_right, table_right.num_columns_);
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
    writeRows(new_table_file_name, new_rows);
    file_left.close();
    file_right.close();
    ColumnarRelationalTable toReturn = ColumnarRelationalTable(new_table_file_name);
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
