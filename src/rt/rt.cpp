#include "rt.hpp"
#include "helper.hpp"

#include <fstream>
#include <iostream>

// RelationalTable


RelationalTable::RelationalTable() {}

RelationalTable::RelationalTable(const std::string &file_name) : file_name_(file_name)
{
    if (!parseMetadata())
    {
        std::cerr << "Error: Unable to parse metadata for table " << file_name << std::endl;
    }
}

RelationalTable::RelationalTable(const std::string &file_name, const uint32_t num_columns) : file_name_(file_name), num_columns_(num_columns)
{
    if (fileExists(file_name))
    {
        std::cerr << "Error: Table " << file_name << " already exists" << std::endl;
        return;
    }

    createFile(file_name);

    if (!writeMetadata(0, num_columns))
    {
        std::cerr << "Error: Unable to write metadata for table " << file_name << std::endl;
    }

    this->num_entries_ = 0;
    this->num_columns_ = num_columns;
}

// uses float--we don't really need uint32_t
void RelationalTable::printTable() const
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

    // calculate the size of a row in bytes
    uint32_t row_size = calculateRowSize();

    // loop through each row and print the data
    for (int i = 0; i < num_entries_; i++)
    {
        // calculate the offset to the row_index
        uint32_t offset = sizeof(num_entries_) + sizeof(num_columns_) + i * row_size;

        // seek to the offset
        file.seekg(offset);

        // read the row data
        for (float item : this->getRow_float(i))
        {
            std::cout << item << " ";
        }

        std::cout << std::endl;
    }

    file.close();
}

void RelationalTable::addRow_uint32_t(const std::vector<uint32_t> &row_data)
{
    if (row_data.size() != num_columns_)
    {
        std::cerr << "Error: Row data size does not match number of columns" << std::endl;
        return;
    }

    std::ofstream file(other.file_name_, std::ios::binary | std::ios::app);
    if (!file.is_open())
    {
        std::cerr << "Error: Unable to open file " << other.file_name_ << std::endl;
        return;
    }

    // loop through row_data and write each entry to the file
    // for (const auto& entry : row_data) {
    //     file.write(reinterpret_cast<const char*>(&entry), sizeof(entry));
    // }

    // write the entire row_data vector to the file
    file.write(reinterpret_cast<const char *>(row_data.data()), row_data.size() * sizeof(row_data[0]));

    file.close();
    this->num_entries_++;
    writeNumEntries(num_entries_);
}

void RelationalTable::addRow_float(const std::vector<float> &row_data)
{
    if (row_data.size() != num_columns_)
    {
        std::cerr << "Error: Row data size does not match number of columns" << std::endl;
        return;
    }

    std::ofstream file(file_name_, std::ios::binary | std::ios::app);
    if (!file.is_open())
    {
        std::cerr << "Error: Unable to open file " << file_name_ << std::endl;
        return;
    }

    // write the entire row_data vector to the file
    file.write(reinterpret_cast<const char *>(row_data.data()), row_data.size() * sizeof(row_data[0]));

    file.close();
    this->num_entries_++;
    writeNumEntries(num_entries_);
}

std::vector<uint32_t> RelationalTable::getRow_uint32_t(uint32_t row_index) const
{
    std::ifstream file(file_name_, std::ios::binary | std::ios::in);
    if (!file.is_open())
    {
        std::cerr << "Error: Unable to open file " << file_name_ << std::endl;
        return {};
    }

    // calculate the size of a row in bytes
    uint32_t row_size = calculateRowSize();
    // calculate the offset to the row_index
    uint32_t offset = sizeof(num_entries_) + sizeof(num_columns_) + row_index * row_size;

    // seek to the offset
    file.seekg(offset);

    // read the row data
    std::vector<uint32_t> row_data(num_columns_);
    file.read(reinterpret_cast<char *>(row_data.data()), row_data.size() * sizeof(row_data[0]));

    file.close();
    return row_data;
}

std::vector<float> RelationalTable::getRow_float(uint32_t row_index) const
{
    std::ifstream file(file_name_, std::ios::binary | std::ios::in);
    if (!file.is_open())
    {
        std::cerr << "Error: Unable to open file " << file_name_ << std::endl;
        return {};
    }

    // calculate the size of a row in bytes
    uint32_t row_size = calculateRowSize();
    // calculate the offset to the row_index
    uint32_t offset = sizeof(num_entries_) + sizeof(num_columns_) + row_index * row_size;

    // seek to the offset
    file.seekg(offset);

    // read the row data
    std::vector<float> row_data(num_columns_);
    file.read(reinterpret_cast<char *>(row_data.data()), row_data.size() * sizeof(row_data[0]));

    file.close();
    return row_data;
}

// Perform a join operation with another table and make the new file
RelationalTable RelationalTable::full_outer_join(const RelationalTable &other, const std::string &new_table_file_name) const 
{
    // Left Table Open
    RelationalTable table_left = *this;
    RelationalTable table_right = other;

    // Get the number of columns for the new table
    uint32_t num_columns_new = table_left.num_columns_ + table_right.num_columns_;

    // New Table Open
    RelationalTable table_new(new_table_file_name, num_columns_new);

    // Left Table Open
    std::ifstream file_left(file_name_, std::ios::binary | std::ios::in);
    if (!file_left.is_open())
    {
        std::cerr << "Error: Unable to open file " << file_name_ << std::endl;
        return RelationalTable();
    }

    // Right Table Open
    std::ifstream file_right(other.file_name_, std::ios::binary | std::ios::in);
    if (!file_right.is_open())
    {
        std::cerr << "Error: Unable to open file " << other.file_name_ << std::endl;
        return RelationalTable();
    }

    // Every entry, join left and right and add rows
    for (uint32_t entry_left = 0; entry_left < table_left.num_entries_; entry_left++)
    {
        for (uint32_t entry_right = 0; entry_right < table_right.num_entries_; entry_right++)
        {
            // Get the row data for the left table
            std::vector<float> row_data_left = table_left.getRow_float(entry_left);

            // Get the row data for the right table
            std::vector<float> row_data_right = table_right.getRow_float(entry_right);

            // Combine the row data
            std::vector<float> row_data_new = row_data_left;
            row_data_new.insert(row_data_new.end(), row_data_right.begin(), row_data_right.end());

            // Add the row to the new table
            table_new.addRow_float(row_data_new);
        }
    }

    return RelationalTable(new_table_file_name);
}

RelationalTable RelationalTable::inner_join(const RelationalTable &other, const std::string &new_table_file_name, const std::vector<uint32_t> col1, const std::vector<uint32_t> col2) const
{
    // Left Table Open
    RelationalTable table_left = *this;
    RelationalTable table_right = other;

    // Get the number of columns for the new table
    uint32_t num_columns_new = table_left.num_columns_ + table_right.num_columns_;

    // New Table Open
    RelationalTable table_new(new_table_file_name, num_columns_new);

    // Left Table Open
    std::ifstream file_left(file_name_, std::ios::binary | std::ios::in);
    if (!file_left.is_open())
    {
        std::cerr << "Error: Unable to open file " << file_name_ << std::endl;
        return RelationalTable();
    }

    // Right Table Open
    std::ifstream file_right(other.file_name_, std::ios::binary | std::ios::in);
    if (!file_right.is_open())
    {
        std::cerr << "Error: Unable to open file " << other.file_name_ << std::endl;
        return RelationalTable();
    }

    // Every entry, join left and right and add rows if the columns match
    for (uint32_t entry_left : col1)
    {
        for (uint32_t entry_right : col2)
        {
            // Get the row data for the left table
            std::vector<float> row_data_left = table_left.getRow_float(entry_left);

            // Get the row data for the right table
            std::vector<float> row_data_right = table_right.getRow_float(entry_right);

            // Combine the row data
            std::vector<float> row_data_new = row_data_left;
            row_data_new.insert(row_data_new.end(), row_data_right.begin(), row_data_right.end());

            // Add the row to the new table
            table_new.addRow_float(row_data_new);
        }
    }

    return RelationalTable(new_table_file_name);
}

uint32_t RelationalTable::readNumEntries() const
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

uint32_t RelationalTable::readNumColumns() const
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

bool RelationalTable::parseMetadata()
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

// Calculate the size of a row in bytes
uint32_t RelationalTable::calculateRowSize() const
{
    return this->num_columns_ * sizeof(uint32_t);
}

bool RelationalTable::writeMetadata(uint32_t num_entries, uint32_t num_columns)
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

bool RelationalTable::writeNumEntries(uint32_t num_entries)
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

bool RelationalTable::writeNumColumns(uint32_t num_columns)
{
    std::ofstream file(file_name_, std::ios::binary | std::ios::in | std::ios::out);
    if (!file.is_open())
    {
        return false;
    }

    file.seekp(sizeof(num_entries_));
    file.write(reinterpret_cast<const char *>(&num_columns), sizeof(num_columns));
    file.close();
    return true;
}