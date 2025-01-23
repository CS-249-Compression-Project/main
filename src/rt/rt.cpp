#include "rt.hpp"

#include <fstream>
#include <iostream>

// Helper

bool fileExists(const std::string& file_name) {
    std::ifstream file(file_name);  // Try to open the file in input mode
    return file.good();             // If the file is openable, it exists
}

void createFile(const std::string& file_name) {
    std::ofstream file(file_name, std::ios::out | std::ios::trunc);
    file.close();
}

// RelationalTable

RelationalTable::RelationalTable(const std::string& file_name) : file_name_(file_name) {
    if (!parseMetadata()) {
        std::cerr << "Error: Unable to parse metadata for table " << file_name << std::endl;
    }
}

RelationalTable::RelationalTable(const std::string& file_name, const uint32_t num_columns) : file_name_(file_name), num_columns_(num_columns) {
    if (fileExists(file_name)) {
        std::cerr << "Error: Table " << file_name << " already exists" << std::endl;
        return;
    }

    createFile(file_name);
    
    if (!writeMetadata(0, num_columns)) {
        std::cerr << "Error: Unable to write metadata for table " << file_name << std::endl;
    }

    this->num_entries_ = 0;
    this->num_columns_ = num_columns;
}

bool RelationalTable::writeNumEntries(uint32_t num_entries) {
    std::ofstream file(file_name_, std::ios::binary | std::ios::in | std::ios::out);
    if (!file.is_open()) {
        return false;
    }

    file.seekp(0);
    file.write(reinterpret_cast<const char*>(&num_entries), sizeof(num_entries));
    file.close();
    return true;
}

bool RelationalTable::writeNumColumns(uint32_t num_columns) {
    std::ofstream file(file_name_, std::ios::binary | std::ios::in | std::ios::out);
    if (!file.is_open()) {
        return false;
    }

    file.seekp(sizeof(num_entries_));
    file.write(reinterpret_cast<const char*>(&num_columns), sizeof(num_columns));
    file.close();
    return true;
}

bool RelationalTable::parseMetadata() {
    std::ifstream file(file_name_, std::ios::binary | std::ios::in);
    if (!file.is_open()) {
        return false;
    }

    file.read(reinterpret_cast<char*>(&num_entries_), sizeof(num_entries_));
    file.read(reinterpret_cast<char*>(&num_columns_), sizeof(num_columns_));
    file.close();
    return true;
}

// Calculate the size of a row in bytes
uint32_t RelationalTable::calculateRowSize() const {
    return this->num_columns_ * sizeof(uint32_t);
}

bool RelationalTable::writeMetadata(uint32_t num_entries, uint32_t num_columns) {
    std::ofstream file(file_name_, std::ios::binary | std::ios::in | std::ios::out);
    if (!file.is_open()) {
        return false;
    }

    file.write(reinterpret_cast<const char*>(&num_entries), sizeof(num_entries));
    file.write(reinterpret_cast<const char*>(&num_columns), sizeof(num_columns));
    file.close();
    return true;
}

uint32_t RelationalTable::readNumEntries() const {
    std::ifstream file(file_name_, std::ios::binary | std::ios::in);
    if (!file.is_open()) {
        return 0;
    }

    uint32_t num_entries;
    file.read(reinterpret_cast<char*>(&num_entries), sizeof(num_entries));
    file.close();
    return num_entries;
}

uint32_t RelationalTable::readNumColumns() const {
    std::ifstream file(file_name_, std::ios::binary | std::ios::in);
    if (!file.is_open()) {
        return 0;
    }

    uint32_t num_columns;
    file.seekg(sizeof(num_entries_));
    file.read(reinterpret_cast<char*>(&num_columns), sizeof(num_columns));
    file.close();
    return num_columns;
}

#include <iostream>