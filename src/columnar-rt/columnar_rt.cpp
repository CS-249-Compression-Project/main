#include <string>
#include <fstream>
#include <vector>
using std::vector;

enum RepresentationKind : uint8_t
{
    Direct = 0,
};

// Create a new table with the given column metadata
bool MakeColumnarRelationalTable(const std::string &file_name, const uint32_t num_columns)
{
    // We don't need to specify the type of our columns since we support only integers and floats, both 32 bits.
    std::ofstream file(file_name, std::ios::binary | std::ios::in | std::ios::out);
    if (!file.is_open())
    {
        return false;
    }

    uint32_t num_entries = 0;
    file.write(reinterpret_cast<const char *>(&num_columns), sizeof(num_columns));
    file.write(reinterpret_cast<const char *>(&num_entries), sizeof(num_entries));
    file.close();
    return true;
}

// The eventual generalization of this will be a WriteRowGroup_uint32 that takes another parameter specifying what
// encoding to use for each column.
void WriteRowGroupUncompressed_uint32(std::ofstream &file, const vector<vector<uint32_t>> rows)
{
    size_t num_entries = rows.size();
    size_t num_columns = rows.at(0).size();
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

vector<vector<uint32_t>> ReadRowGroup_uint32(std::ifstream &file, const uint32_t num_columns)
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
                throw "Columns have different row counts";
            }
            entry.push_back(columnData[column][row]);
        }
        rowData.push_back(std::move(entry));
    }
    return rowData;
}
