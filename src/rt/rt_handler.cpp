#include "rt.hpp"

int main(int argc, char *argv[])
{
    if (argc < 3)
    {
        std::cerr << "Usage: " << argv[0] << " <create/read/add/outerjoin/innerjoin> <filename> [num_columns]\n";
        return 1;
    }

    std::string command = argv[1];
    std::string filename = argv[2];

    if (command == "create")
    {
        if (argc < 4)
        {
            std::cerr << "Error: You must specify the number of columns to create.\n";
            return 1;
        }

        int num_columns = std::stoi(argv[3]); // Convert the number of columns from string to int
        RelationalTable table(filename, num_columns);
        std::cout << "Table " << filename << " created with " << num_columns << " columns.\n";
    }
    else if (command == "read")
    {
        RelationalTable table(filename);
        table.printTable();
    }
    else if (command == "add")
    {
        if (argc < 4)
        {
            std::cerr << "Error: You must specify the row data to add.\n";
            return 1;
        }

        RelationalTable table(filename);
        std::vector<float> row_data;
        for (int i = 3; i < argc; i++)
        {
            row_data.push_back(std::stof(argv[i]));
        }

        table.addRow_float(row_data);
        std::cout << "Row added to table " << filename << ".\n";
    }
    else if (command == "fullouterjoin")
    {
        if (argc < 5)
        {
            std::cerr << "Error: You must specify two tables to join and the new table name.\n";
            return 1;
        }
        
        RelationalTable table1(argv[3]);
        RelationalTable table2(argv[4]);

        RelationalTable new_table = table1.full_outer_join(table2, filename);
        new_table.printTable();
    }
    else if (command == "innerjoin")
    {
        if (argc < 6)       
        {
            std::cerr << "Error: You must specify two tables to join and the new table name.\n";
            return 1;
        }

        RelationalTable table1(argv[3]);
        RelationalTable table2(argv[5]);
 
        // Split the string of column indices into a vector of integers
        std::vector<uint32_t> col1 = splitString(argv[4]);
        std::vector<uint32_t> col2 = splitString(argv[6]);

        RelationalTable new_table = table1.inner_join(table2, filename, col1, col2);
        new_table.printTable();
    }
    else
    {
        std::cerr << "Invalid command. Use 'create', 'read', 'add', 'fullouterjoin', or 'innerjoin'.\n";
        return 1;
    }

    return 0;
}