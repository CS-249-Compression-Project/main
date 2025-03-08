#include "columnar_rt.hpp"
#include "helper.hpp"
#include <cstdint>

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
            std::cerr << "Usage: ./rt_program create <filename.tbl> <num_col>\n";
            return 1;
        }

        int num_columns = std::stoi(argv[3]); // Convert the number of columns from string to int
        ColumnarRelationalTable table(filename, num_columns);
        std::cout << "Table " << filename << " created with " << num_columns << " columns.\n";
    }
    else if (command == "read")
    {
        if (argc < 3)
        {
            std::cerr << "Usage: ./rt_program read <filename.tbl>\n";
            return 1;
        }
        ColumnarRelationalTable table(filename);
        table.printTable();
    }
    else if (command == "add")
    {
        if (argc < 4)
        {
            std::cerr << "Usage: ./rt_program add <filename.tbl> <item1> <item2> ... <itemN>\n";
            return 1;
        }

        ColumnarRelationalTable table(filename);
        std::vector<uint32_t> row_data;
        for (int i = 3; i < argc; i++)
        {
            row_data.push_back(std::stof(argv[i]));
        }
        std::vector<std::vector<uint32_t>> row_wrap;
        row_wrap.push_back(row_data);
        table.writeRows(filename, row_wrap);
        std::cout << "Row added to table " << filename << ".\n";
    }
    else if (command == "fullouterjoin")
    {
        if (argc < 5)
        {
            std::cerr << "Usage: ./rt_program fullouterjoin <new_filename.tbl> <table1.tbl> <table2.tbl>\n";
            return 1;
        }
        
        ColumnarRelationalTable table1(argv[3]);
        ColumnarRelationalTable table2(argv[4]);

        ColumnarRelationalTable new_table = table1.full_outer_join(table2, filename);
        new_table.printTable();
    }
    else if (command == "innerjoin")
    {
        if (argc < 6)       
        {
            std::cerr << "Usage: ./rt_program innerjoin <new_filename.tbl> <table1.tbl> <\"#,#,#,...\"> <table2.tbl> <\"#,#,#\">\n";
            return 1;
        }

        ColumnarRelationalTable table1(argv[3]);
        ColumnarRelationalTable table2(argv[5]);
 
        // Split the string of column indices into a vector of integers
        std::vector<uint32_t> col1 = splitString(argv[4]);
        std::vector<uint32_t> col2 = splitString(argv[6]);

        ColumnarRelationalTable new_table = table1.inner_join(table2, filename, col1, col2);
        new_table.printTable();
    }
    else
    {
        std::cerr << "Invalid command. Use 'create', 'read', 'add', 'fullouterjoin', or 'innerjoin'.\n";
        return 1;
    }

    return 0;
}