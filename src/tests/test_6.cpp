#include "../rt/rt.hpp"
#include "../rt/helper.hpp"

int main()
{
    // Make table 8
    removeFile("table8.tbl");

    RelationalTable a("table8.tbl", 3);
    std::cout << a.readNumEntries() << std::endl;
    std::cout << a.readNumColumns() << std::endl;

    a.addRow_float({1.51, 2.2, 3.3});
    a.addRow_float({4.5252, 5.1, 6.51});
    a.addRow_float({7.131, 8.32, 9.3});
    a.addRow_float({10.1, 11.2, 12.3});
    a.addRow_float({13.1, 14.2, 15.3});
    a.addRow_float({16.1, 17.2, 18.3});
    a.addRow_float({19.1, 20.2, 21.3});

    a.printTable();

    std::cout << std::endl;

    // Make table 9
    removeFile("table9.tbl");

    RelationalTable b("table9.tbl", 3);
    std::cout << b.readNumEntries() << std::endl;
    std::cout << b.readNumColumns() << std::endl;

    b.addRow_float({1.51, 2.2, 3.3});
    b.addRow_float({4.5252, 5.1, 6.51});
    b.addRow_float({7.131, 8.32, 9.3});

    for (int i = 0; i < b.readNumEntries(); i++)
    {
        for (float item : b.getRow_float(i))
        {
            std::cout << item << " ";
        }
        std::cout << std::endl;
    }

    std::cout << std::endl;

    // Make table 10 via full outer join
    removeFile("table10.tbl");
    
    std::vector<uint32_t> col1 = {0, 1, 2};
    std::vector<uint32_t> col2 = {2, 1, 0};
    RelationalTable c = a.inner_join(b, "table10.tbl", col1, col2);

    c.printTable();

    return 0;
}