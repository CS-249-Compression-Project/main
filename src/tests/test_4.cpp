#include "../rt/rt.hpp"
#include "../rt/helper.hpp"

int main()
{
    removeFile("table4.tbl");

    RelationalTable a("table4.tbl", 3);
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
    return 0;
}