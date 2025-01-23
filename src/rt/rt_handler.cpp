#include "rt.hpp"

int main() {
    RelationalTable a("table1.tbl", 69);
    std::cout << a.readNumEntries() << std::endl;
    std::cout << a.readNumColumns() << std::endl;
    

    RelationalTable b("table1.tbl");
    std::cout << b.readNumEntries() << std::endl;
    std::cout << b.readNumColumns() << std::endl;
    return 0;
}