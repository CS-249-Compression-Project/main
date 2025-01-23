#include "rt.hpp"

int main() {
    RelationalTable a("table1.tbl", 69);
    std::cout << a.readNumEntries() << std::endl;
    std::cout << a.readNumColumns() << std::endl;
    return 0;
}