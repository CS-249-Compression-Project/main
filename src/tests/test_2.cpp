#include "../rt/rt.hpp"
#include "../rt/helper.hpp"

int main() {
    removeFile("table2.tbl");
    
    RelationalTable a("table2.tbl", 3);
    std::cout << a.readNumEntries() << std::endl;
    std::cout << a.readNumColumns() << std::endl;
    
    a.addRow_uint32_t({1, 2, 3});
    a.addRow_uint32_t({4, 5, 6});
    a.addRow_uint32_t({7, 8, 9});

    for (int i = 0; i < a.readNumEntries(); i++) {
        for (uint32_t item : a.getRow_uint32_t(i)) {
            std::cout << item << " ";
        }
        std::cout << std::endl;
    }

    std::cout << std::endl;
    return 0;
}