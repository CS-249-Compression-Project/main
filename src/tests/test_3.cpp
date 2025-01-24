#include "../rt/rt.hpp"
#include "../rt/helper.hpp"

int main() {
    removeFile("table3.tbl");
    
    RelationalTable a("table3.tbl", 3);
    std::cout << a.readNumEntries() << std::endl;
    std::cout << a.readNumColumns() << std::endl;
    
    a.addRow_float({1.51, 2.2, 3.3});
    a.addRow_float({4.5252, 5.1, 6.51});
    a.addRow_float({7.131, 8.32, 9.3});

    for (int i = 0; i < a.readNumEntries(); i++) {
        for (float item : a.getRow_float(i)) {
            std::cout << item << " ";
        }
        std::cout << std::endl;
    }

    std::cout << std::endl;
    return 0;
}