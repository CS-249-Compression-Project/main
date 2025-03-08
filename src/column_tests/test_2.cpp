#include "../columnar-rt/columnar_rt.hpp"
#include "../columnar-rt/helper.hpp"

int main()
{
    removeFile("table2.tbl");

    ColumnarRelationalTable a("table2.tbl", 3);
    std::cout << a.readNumEntries() << std::endl;
    std::cout << a.readNumColumns() << std::endl;

    std::vector<std::vector<uint32_t>> rows;
    rows.push_back({1, 2, 3});
    rows.push_back({4, 5, 6});
    rows.push_back({7, 8, 9});
    a.writeRows("table2.tbl", rows);

    std::vector<std::vector<uint32_t>> contents = a.readColumns("table2.tbl", 3);
    for (int i = 0; i < a.readNumEntries(); i++)
    {
        for (uint32_t item : contents[i])
        {
            std::cout << item << " ";
        }
        std::cout << std::endl;
    }

    std::cout << std::endl;
    return 0;
}