#include "../columnar-rt/columnar_rt.hpp"
#include "../columnar-rt/helper.hpp"

int main()
{
    removeFile("table3.tbl");

    ColumnarRelationalTable a("table3.tbl", 5);

    std::vector<std::vector<uint32_t>> rows;
    rows.push_back({1, 2, 3});
    rows.push_back({4, 5, 6});
    rows.push_back({7, 8, 9});
    rows.push_back({10, 11, 12});
    rows.push_back({13, 14, 15});
    a.writeRows("table3.tbl", rows);

    std::vector<std::vector<uint32_t>> contents = a.readColumns("table3.tbl", 3);
    for (std::vector<uint32_t> row : contents)
    {
        for (uint32_t item : row)
        {
            std::cout << item << " ";
        }
        std::cout << std::endl;
    }

    std::cout << std::endl;
    return 0;
}