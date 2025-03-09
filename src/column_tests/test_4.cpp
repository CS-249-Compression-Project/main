#include "../columnar-rt/columnar_rt.hpp"
#include "../columnar-rt/helper.hpp"

int main()
{
    // Make table 5
    removeFile("table5.tbl");

    ColumnarRelationalTable a("table5.tbl", 7);
    std::cout << a.readNumEntries() << std::endl;
    std::cout << a.readNumColumns() << std::endl;

    std::vector<std::vector<uint32_t>> rows;
    rows.push_back({1, 2, 3});
    rows.push_back({4, 5, 6});
    rows.push_back({7, 8, 9});
    rows.push_back({10, 11, 12});
    rows.push_back({13, 14, 15});
    rows.push_back({16, 17, 18});
    rows.push_back({19, 20, 21});
    a.writeRows("table5.tbl", rows);

    std::vector<std::vector<uint32_t>> contents_a = a.readRows("table5.tbl", 3);
    for (std::vector<uint32_t> row : contents_a)
    {
        for (uint32_t item : row)
        {
            std::cout << item << " ";
        }
        std::cout << std::endl;
    }

    std::cout << std::endl;

    // Make table 6
    removeFile("table6.tbl");

    ColumnarRelationalTable b("table6.tbl", 3);
    std::cout << b.readNumEntries() << std::endl;
    std::cout << b.readNumColumns() << std::endl;

    std::vector<std::vector<uint32_t>> rows_b;
    rows_b.push_back({1, 2, 3});
    rows_b.push_back({4, 5, 6});
    rows_b.push_back({7, 8, 9});
    b.writeRows("table6.tbl", rows_b);

    std::vector<std::vector<uint32_t>> contents_b = b.readRows("table6.tbl", 3);
    for (std::vector<uint32_t> row : contents_b)
    {
        for (uint32_t item : row)
        {
            std::cout << item << " ";
        }
        std::cout << std::endl;
    }

    // Make table 7 via full outer join
    removeFile("table7.tbl");
    ColumnarRelationalTable c = a.full_outer_join(b, "table7.tbl");
    cout << endl;

    std::vector<std::vector<uint32_t>> contents_c = c.readRows("table7.tbl", 6);
    for (std::vector<uint32_t> row : contents_c)
    {
        for (uint32_t item : row)
        {
            std::cout << item << " ";
        }
        std::cout << std::endl;
    }

    return 0;
}