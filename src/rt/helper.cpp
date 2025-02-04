#include "helper.hpp"

bool fileExists(const std::string &file_name)
{
    std::ifstream file(file_name); // Try to open the file in input mode
    return file.good();            // If the file is openable, it exists
}

void createFile(const std::string &file_name)
{
    std::ofstream file(file_name, std::ios::out | std::ios::trunc);
    file.close();
}

bool removeFile(const std::string &file_name)
{
    if (std::remove(file_name.c_str()) != 0)
    {
        return false;
    }

    return true;
}

std::vector<uint32_t> splitString(const std::string &str)
{
    std::vector<uint32_t> result;
    std::string temp;

    for (char c : str)
    {
        if (c == ',')
        {
            result.push_back(std::stoi(temp));
            temp.clear();
        }
        else
        {
            temp.push_back(c);
        }
    }

    result.push_back(std::stoi(temp));
    return result;
}