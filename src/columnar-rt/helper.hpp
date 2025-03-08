#ifndef _helper_h_
#define _helper_h_

#include <string>
#include <vector>
#include <fstream>
#include <iostream>
#include <cstdint>

bool fileExists(const std::string& file_name);
void createFile(const std::string& file_name);
bool removeFile(const std::string& file_name);
std::vector<uint32_t> splitString(const std::string& str);

#endif