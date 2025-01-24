#ifndef _helper_h_
#define _helper_h_

#include <string>
#include <fstream>
#include <iostream>

bool fileExists(const std::string& file_name);
void createFile(const std::string& file_name);
bool removeFile(const std::string& file_name);

#endif