#include "MigrationManager.h"

void MigrationManager::AOFParser(std::string aof_file_path, std::vector<dst_cmd_q> & new_commands, int offset = 0) {
    std::ifstream aof_file(aof_file_path);
    std::string line;
    std::string cmd_line;
    dst_cmd_q cmd;
    int argc = 0;
    if (aof_file.is_open()) {
        // aof_file.seekg(0, aof_file.beg);
        while (std::getline(aof_file, line)) {
            
            if (line.size() == 0) 
                break;
            if (line[0] != '*') 
                std::cout << "AOF file format error at byte " << aof_file.tellg() << std::endl;

            line.erase(std::remove(line.begin(), line.end(), '\n'), line.end());
            line.erase(std::remove(line.begin(), line.end(), '\r'), line.end());

            argc = stoi(line.substr(1));
            if (argc < 1) 
                std::cout << "AOF file format error" << std::endl;
            
            
            bool flag = 0;
            cmd.op = cmd.val = cmd.key = "";
            for (int i = 0; i < argc * 2; i++) {
                std::getline(aof_file, cmd_line);
                if (cmd_line[0] == '$') 
                    continue;
                
                cmd_line.erase(std::remove(cmd_line.begin(), cmd_line.end(), '\n'), cmd_line.end());
                cmd_line.erase(std::remove(cmd_line.begin(), cmd_line.end(), '\r'), cmd_line.end());

                if (i > 0) {
                    if ((i-1) / 2 == 0) 
                        cmd.op = cmd_line;
                    else if ((i-1)/2 == 1) 
                        cmd.key = cmd_line;
                    else if ((i-1)/2 == 2)
                        cmd.val = cmd_line;
                    else {
                        std::cout << "Not supported cmd" << std::endl;
                        flag = 1;
                        break;
                    }     
                }
            }

            if (flag == 0) {
                new_commands.push_back(cmd);
            }
        }
    }
    else 
        std::cout << aof_file_path << " open error" << std::endl;
    aof_file.close();
}

/*
$cat appendonly.aof.manifest 
file appendonly.aof.15.base.aof seq 15 type b
file appendonly.aof.15.incr.aof seq 15 type i
*/
std::string MigrationManager::getFilePath(std::string type) {
    std::ifstream manifest_file("/var/lib/redis/appendonlydir/appendonly.aof.manifest", std::ifstream::in);
    if (manifest_file.is_open()) {
      std::string line_1, line_2;
      std::getline(manifest_file, line_1);
      std::getline(manifest_file, line_2);
      line_1 = line_1.substr(5);
      line_2 = line_2.substr(5);
      line_1 = line_1.substr(0, line_1.find(" "));
      line_2 = line_2.substr(0, line_2.find(" "));
      if (type == "base") {
        return line_1;
      }
      else if (type == "incr") {
        return line_2;
      }
      else 
          std::cout << "Unknown file type" << std::endl;
      
      manifest_file.close();
    }
    // else 
      // std::cout << "Unable to open manifest file" << std::endl;
    return "";
}