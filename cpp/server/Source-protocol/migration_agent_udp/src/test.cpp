#include "MigrationManager.h"

/*
void MigrationManager::TestReceiveFile() {
    uint64_t file_size = 0;
    receiver->ReceiveFile("/home/zeying/redis_log/testfile.2", file_size);
    std::cout << "received_file_size = " << fs::file_size("/home/zeying/redis_log/testfile.2") << std::endl;
}

void MigrationManager::TestSendFile() {
    uint64_t file_size = (uint64_t)fs::file_size("/home/zeying/redis_log/testfile.1");
    std::cout << "file_size = " << file_size << std::endl;
    sender->SendFile("/home/zeying/redis_log/testfile.1", 0, file_size);
}

*/

// bash: truncate -s 10G testfile.1
// bash: diff testfile.1 testfile.2