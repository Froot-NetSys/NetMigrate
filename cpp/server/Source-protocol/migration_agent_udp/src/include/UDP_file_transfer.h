#pragma once
#include "socket.h"
#include "concurrentqueue.h"
#include <mutex>
#include <vector>
#include <fstream>
#include <cstdio>
#include <cstdint>
#include <thread>
#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include <sys/types.h>
#include <netinet/in.h>
#include <inttypes.h>
#include <arpa/inet.h>
#include <cstdlib>
#include <cstring>

#define MAXBUFSIZE 1400
#define MAX_WRITE_THREAD_NUM 16
#define SEQ_SIZE 8

#define htonll(x) ((1==htonl(1)) ? (x) : \
    ((uint64_t)htonl((x) & 0xFFFFFFFF) << 32) | htonl((x) >> 32))
#define ntohll(x) ((1==ntohl(1)) ? (x) : \
    ((uint64_t)ntohl((x) & 0xFFFFFFFF) << 32) | ntohl((x) >> 32))

struct buf_q {
    char buf[BUFSIZE];
    uint64_t len;
};

class Sender {
public:
    Sender() { }
    Sender(std::string server_ip, uint16_t server_start_port, uint16_t client_start_port, uint32_t thread_num) 
        : thread_num(thread_num) {
        for (uint32_t i = 0; i < thread_num; i++) {
            ports.push_back(i);
            sockets.emplace_back(std::make_shared<ClientSocket>(server_ip.c_str(), server_start_port + i, client_start_port + i));
            file_streams.push_back(std::ifstream());
        }

    }
    ~Sender() {
        for (int i = 0; i < sockets.size(); i ++) {
            sockets.at(i)->ClientSocketClose();
        }
    }
    
    void SendFile(std::string file_path, uint64_t begin_offset, uint64_t end_offset);

private:

    void SendFileInfo(std::string file_path, uint64_t file_size);
    void SendFileThread(uint32_t thread_id, uint64_t begin_offset, uint64_t end_offset);
    void SendFileComplete();

    std::string file_path = "";
    std::vector<std::ifstream> file_streams;

    uint32_t thread_num;
    std::vector<std::thread> thread_send;
    std::vector<uint16_t> ports;
    std::vector<std::shared_ptr<ClientSocket>> sockets;
    uint64_t seg_file_size[MAX_WRITE_THREAD_NUM];
    uint64_t begin_off[MAX_WRITE_THREAD_NUM];
    uint64_t end_off[MAX_WRITE_THREAD_NUM];
};

class Receiver {
public:
    Receiver() { }
    Receiver(uint16_t start_port, uint32_t thread_num) : thread_num(thread_num) {
        
        for (uint16_t i = start_port; i < start_port + thread_num; i++) {
            ports.push_back(i);
            sockets.emplace_back(std::make_shared<ServerSocket>(i));
            file_streams.push_back(std::ofstream());
        }
    }

    ~Receiver() {  }

    void ReceiveFile(std::string file_path, uint64_t & recv_file_size);

private:
    void ReceiveFileInfo(std::string & file_path, uint64_t & file_size);
    void ReceiveFileThread(uint32_t thread_id, uint64_t begin_offset, uint64_t seg_file_size);
    void WriteFileThread(uint32_t thread_id, uint64_t begin_offset);
    void ReceiveFileComplete(); 

    std::vector<std::ofstream> file_streams;

    uint32_t thread_num;
    std::vector<std::thread> thread_recv;
    std::vector<std::thread> thread_write;
    std::vector<uint16_t> ports;
    std::vector<std::shared_ptr<ServerSocket>> sockets;
    moodycamel::ConcurrentQueue<buf_q> write_buf[MAX_WRITE_THREAD_NUM];
    bool recv_term[MAX_WRITE_THREAD_NUM];
    uint64_t seg_file_size[MAX_WRITE_THREAD_NUM];
    uint64_t begin_off[MAX_WRITE_THREAD_NUM];
};