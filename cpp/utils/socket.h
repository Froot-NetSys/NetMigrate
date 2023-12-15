#pragma once

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <iostream>

#define BUFSIZE 1454 // Make it MTU
#define MAX_RETRY_TIMES 5

class ServerSocket {
public:
    ServerSocket(int port);
    ~ServerSocket() {
        // std::cout << "ServerSocket closed" << std::endl;
        close(sockfd);
    }
    int Send(const char * buf, size_t len); 
    int Recv(char * buf);

    int sockfd = 0;
    socklen_t addrlen;
    struct sockaddr_in server_addr;
    struct sockaddr_in client_addr;
    char interface[10];
};

class BlockingClientSocket {
public:
    BlockingClientSocket() { } // for YCSB factory class
    BlockingClientSocket(const char *server_ip, int server_port, int client_port);
    ~BlockingClientSocket() { } 
    void ClientSocketClose() {
        // std::cout << "ClientSocket closed" << std::endl;
        close(sockfd);
    }
    int Send(const char * buf, size_t len);
    int Recv(char * buf);

    int sockfd = 0;
    socklen_t addrlen;
    struct sockaddr_in server_addr;
    struct sockaddr_in client_addr;
    char interface[10];
};

// Non-blocking client socket
class ClientSocket {
public:
    ClientSocket() { } // for YCSB factory class
    ClientSocket(const char *server_ip, int server_port, int client_port);
    ~ClientSocket() { } 
    void ClientSocketClose() {
        // std::cout << "ClientSocket closed" << std::endl;
        close(sockfd);
    }
    int Send(const char * buf, size_t len);
    int Recv(char * buf);

    int sockfd = 0;
    socklen_t addrlen;
    struct sockaddr_in server_addr;
    struct sockaddr_in server_addr_1;
    struct sockaddr_in client_addr;
    char interface[10];
};

