#include "socket.h"
#include <sys/time.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>

ServerSocket::ServerSocket(int port) {
    // create socket 
    if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) == 0) {
        perror("cannot create socket\n");
        exit(1);
    }

    addrlen = sizeof(client_addr);

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind failed");
        exit(1);
    }

    // int n = sprintf(interface, "ens2f0np0");
    int disable = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_NO_CHECK, (void*)&disable, sizeof(disable)) < 0) {
        perror("setsockopt failed");
    }

}

int ServerSocket::Send(const char * buf, size_t len) {
    if (sendto(sockfd, buf, len, 0, (struct sockaddr*)&client_addr, addrlen) == -1) {
        perror("sendto failed");
        exit(1);
    }
    return 0;
}

int ServerSocket::Recv(char * buf) {
    int recvlen = -1;
    if ((recvlen = recvfrom(sockfd, buf, BUFSIZE, 0, (struct sockaddr *)&client_addr, &addrlen)) == -1) {
        perror("recvfrom failed");
        exit(1);
    }
    return recvlen;
}

BlockingClientSocket::BlockingClientSocket(const char *server_ip, int server_port, int client_port) {
    if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) == 0) { 
        perror("cannot create socket\n");
        exit(1);
    }
  
    addrlen = sizeof(server_addr);

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_addr.s_addr = inet_addr(server_ip);
    server_addr.sin_port = htons(server_port);
    server_addr.sin_family = AF_INET;

    memset(&client_addr, 0, sizeof(client_addr));
    client_addr.sin_addr.s_addr = INADDR_ANY;
    client_addr.sin_port = htons(client_port);
    client_addr.sin_family = AF_INET;

    if (bind(sockfd, (struct sockaddr*)&client_addr, sizeof(struct sockaddr_in)) < 0) {
        perror("bind failed");
        exit(1);
    }

    // int n = sprintf(interface, "ens2f0np0");
    int disable = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_NO_CHECK, (void*)&disable, sizeof(disable)) < 0) {
        perror("setsockopt failed");
    }
}

int BlockingClientSocket::Send(const char * buf, size_t len) {
    if (sendto(sockfd, buf, len, 0, (struct sockaddr*)&server_addr, addrlen) == -1) {
        perror("sendto failed");
        exit(1);
    }
    return 0;
}

int BlockingClientSocket::Recv(char * buf) {
    int recvlen = -1;
    if ((recvlen = recvfrom(sockfd, buf, BUFSIZE, 0, (struct sockaddr *)&server_addr, &addrlen)) == -1) {
        perror("recvfrom failed");
        exit(1);
    }
    return recvlen;
}

// Non-blocking client socket, used by YCSB client
ClientSocket::ClientSocket(const char *server_ip, int server_port, int client_port) {
    if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) == 0) { 
        perror("cannot create socket\n");
        exit(1);
    }
  
    addrlen = sizeof(server_addr);

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_addr.s_addr = inet_addr(server_ip);
    server_addr.sin_port = htons(server_port);
    server_addr.sin_family = AF_INET;

    // for Client recving 
    memset(&server_addr_1, 0, sizeof(server_addr_1));

    memset(&client_addr, 0, sizeof(client_addr));
    client_addr.sin_addr.s_addr = INADDR_ANY;
    client_addr.sin_port = htons(client_port);
    client_addr.sin_family = AF_INET;

    if (bind(sockfd, (struct sockaddr*)&client_addr, sizeof(struct sockaddr_in)) < 0) {
        perror("bind failed");
        exit(1);
    }

    // int n = sprintf(interface, "ens2f0np0");
    int disable = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_NO_CHECK, (void*)&disable, sizeof(disable)) < 0) {
        perror("setsockopt failed");
    }


    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 8000;
    if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        perror("timeout set failed");
    }

}

int ClientSocket::Send(const char * buf, size_t len) {
    if (sendto(sockfd, buf, len, 0, (struct sockaddr*)&server_addr, addrlen) == -1) {
        perror("sendto failed");
        exit(1);
    }
    return 0;
}

int ClientSocket::Recv(char * buf) {
    int recvlen = -1;
    if ((recvlen = recvfrom(sockfd, buf, BUFSIZE, 0, (struct sockaddr *)&server_addr_1, &addrlen)) == -1) {
        // perror("recvfrom failed");
        // exit(1);
    }
    return recvlen;
}