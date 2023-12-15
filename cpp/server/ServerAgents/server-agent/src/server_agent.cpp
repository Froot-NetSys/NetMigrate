#include <mutex>

#include "server_agent.h"
#include "constants.h"

std::mutex iomutex;

void ServerAgent::RequestHandlerThread(uint32_t thread_id) {
    char buf[BUFSIZE];
    memset(buf, 0, sizeof(char) * BUFSIZE);

    uint32_t recvPkt = 0;
    struct mg_hdr_t mg_hdr;

    OptionalString val;
    struct mg_read pkt_read;
    struct mg_read_reply reply_pkt_read;
    struct mg_write pkt_write;
    struct mg_write_reply reply_pkt_write;
    struct mg_delete pkt_delete;
    struct mg_delete_reply reply_pkt_delete;
    long long res = 0;
    int recvlen = 0;

    char key_pkt[KEY_SIZE + 1];
    char val_pkt[VAL_SIZE + 1];
    
    while (true) {
        if ((recvlen = server_sockets[thread_id]->Recv(buf)) < 0) {
            const std::lock_guard<std::mutex> lock(iomutex);
            cout << "recvlen = " << recvlen << endl;
            continue;
        }
        
        recvPkt++;
        if(recvPkt % MILLION == 0) {
            const std::lock_guard<std::mutex> lock(iomutex);
            std::cout << "Thread " << thread_id << ": Recv packet " << recvPkt << std::endl;
        }   
            

        // Deserialize packet and handle client requests to Redis
        DeserializeMgHdr(buf, &mg_hdr);
    
        switch (mg_hdr.op) {
            case _MG_READ: 
                DeserializeReadPkt(buf, &pkt_read);
                mg_hdr.op = _MG_READ_REPLY;
                DeserializeKey(key_pkt, pkt_read.key);
                val = redis_agents[mg_hdr.dbPort]->redis->get(std::string(key_pkt)); // TODO: pin each redis instance to a thread for performance improvement?
                reply_pkt_read = {0};
                if (val.has_value()) {
                    strncpy(reply_pkt_read.val, val.value().c_str(), std::min(VAL_SIZE, (int)val.value().size()));
                    reply_pkt_read.ver = pkt_read.ver | (server_type << 1) | 1; 
                }
                else {
                    strncpy(reply_pkt_read.val, val_not_found, VAL_SIZE);
                    reply_pkt_read.ver = pkt_read.ver | (server_type << 1); 
                }
                
                SerializeMgHdr(buf, &mg_hdr);
                SerializeReadReplyPkt(buf, &reply_pkt_read);
                
                server_sockets[thread_id]->Send(buf, HDR_SIZE + sizeof(struct mg_read_reply));
                break;

            case _MG_WRITE:
                DeserializeWritePkt(buf, &pkt_write);
                mg_hdr.op = _MG_WRITE_REPLY;
                reply_pkt_write = {0};
                DeserializeKey(key_pkt, pkt_write.key);
                strncpy(val_pkt, pkt_write.val, VAL_SIZE);
                res = redis_agents[mg_hdr.dbPort]->redis->set(string(key_pkt), string(val_pkt));
                reply_pkt_write.ver = (server_type << 1) | (res > 0); 
                
                SerializeMgHdr(buf, &mg_hdr);
                SerializeWriteReplyPkt(buf, &reply_pkt_write);
                server_sockets[thread_id]->Send(buf, HDR_SIZE + sizeof(struct mg_write_reply));
                break;

            case _MG_DELETE:
                
                DeserializeDeletePkt(buf, &pkt_delete);
                mg_hdr.op = _MG_DELETE_REPLY;
                reply_pkt_delete = {0};
                DeserializeKey(key_pkt, pkt_delete.key);
                res = redis_agents[mg_hdr.dbPort]->redis->del(std::string(key_pkt));
                reply_pkt_delete.ver = (server_type << 1) | (res > 0); 
                
                SerializeMgHdr(buf, &mg_hdr);
                SerializeDeleteReplyPkt(buf, &reply_pkt_delete);
                server_sockets[thread_id]->Send(buf, sizeof(mg_delete_reply));
                break;
            default:
                {
                    const std::lock_guard<std::mutex> lock(iomutex);
                    std::cout << "Currently not handled packet" << std::endl;
                }
        }
         
    }

}

void ServerAgent::RequestHandler() {
    for (uint32_t i = 0; i < thread_num; i++) {
        threads.emplace_back(&ServerAgent::RequestHandlerThread, this, i);
    }

    for (uint32_t i = 0; i < thread_num; i++) {
        threads.at(i).join();
    }
}