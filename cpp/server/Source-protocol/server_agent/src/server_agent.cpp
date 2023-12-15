#include <mutex>

#include "server_agent.h"
#include "constants.h"
#include "concurrentqueue.h" //thread-safe queue

std::mutex iomutex;
std::mutex send_src_exit_mutex;

using namespace std;

void ServerAgent::RequestHandlerThread(uint32_t thread_id) {
    char buf[BUFSIZE];
    memset(buf, 0, sizeof(char) * BUFSIZE);

    uint32_t recvPkt = 0;
    struct mg_hdr_t mg_hdr;

    OptionalString val;
    struct mg_read pkt_read;
    struct mg_write pkt_write;
    int recvlen = 0;

    char key_pkt[KEY_SIZE + 1] = {0};
    char val_pkt[VAL_SIZE + 1] = {0};

    vector<cmd_q> op_cmds;
    cmd_q tmp_write;
    cmd_q tmp_read;
    
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
            
                tmp_read.op = "GET";
                tmp_read.key = std::string(key_pkt);
                tmp_read.val = "";
                tmp_read.socket_id = thread_id;
                tmp_read.seq = mg_hdr.seq;
                tmp_read.ver = pkt_read.ver;
                op_cmds.push_back(tmp_read);
                break;

            case _MG_WRITE:
                DeserializeWritePkt(buf, &pkt_write);
                mg_hdr.op = _MG_WRITE_REPLY;
                DeserializeKey(key_pkt, pkt_write.key);
                strncpy(val_pkt, pkt_write.val, VAL_SIZE);
                
                
                tmp_write.op = "SET";
                tmp_write.key = string(key_pkt);
                tmp_write.val = string(val_pkt);
                tmp_write.socket_id = thread_id;
                tmp_write.seq = mg_hdr.seq;
                tmp_write.ver = 0;
                op_cmds.push_back(tmp_write);
                break;

            default:
                {
                    const std::lock_guard<std::mutex> lock(iomutex);
                    std::cout << "Currently not handled packet" << std::endl;
                }
        }

        if (recvPkt % PIPELINE == 0)
        {
            op_queue[mg_hdr.dbPort].enqueue_bulk(op_cmds.data(), op_cmds.size());
            vector<cmd_q>().swap(op_cmds);
        }
         
    }

}

void ServerAgent::RedisHandlerThread(uint32_t thread_id) {
    uint32_t scale_id = thread_id / redis_ports.size();
    uint32_t redis_id = thread_id % redis_ports.size();
    
    char buf[BUFSIZE];
    memset(buf, 0, sizeof(char) * BUFSIZE);

    struct mg_hdr_t mg_hdr;

    OptionalString val;
    bool res;
    struct mg_read_reply reply_pkt_read;
    struct mg_write_reply reply_pkt_write;

    cmd_q bulk_data[PIPELINE];
    vector<cmd_q> cmds;
    int socket_id;
    uint64_t seq;
    uint8_t ver;

    uint32_t last_cmd_idx;

    uint32_t send_src_exit_count = 0;

    while (true) {
        if (op_queue[redis_ports[redis_id]].size_approx() > 0) {
            size_t count = op_queue[redis_ports[redis_id]].try_dequeue_bulk(bulk_data, PIPELINE);
            for (uint32_t i = 0; i < count; i++) {
                string op = bulk_data[i].op;
                string key = bulk_data[i].key;
                string val = bulk_data[i].val;
                try {
                    redis_agents.at(redis_ports[redis_id])->AppendCmd(scale_id, op, key, val);
                }
                catch (const Error &err) {
                    // std::cout << "redis io error catched" << std::endl;
                    exit_flag = true;
                }
                    
                cmds.push_back(bulk_data[i]);
            }

            last_cmd_idx = 0;
          
            if (!exit_flag || server_type == 1) {
                try {
                    auto pipe_replies = redis_agents.at(redis_ports[redis_id])->pipe.at(scale_id)->exec();
                    for (uint32_t i = 0; i < cmds.size(); i++) {
                        last_cmd_idx = i;
                        if (exit_flag && server_type == 0) 
                            break;
                    
                        socket_id = cmds[i].socket_id;
                        seq = cmds[i].seq;
                        ver = cmds[i].ver;
                        if (cmds[i].op == "GET") {
                            reply_pkt_read = {0};
                            val = pipe_replies.get<OptionalString>(i);
                            
                            if (val.has_value()) {
                                strncpy(reply_pkt_read.val, val.value().c_str(), std::min(VAL_SIZE, (int)val.value().size()));
                                reply_pkt_read.ver = ver | (server_type << 1) | 1; 
                            }
                            else {
                                strncpy(reply_pkt_read.val, val_not_found, VAL_SIZE);
                                reply_pkt_read.ver = ver | (server_type << 1); 
                            }
                            
                            mg_hdr.op = _MG_READ_REPLY;
                            mg_hdr.dbPort = redis_ports[redis_id];
                            mg_hdr.seq = seq;

                            SerializeMgHdr(buf, &mg_hdr);
                            SerializeReadReplyPkt(buf, &reply_pkt_read);
                            
                            server_sockets[socket_id]->Send(buf, HDR_SIZE + sizeof(struct mg_read_reply));
                        }
                        else {
                            res = pipe_replies.get<bool>(i);

                            reply_pkt_write = {0};
                            reply_pkt_write.ver = (server_type << 1) | (res > 0); 
                            
                            mg_hdr.op = _MG_WRITE_REPLY;
                            mg_hdr.dbPort = redis_ports[redis_id];
                            mg_hdr.seq = seq;

                            SerializeMgHdr(buf, &mg_hdr);
                            SerializeWriteReplyPkt(buf, &reply_pkt_write);
                            server_sockets[socket_id]->Send(buf, HDR_SIZE + sizeof(struct mg_write_reply));
                        }
                    }
                    vector<cmd_q>().swap(cmds);
                }
                catch (const Error &err) {
                    // std::cout << "redis io error catched" << std::endl;
                    exit_flag = true;
                }
            }

            if (exit_flag && server_type == 0) {
                for (uint32_t i = last_cmd_idx; i < cmds.size(); i++) {
                    socket_id = cmds[i].socket_id;
                    seq = cmds[i].seq;
                    ver = cmds[i].ver;
                    if (cmds[i].op == "GET") {
                        reply_pkt_read = {0};
                        reply_pkt_read.ver = 255;
                        mg_hdr.op = _MG_READ_REPLY;
                        mg_hdr.dbPort = redis_ports[redis_id];
                        mg_hdr.seq = seq;
                        SerializeMgHdr(buf, &mg_hdr);
                        SerializeReadReplyPkt(buf, &reply_pkt_read);
                        server_sockets[socket_id]->Send(buf, HDR_SIZE + sizeof(struct mg_read_reply));
                    }
                    else { // "SET"
                        reply_pkt_write = {0};
                        reply_pkt_write.ver = 255;
                        mg_hdr.op = _MG_WRITE_REPLY;
                        mg_hdr.dbPort = redis_ports[redis_id];
                        mg_hdr.seq = seq;
                        SerializeMgHdr(buf, &mg_hdr);
                        SerializeWriteReplyPkt(buf, &reply_pkt_write);
                        server_sockets[socket_id]->Send(buf, HDR_SIZE + sizeof(struct mg_write_reply));
                    }
                    send_src_exit_signal[socket_id] = true;    
                }

                send_src_exit_count = 0;
                for (uint32_t i = 0; i < thread_num; i++) {
                    send_src_exit_count += send_src_exit_signal[i];
                }
                if (send_src_exit_count == thread_num) {
                    std::cout << "Send switch to destination signal to clients. Exit..." << std::endl;
                    exit(1);
                }
            }  
            
        }
    }
}

void ServerAgent::RequestHandler() {
    for (uint32_t i = 0; i < thread_num; i++) {
        threads.emplace_back(&ServerAgent::RequestHandlerThread, this, i);
    }

    uint32_t redis_thread_id = 0;
    for (uint32_t j = 0; j < redis_handler_scale; j++) { 
        for (uint32_t i = 0; i < redis_ports.size(); i++) {
            threads.emplace_back(&ServerAgent::RedisHandlerThread, this, redis_thread_id);
            redis_thread_id++;
        }
    }
    

    for (uint32_t i = 0; i < thread_num + redis_ports.size(); i++) {
        threads.at(i).join();
    }
}