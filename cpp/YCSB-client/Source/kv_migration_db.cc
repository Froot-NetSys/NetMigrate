#include <sys/stat.h>
#include <cstring>
#include <thread>
#include <chrono>
#include <regex>

#include "kv_migration_db.h"

#include "../core/properties.h"
#include "../core/db_factory.h"

#include "../../utils/constants.h"
#include "../../utils/pkt_headers.h"

using namespace std::chrono;

namespace ycsbc {

    void KVDB::Init(uint32_t thread_id) {
        seq = 0;

        const utils::Properties& props =  *props_;

        std::string src_redis_port_list = props.GetProperty("src_redis.port_list");
        std::regex reg("[,]+");
        std::sregex_token_iterator iter(src_redis_port_list.begin(), src_redis_port_list.end(), reg, -1);
        std::sregex_token_iterator end;
        std::vector<std::string> src_str_redis_ports(iter, end);
        for (auto & port : src_str_redis_ports) {
            src_redis_ports.push_back(stoi(port));
        }

        std::string src_agent_ip = props.GetProperty("src_agent.ip_addr");
        src_agent_start_port = stoi(props.GetProperty("src_agent.start_port"));
        uint16_t src_client_start_port = stoi(props.GetProperty("src_client.start_port"));
        // thread_num = stoi(props.GetProperty("agent.thread_num"));
        std::string src_trans_ip = props.GetProperty("src_trans.ip_addr");

        std::string dst_redis_port_list = props.GetProperty("dst_redis.port_list");
        std::sregex_token_iterator dst_iter(dst_redis_port_list.begin(), dst_redis_port_list.end(), reg, -1);
        std::sregex_token_iterator dst_end;
        std::vector<std::string> dst_str_redis_ports(dst_iter, dst_end);
        for (auto & port : dst_str_redis_ports) {
            dst_redis_ports.push_back(stoi(port));
        }

        std::string dst_agent_ip = props.GetProperty("dst_agent.ip_addr");
        dst_agent_start_port = stoi(props.GetProperty("dst_agent.start_port"));
        uint16_t dst_client_start_port = stoi(props.GetProperty("dst_client.start_port"));
        std::string dst_trans_ip = props.GetProperty("dst_trans.ip_addr");

        redis_ports.clear();
        std::copy(src_redis_ports.begin(), src_redis_ports.end(), std::back_inserter(redis_ports));
        
        decltype(uniform_distr.param()) new_range(0, src_redis_ports.size()-1);
        uniform_distr.param(new_range);
        
        socket_id = 0;
        /*
        // debug 
        socket_id = 1;
        redis_ports.clear();
        std::copy(dst_redis_ports.begin(), dst_redis_ports.end(), std::back_inserter(redis_ports));
        */


        std::cout << "Redis address: tcp://" << src_agent_ip << ":";
        for (uint32_t i = 0; i < src_redis_ports.size(); i++) {
            std::cout << src_redis_ports[i] << ", " ;
        }
        std::cout << std::endl;

        std::cout << "KV Server Agent Address " << thread_id << ": " << src_trans_ip + ":" + std::to_string(src_agent_start_port + thread_id) << std::endl;
        sockets.emplace_back(std::make_shared<ClientSocket>(src_trans_ip.c_str(), src_agent_start_port + thread_id, src_client_start_port + thread_id));
        sockets.emplace_back(std::make_shared<ClientSocket>(dst_trans_ip.c_str(), dst_agent_start_port + thread_id, dst_client_start_port + thread_id));
    }

    void KVDB::Cleanup() { 
        sockets.at(0)->ClientSocketClose();
        sockets.at(1)->ClientSocketClose();
    }

    void KVDB::UpdateSendRate() {
        
        float loss_rate = 0;
        uint64_t pkts_sent_cur = pkts_sent - pkts_sent_prev;
        uint64_t pkts_recv_cur = pkts_recv - pkts_recv_prev;
        pkts_sent_prev = pkts_sent;
        pkts_recv_prev = pkts_recv;
        
        if (pkts_sent_cur > pkts_recv_cur) {
            loss_rate = (pkts_sent_cur - pkts_recv_cur) / (float)pkts_sent_cur;
        }

        if (loss_rate < loss_rate_min) {
            if (quick_start) {
                pkts_limit_us = pkts_limit_us * 2;
            }
            else if (loss_rate < 0.001 && pkts_limit_us > 10) {
                pkts_limit_us = pkts_limit_us * 1.1;
            }
            else if (pkts_limit_us > 20) {
                pkts_limit_us = pkts_limit_us * (1 + loss_rate_max);
            } 
            else {
                pkts_limit_us = pkts_limit_us + 1;
            }
        } else if (loss_rate > loss_rate_max) {
            if (quick_start) {
                pkts_limit_us = pkts_limit_us / 2;
                quick_start = false;
            }
            else {
                pkts_limit_us = pkts_limit_us * (1 - loss_rate);
            }
        }

        if (pkts_limit_us < pkts_limit_us_min) {
            pkts_limit_us = pkts_limit_us_min;
        }
    }


    std::tuple<uint32_t, uint64_t> KVDB::Read(const std::string &table, const std::vector<std::string> &keys,
                            const std::vector<std::string> *fields,
                            std::vector<std::vector<Field>> &results, uint32_t thread_id, int pipeline,
                            bool & migration_start, bool & migration_finish) { 
            

        high_resolution_clock::time_point t_last_adj, t_last_us;
        high_resolution_clock::duration t_elapsed;
        uint64_t pkts_sent_cur = 0;

        // construct READ packet
        struct mg_hdr_t mg_hdr;
        struct mg_read pkt_read;
        char buf[BUFSIZE];

        struct mg_hdr_t mg_hdr_reply;
        struct mg_read_reply pkt_read_reply;
        char buf_reply[BUFSIZE];
        int recvlen = -1;
        int retry_times = MAX_RETRY_TIMES;

        char val_pkt[VAL_SIZE + 1];

        t_last_adj = high_resolution_clock::now();
        t_last_us = high_resolution_clock::now();

        int i = 0;
        while (i < pipeline) {
            t_elapsed = high_resolution_clock::now() - t_last_adj;
            long time_us = duration_cast<std::chrono::microseconds>(t_elapsed).count();
            if(time_us > send_rate_adjust_period * FLAGS_tscale) {
                UpdateSendRate();
                t_last_adj = high_resolution_clock::now();
            }

            t_elapsed = high_resolution_clock::now() - t_last_us;
            long time_us_1 = duration_cast<std::chrono::microseconds>(t_elapsed).count();
            if(time_us_1 >= 1 * FLAGS_tscale) {
                pkts_sent_cur = 0;
                t_last_us = high_resolution_clock::now();
            }

            if (pkts_sent_cur <= pkts_limit_us) {
                // Randomly choose a redis instance, uniformly
                uint32_t redis_idx = 0; // thread_id % redis_ports.size(); // assume client thread_num is #redis multiplies
                uint16_t redis_port = redis_ports[redis_idx];

                std::string redis_key = table + "_" + keys[i];
                
                mg_hdr.op = _MG_READ;
                mg_hdr.seq = seq++;
                mg_hdr.dbPort = redis_port;
                pkt_read.bitmap = 0x1; // single read
                pkt_read.ver = 0;
                size_t len = redis_key.size();
                int pkt_idx = 0;
                for (size_t idx = len - KEY_SIZE; idx < len; idx++) {
                    pkt_read.key[pkt_idx++] = redis_key[idx];
                    assert(pkt_read.key[pkt_idx - 1] != 0);
                }

                SerializeMgHdr(buf, &mg_hdr);
                SerializeReadPkt(buf, &pkt_read);

                sockets.at(socket_id)->Send(buf, HDR_SIZE + sizeof(struct mg_read));
                i++;
                pkts_sent++;
                pkts_sent_cur++;
            }

        }

        bool switched = false;
        uint32_t pkt_loss = 0;
        uint32_t success = 0;
        for (int i = 0; i < pipeline; i++) {
            recvlen = sockets.at(socket_id)->Recv(buf_reply);
            if (recvlen == -1) {
                pkt_loss++;
                continue;
            }
            pkts_recv++;
            DeserializeMgHdr(buf_reply, &mg_hdr_reply);
            DeserializeReadReplyPkt(buf_reply, &pkt_read_reply);

            if (pkt_read_reply.ver & 128) {
                migration_start = true;
            }

            if (socket_id == 0 && (pkt_read_reply.ver == 255)) {
                // switch to destination redis agent
                // std::cout << "switch to desitation redis!" << std::endl;
                // std::cout << (uint32_t)pkt_read_reply.ver << std::endl;
                switched = true;
                migration_finish = true;
                continue;
            }

            assert(pkt_read_reply.bitmap == 0x1); // single op will not change bitmap in switch  
            assert(pkt_read_reply.seq == pkt_read.seq); 
            Field f;

            if (pkt_read_reply.ver & 1) {
                strncpy(val_pkt, pkt_read_reply.val, VAL_SIZE);
                val_pkt[VAL_SIZE] = 0;
                f.name = f.value = std::string(val_pkt);
                std::vector<DB::Field> result;
                result.push_back(f);
                results.push_back(result);
                success ++;
            }
        } 

        if (switched) {
            socket_id = 1;
            redis_ports.clear();
            std::copy(dst_redis_ports.begin(), dst_redis_ports.end(), std::back_inserter(redis_ports));
        }

        return std::make_tuple(success, 0);
    }

    DB::Status KVDB::Scan(const std::string &table, const std::string &key,
                int record_count, const std::vector<std::string> *fields,
                std::vector<std::vector<Field> > &result, uint32_t thread_id) {
        return DB::Status::kNotImplemented;
    }

    std::tuple<uint32_t, uint64_t> KVDB::Update(const std::string &table, const std::vector<std::string> &keys,
                  std::vector<std::vector<Field>> &values, uint32_t thread_id, int pipeline,
                  bool & migration_start, bool & migration_finish){
        

        high_resolution_clock::time_point t_last_adj, t_last_us;
        high_resolution_clock::duration t_elapsed;
        uint64_t pkts_sent_cur = 0;


        struct mg_hdr_t mg_hdr;
        struct mg_write pkt_write;
        char buf[BUFSIZE];

        struct mg_hdr_t mg_hdr_reply;
        struct mg_write_reply pkt_write_reply;
        char buf_reply[BUFSIZE];
        int recvlen = -1;
        int retry_times = MAX_RETRY_TIMES;

        size_t val_len = VAL_SIZE;

        t_last_adj = high_resolution_clock::now();
        t_last_us = high_resolution_clock::now();

        int i = 0;
        while (i < pipeline) {
            t_elapsed = high_resolution_clock::now() - t_last_adj;
            long time_us = duration_cast<std::chrono::microseconds>(t_elapsed).count();
            if(time_us > send_rate_adjust_period * FLAGS_tscale) {
                UpdateSendRate();
                t_last_adj = high_resolution_clock::now();
            }

            t_elapsed = high_resolution_clock::now() - t_last_us;
            long time_us_1 = duration_cast<std::chrono::microseconds>(t_elapsed).count();
            if(time_us_1 >= 1 * FLAGS_tscale) {
                pkts_sent_cur = 0;
                t_last_us = high_resolution_clock::now();
            }


            if (pkts_sent_cur <= pkts_limit_us) {
                // Randomly choose a redis instance, uniformly
                uint32_t redis_idx = thread_id % redis_ports.size();
                uint16_t redis_port = redis_ports[redis_idx];

                std::string redis_key = table + "_" + keys[i];
                std::string redis_val = "";
                
                // construct WRITE packet
                mg_hdr.op = _MG_WRITE;
                mg_hdr.seq = seq++;
                mg_hdr.dbPort = redis_port;
                pkt_write.bitmap = 0x1; // single write
                size_t len = redis_key.size();
                int pkt_idx = 0;
                for (size_t idx = len - KEY_SIZE; idx < len; idx++) {
                    pkt_write.key[pkt_idx++] = redis_key[idx];
                    assert(pkt_write.key[pkt_idx - 1] != 0);
                }

                auto value_ = values[i];
                for (auto &val: value_) {
                    redis_val += val.name + val.value;
                }
                
                val_len = std::min(VAL_SIZE, (int)redis_val.size());
                strncpy(pkt_write.val, redis_val.c_str(), val_len);

                SerializeMgHdr(buf, &mg_hdr);
                SerializeWritePkt(buf, &pkt_write);

                sockets.at(socket_id)->Send(buf, HDR_SIZE + sizeof(struct mg_write));
                i++;
                pkts_sent++;
                pkts_sent_cur++;
            }
        }

        bool switched = false;
        uint32_t pkt_loss = 0;
        uint32_t success = 0;
        for (int i = 0; i < pipeline; i++) {
            recvlen = sockets.at(socket_id)->Recv(buf_reply);
            if (recvlen == -1) {
                pkt_loss ++;
                continue;
            }
            pkts_recv++;
            DeserializeMgHdr(buf_reply, &mg_hdr_reply);
            DeserializeWriteReplyPkt(buf_reply, &pkt_write_reply);
            if (pkt_write_reply.ver & 128) {
                migration_start = true;
            }
            if (socket_id == 0 && (pkt_write_reply.ver == 255)) {
                // switch to destination redis agent
                switched = 1;
                migration_finish = true;
                continue;
            }
 
            assert(pkt_write_reply.bitmap == 0x1); // single op will not change bitmap in switch   
            assert(pkt_write_reply.seq == pkt_write.seq);
            success++;
        }

        if (switched) {
            socket_id = 1;
            redis_ports.clear();
            std::copy(dst_redis_ports.begin(), dst_redis_ports.end(), std::back_inserter(redis_ports));
        }
        return std::make_tuple(success, 0);
    }

    DB::Status KVDB::Insert(const std::string &table, const std::string &key,
                  std::vector<Field> &values, uint32_t thread_id, bool doload){

        // Randomly choose a redis instance, uniformly
        uint32_t redis_idx = thread_id % redis_ports.size();
        uint16_t redis_port = redis_ports[redis_idx];

        std::string redis_key = table + "_" + key;
        std::string redis_val = "";

        // construct WRITE packet
        struct mg_hdr_t mg_hdr;
        struct mg_write pkt_write;
        char buf[BUFSIZE];

        struct mg_hdr_t mg_hdr_reply;
        struct mg_write_reply pkt_write_reply;
        char buf_reply[BUFSIZE];
        int recvlen = -1;
        int retry_times = MAX_RETRY_TIMES;

        size_t val_len = VAL_SIZE;

        mg_hdr.op = _MG_WRITE;
        mg_hdr.seq = seq++;
        mg_hdr.dbPort = redis_port;
        pkt_write.bitmap = 0x1; // single write
        size_t len = redis_key.size();
        int pkt_idx = 0;
        for (size_t idx = len - KEY_SIZE; idx < len; idx++) {
            pkt_write.key[pkt_idx++] = redis_key[idx];
            assert(pkt_write.key[pkt_idx - 1] != 0);
        }

        for (auto &val: values) {
            redis_val += val.name + val.value;
        }
            
        val_len = std::min(VAL_SIZE, (int)redis_val.size());
        strncpy(pkt_write.val, redis_val.c_str(), val_len-1);
        pkt_write.val[val_len-1] = 0;

        SerializeMgHdr(buf, &mg_hdr);
        SerializeWritePkt(buf, &pkt_write);

        while (retry_times -- && recvlen < 0) {
            sockets.at(socket_id)->Send(buf, HDR_SIZE + sizeof(struct mg_write));
                
            if ((recvlen = sockets.at(socket_id)->Recv(buf_reply)) < 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }
            DeserializeMgHdr(buf_reply, &mg_hdr_reply);
            DeserializeWriteReplyPkt(buf_reply, &pkt_write_reply);
            assert(pkt_write_reply.bitmap == 0x1); // single op will not change bitmap in switch   
            assert(pkt_write_reply.seq == pkt_write.seq);
            return (pkt_write_reply.ver | 1) ? DB::Status::kOK : DB::Status::kNotFound;
        }   
        return DB::Status::kError;
    }

    DB::Status KVDB::Delete(const std::string &table, const std::string &key, uint32_t thread_id){
        return DB::Status::kNotImplemented;
    }

    DB *NewKVDB() {
        return new KVDB;
    }

    const bool registered = DBFactory::RegisterDB("KV", NewKVDB);

} // ycsbc