#include <sys/stat.h>
#include <cstring>
#include <thread>
#include <chrono>
#include <iostream>
#include <regex>

#include "kv_migration_db.h"

#include "../core/properties.h"
#include "../core/db_factory.h"

std::mutex MCR_update_mutex;
std::mutex SPH_update_mutex;

#define DEBUG 0

using namespace std::chrono;

namespace ycsbc {

    void KVDB::Init(uint32_t thread_id) {
        seq = 0;

        const utils::Properties& props =  *props_;

        std::string redis_port_list = props.GetProperty("src_redis.port_list");
        std::regex reg("[,]+");
        std::sregex_token_iterator iter(redis_port_list.begin(), redis_port_list.end(), reg, -1);
        std::sregex_token_iterator end;
        std::vector<std::string> src_str_redis_ports(iter, end);
        for (auto & port : src_str_redis_ports) {
            src_redis_ports.push_back(stoi(port));
        }

        redis_port_list = props.GetProperty("dst_redis.port_list");
        std::sregex_token_iterator iter_dst(redis_port_list.begin(), redis_port_list.end(), reg, -1);
        std::sregex_token_iterator end_dst;
        std::vector<std::string> dst_str_redis_ports(iter_dst, end_dst);
        for (auto & port : dst_str_redis_ports) {
            dst_redis_ports.push_back(stoi(port));
        }

        std::string src_agent_ip = props.GetProperty("src_agent.ip_addr");
        src_agent_start_port = stoi(props.GetProperty("src_agent.start_port"));

        std::string dst_agent_ip = props.GetProperty("dst_agent.ip_addr");
        dst_agent_start_port = stoi(props.GetProperty("dst_agent.start_port"));
        uint16_t client_thread_num = stoi(props.GetProperty("client.thread_num"));
        mcr_size = stoi(props.GetProperty("migration_thread_num"));
        std::string src_trans_ip = props.GetProperty("src_trans.ip_addr");
        std::string dst_trans_ip = props.GetProperty("dst_trans.ip_addr");
        this_thread_id = 0;

        uint16_t client_start_port = stoi(props.GetProperty("client.start_port"));

        decltype(uniform_distr.param()) new_range(0, src_redis_ports.size()-1);
        uniform_distr.param(new_range);
        

        std::cout << "source Redis address: tcp://" << src_agent_ip << ":";
        for (uint32_t i = 0; i < src_redis_ports.size(); i++) {
            std::cout << src_redis_ports[i] << ", " ;
        }
        std::cout << std::endl;

        std::cout << "destination Redis address: tcp://" << dst_agent_ip << ":";
        for (uint32_t i = 0; i < dst_redis_ports.size(); i++) {
            std::cout << dst_redis_ports[i] << ", " ;
        }
        std::cout << std::endl;

        std::cout << "KV Source Server Agent Address " << thread_id << ": " << src_trans_ip + ":" + std::to_string(src_agent_start_port + thread_id) << std::endl;
        sockets.emplace_back(std::make_shared<ClientSocket>(src_trans_ip.c_str(), src_agent_start_port + thread_id, client_start_port + thread_id));
        
        std::cout << "KV Destination Server Agent Address " << thread_id << ": " << dst_trans_ip + ":" + std::to_string(dst_agent_start_port + thread_id) << std::endl;
        sockets.emplace_back(std::make_shared<ClientSocket>(dst_trans_ip.c_str(), dst_agent_start_port + thread_id, client_start_port + client_thread_num + thread_id));

        under_migration = false;
        finish_migration = false;
        
        crc64_init();
        m_StartTime = std::chrono::system_clock::now();
    }

    void KVDB::Cleanup() { 
        sockets.at(0)->ClientSocketClose();
        sockets.at(1)->ClientSocketClose();
    }

    void KVDB::UpdateMCRBegin(uint32_t * mcr_begin) {
#if DEBUG == 1
        std::cout << "Updated MCR begin: " << std::endl;
#endif
        for (size_t i = 0; i < mcr_size; i++) {
            MCR_begin[i] = mcr_begin[i];
#if DEBUG == 1
            std::cout << mcr_begin[i] << " ";
#endif
        }
        assert(mcr_size > 1);
        dict_mask = (MCR_begin[mcr_size-1] + MCR_begin[1]) - 1;
#if DEBUG == 1
        std::cout << std::endl;
#endif
    }

    void KVDB::UpdateMCR(uint32_t * mcr) {
        {
            const std::lock_guard<std::mutex> lock(MCR_update_mutex);
#if DEBUG == 1
            std::cout << "Updated MCR: " << std::endl;
#endif
            for (size_t i = 0; i < mcr_size; i++) {
                if (MCR[i] <= mcr[i]) 
                    MCR[i] = mcr[i];
#if DEBUG == 1
                std::cout << MCR[i] << " ";
#endif
            }
#if DEBUG == 1
            std::cout << std::endl;
#endif
        }
    }   

    void KVDB::UpdateSPH(struct sph_metadata * sph_md) {
        {
            const std::lock_guard<std::mutex> lock(SPH_update_mutex);
#if DEBUG == 1
            std::cout << "Updated SPH" << std::endl;
#endif
            for (int i = 0; i < sph_md->num; i ++) {
                char * key = &sph_md->keys[i * KEY_SIZE];
                uint32_t key_int = (key[3] << 6) + (key[2] << 4) + (key[1] << 2) + key[0];
                SPH.insert(key_int); // SPH stores hot keys rather than hot key hash values
#if DEBUG == 1
                std::cout << key_int << " ";
#endif
            } 
#if DEBUG == 1
            std::cout << std::endl;
#endif
            // wait a timeout to update SPH based on MCR 
            auto endTime = std::chrono::system_clock::now();
            auto elapsed_time = std::chrono::duration_cast<std::chrono::seconds>(endTime - m_StartTime).count();
            if (elapsed_time >= SPH_ERASE_TIMEOUT) {
                std::vector<uint32_t> tmp_keys;
                for (auto iter = SPH.begin(); iter != SPH.end(); iter++) {
                    bool flag = false;
                    char key[KEY_SIZE];
                    uint32_t key_int = *iter;
                    for (size_t idx = 0; idx < KEY_SIZE; idx++) {
                        key[idx] = (char)(key_int >> (8 * idx)) & 0xFF;
                    }
                    uint32_t hash = dict_hash(key);
                    for (size_t i = 0; i < mcr_size; i++) {
                        if (hash >= MCR_begin[i] && hash <= MCR[i]) {
                            flag = true;
                            break;
                        }
                    }
                    if (flag) {
                        tmp_keys.emplace_back(*iter);
                    }
                }
                for (auto & key: tmp_keys) {
                    SPH.erase(key);
                }
                m_StartTime = std::chrono::system_clock::now();
            }
        }
    }

    uint32_t KVDB::dict_hash(char * key) {
        return crc64(0, (const unsigned char*) key, KEY_SIZE) & dict_mask;
    }

    bool KVDB::LookUpMCR_SPH(char * key) {
        uint32_t hash_value = dict_hash(key);
        for (size_t i = 0; i < mcr_size; i++) {
            if (hash_value >= MCR_begin[i] && hash_value <= MCR[i]) {
#if DEBUG == 1
                std::cout << "migrated: " << hash_value << " " << MCR_begin[i] << " " << MCR[i] << std::endl;
#endif
                return true;
            }
        }
        
#if DEBUG == 1
        /*
        {
            // debug
            const std::lock_guard<std::mutex> lock(MCR_update_mutex);
            for (size_t i = 0; i < mcr_size; i++) {
                std::cout << hash_value << " " << MCR_begin[i] << " " << MCR[i] << std::endl;
            }
            std::cout << std::endl;
        }
        */
#endif        

        
        uint32_t key_int = 0;
        for (int idx = KEY_SIZE-1; idx >= 0; idx--) {
            key_int += (key_int << 8) + (uint32_t)key[idx];
        }
        
        if (SPH.find(key_int) != SPH.end()) {
            return true;
        }
        return false;
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

        uint64_t extra_bandwidth = 0;
        uint64_t num_mcr = 0, num_sph = 0;
        
        high_resolution_clock::time_point t_last_adj, t_last_us;
        high_resolution_clock::duration t_elapsed;
        uint64_t pkts_sent_cur = 0;

        // construct READ packet
        struct mg_hdr_t mg_hdr;
        struct mg_read pkt_read;
        char buf[BUFSIZE];
        char buf_1[BUFSIZE];

        struct mg_hdr_t mg_hdr_reply, mg_hdr_reply_1;
        struct mg_read_reply pkt_read_reply, pkt_read_reply_1;
        char buf_reply[BUFSIZE], buf_reply_1[BUFSIZE];
        int recvlen = -1, recvlen_1 = 0;
        int retry_times = MAX_RETRY_TIMES;
        bool migrated = false;

        char val_pkt[VAL_SIZE + 1];

        uint32_t mcr[MAX_THREAD_NUM];
        uint32_t mcr_begin[MAX_THREAD_NUM];

        int decision[PIPELINE] = {0}; // 0: source, 1: destination, 2: both
        int d0 = 0, d1 = 0, d2 = 0;

        // choose a redis instance
        uint32_t redis_idx = 0; // thread_id % src_redis_ports.size(); // assume client thread_num is #redis multiplies
        uint16_t src_redis_port = src_redis_ports[redis_idx];
        uint16_t dst_redis_port = dst_redis_ports[redis_idx];

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


            if (pkts_sent_cur <= pkts_limit_us) 
            {
                std::string redis_key = table + "_" + keys[i];

                mg_hdr.op = _MG_READ;
                mg_hdr.seq = seq++;
                mg_hdr.dbPort = src_redis_port;
                pkt_read.bitmap = 0x1; // single read
                pkt_read.ver = 0; 
                size_t len = redis_key.size();
                int pkt_idx = 0;
                for (size_t idx = len - KEY_SIZE; idx < len; idx++) {
                    pkt_read.key[pkt_idx++] = redis_key[idx];
                    assert(pkt_read.key[pkt_idx] != 0);
                }
                SerializeMgHdr(buf, &mg_hdr);
                SerializeReadPkt(buf, &pkt_read);

                // Step 1: decide whether double read
                migrated = false;
                
                if (under_migration && !finish_migration) {
                    migrated = LookUpMCR_SPH(pkt_read.key);
                }

                if (finish_migration) 
                    migrated = true;

                if (!under_migration && !finish_migration) { // not start migration
                    mg_hdr.dbPort = src_redis_port;
                    sockets.at(0)->Send(buf, HDR_SIZE + sizeof(struct mg_read));
                    decision[i] = 0;
                    d0++;
                }
                else {
                    if ((under_migration && migrated) || finish_migration) {
                        mg_hdr.dbPort = dst_redis_port;
                        SerializeMgHdr(buf, &mg_hdr);
                        sockets.at(1)->Send(buf, HDR_SIZE + sizeof(struct mg_read));
                        decision[i] = 1;
                        d1++;
                    }
                    else { // not migrated or under migration keys, send READ requests to both source and destination
                        struct mg_hdr_t mg_hdr_1 = mg_hdr;
                        mg_hdr_1.dbPort = dst_redis_port;
                        SerializeMgHdr(buf_1, &mg_hdr_1);
                        pkt_read.ver = 0x4; // mark as double read
                        SerializeReadPkt(buf_1, &pkt_read);
                        sockets.at(0)->Send(buf, HDR_SIZE + sizeof(struct mg_read)); // source 
                        sockets.at(1)->Send(buf_1, HDR_SIZE + sizeof(struct mg_read)); // destination
                        decision[i] = 2;
                        d2++;
                    }
                }
                Field f;
                f.name = f.value = "";
                std::vector<DB::Field> result;
                result.push_back(f);
                results.push_back(result);
                i++;
                pkts_sent++;
                pkts_sent_cur++;
            }
        }

        // std::cout << "thread " << thread_id << " " << d0 << " " << d1 << " " << d2 << std::endl;


        uint32_t pkt_loss = 0;
        std::vector<bool> success_vec(pipeline, false);
        std::vector<bool> op_success_vec_0(pipeline, false);
        std::vector<bool> op_success_vec_1(pipeline, false);
        for (int i = 0; i < d0 + d2; i++) {
           
            recvlen = sockets.at(0)->Recv(buf_reply);

            if (recvlen == -1) {
                pkt_loss ++;
                continue;
            }
            success_vec.at(i) = true;
         
            DeserializeMgHdr(buf_reply, &mg_hdr_reply);
            DeserializeReadReplyPkt(buf_reply, &pkt_read_reply);
            Field f;

            if (pkt_read_reply.ver & 64) {
                under_migration = true;
                DeserializeReadReplyMCRPkt(buf_reply, mcr_begin, mcr_size);
                num_mcr += 1;
                UpdateMCRBegin(mcr_begin);
            }

            if (pkt_read_reply.ver & 1) {
                strncpy(val_pkt, pkt_read_reply.val, VAL_SIZE);
                val_pkt[VAL_SIZE] = 0;
                f.name = f.value = std::string(val_pkt);
                std::vector<DB::Field> result;
                result.push_back(f);
                results.at(i) = result;
                op_success_vec_0.at(i) = true;
            }
        }

        
        for (int i = 0; i < d1 + d2; i++) {
            recvlen = sockets.at(1)->Recv(buf_reply_1);
            if (recvlen == -1) {
                pkt_loss++;
                continue;
            }
    
            DeserializeMgHdr(buf_reply_1, &mg_hdr_reply_1);
            if (mg_hdr_reply_1.op != _MG_READ_REPLY) {
                continue;
            }
            DeserializeReadReplyPkt(buf_reply_1, &pkt_read_reply_1);

            success_vec.at(i) = true;
            assert(pkt_read_reply_1.bitmap == 0x1); // single op will not change bitmap in switch  
            Field f;

        
            if (under_migration && (pkt_read_reply_1.ver & 16)) {
                // update MCR based on piggybacked P_i, use fourth bit of version to indicate P_i field
                DeserializeReadReplyMCRPkt(buf_reply_1, mcr, mcr_size);
                // std::cout << (uint32_t)pkt_read_reply_1.ver <<  " " << recvlen << std::endl;
                num_mcr += 1;
                UpdateMCR(mcr);
            }
            else if (under_migration && (pkt_read_reply_1.ver & 32)) { 
                // update SPH based on piggybacked hash values
                struct sph_metadata sph_md;
                DeserializeReadReplySPHPkt(buf_reply_1, &sph_md);
                num_sph += 1;
                UpdateSPH(&sph_md);
            }   
            else if (under_migration && !finish_migration && (pkt_read_reply_1.ver & 128)) {
                under_migration = false;
                finish_migration = true;
            }

            if (pkt_read_reply_1.ver & 1) {
                strncpy(val_pkt, pkt_read_reply_1.val, VAL_SIZE);
                val_pkt[VAL_SIZE] = 0;
                f.name = f.value = std::string(val_pkt);
                std::vector<DB::Field> result;
                result.push_back(f);
                results.at(i) = result;
                op_success_vec_1.at(i) = true;
            }
        }
        
        uint32_t success = 0;
        uint32_t op_success = 0;
        for (int i = 0; i < pipeline; i++) {
            success += success_vec.at(i);
        }
        int op_idx_0 = 0, op_idx_1 = 0;
        for (int i = 0; i < pipeline; i++) {
            bool tmp = false;
            if (decision[i] == 0) {
                tmp = op_success_vec_0.at(op_idx_0);
                op_idx_0++;
            }
            else if (decision[i] == 1) {
                tmp = op_success_vec_1.at(op_idx_1);
                op_idx_1++;
            }
            else if (decision[i] == 2) {
                tmp = op_success_vec_0.at(op_idx_0) | op_success_vec_1.at(op_idx_1);
                op_idx_0++;
                op_idx_1++;
            }
            op_success += tmp;
        }
        pkts_recv = success;
        extra_bandwidth = (d0 + d1 + d2 * 2 - pipeline) * (KEY_SIZE + VAL_SIZE) + \
                          num_mcr * sizeof(uint32_t) * mcr_size + \
                          num_sph * sizeof(struct sph_metadata);
        return std::make_tuple(op_success, extra_bandwidth);
    }

    DB::Status KVDB::Scan(const std::string &table, const std::string &key,
                int record_count, const std::vector<std::string> *fields,
                std::vector<std::vector<Field> > &result, uint32_t thread_id) {
        return DB::Status::kNotImplemented;
    }

    std::tuple<uint32_t, uint64_t> KVDB::Update(const std::string &table, const std::vector<std::string> &keys,
                  std::vector<std::vector<Field>> &values, uint32_t thread_id, int pipeline,
                  bool & migration_start, bool & migration_finish){

        uint64_t num_mcr = 0, num_sph = 0;

        high_resolution_clock::time_point t_last_adj, t_last_us;
        high_resolution_clock::duration t_elapsed;
        uint64_t pkts_sent_cur = 0;

        // construct WRITE packet
        struct mg_hdr_t mg_hdr;
        struct mg_write pkt_write;
        char buf[BUFSIZE];

        struct mg_hdr_t mg_hdr_reply;
        struct mg_write_reply pkt_write_reply;
        char buf_reply[BUFSIZE];
        int recvlen = -1;
        int retry_times = MAX_RETRY_TIMES;

        uint32_t mcr[MAX_THREAD_NUM];
        uint32_t mcr_begin[MAX_THREAD_NUM];

        size_t val_len = VAL_SIZE;

        t_last_adj = high_resolution_clock::now();
        t_last_us = high_resolution_clock::now();

        int i = 0;
        int d0 = 0, d1 = 0;
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


            if (pkts_sent_cur <= pkts_limit_us) 
            {
                // Randomly choose a redis instance, uniformly
                uint32_t redis_idx = 0; // thread_id % src_redis_ports.size(); // assume client thread_num is #redis multiplies
                uint16_t dst_redis_port = dst_redis_ports[redis_idx];
                uint16_t src_redis_port = src_redis_ports[redis_idx];

                std::string redis_key = table + "_" + keys[i];
                std::string redis_val = "";

                mg_hdr.op = _MG_WRITE;
                mg_hdr.seq = seq++;
                mg_hdr.dbPort = dst_redis_port; // always write to destination
                pkt_write.bitmap = 0x1; // single write
                size_t len = redis_key.size();
                int pkt_idx = 0;
                for (size_t idx = len - KEY_SIZE; idx < len; idx++) {
                    pkt_write.key[pkt_idx++] = redis_key[idx];
                    assert(pkt_write.key[pkt_idx] != 0);
                }
                auto value_ = values[i];
                for (auto &val: value_) {
                    redis_val += val.name + val.value;
                }
                    
                val_len = std::min(VAL_SIZE, (int)redis_val.size());
                strncpy(pkt_write.val, redis_val.c_str(), val_len);

                SerializeMgHdr(buf, &mg_hdr);
                SerializeWritePkt(buf, &pkt_write);

                if (!under_migration && !finish_migration) {
                    mg_hdr.dbPort = src_redis_port;
                    SerializeMgHdr(buf, &mg_hdr);
                    sockets.at(0)->Send(buf, HDR_SIZE + sizeof(struct mg_write));
                    d0++;
                }
                else {
                    sockets.at(1)->Send(buf, HDR_SIZE + sizeof(struct mg_write));
                    d1++;
                }
                i++;
                pkts_sent++;
                pkts_sent_cur++;
            }
        }

        // std::cout << d0 << " " << d1 << std::endl;

        uint32_t pkt_loss = 0;
        for (int i = 0; i < d0; i++) {
            recvlen = sockets.at(0)->Recv(buf_reply);
            if (recvlen == -1) {
                pkt_loss++;
                continue;
            }
            pkts_recv++;
            DeserializeMgHdr(buf_reply, &mg_hdr_reply);
            DeserializeWriteReplyPkt(buf_reply, &pkt_write_reply);
            assert(pkt_write_reply.bitmap == 0x1); // single op will not change bitmap in switch   
            
            if (pkt_write_reply.ver & 64) {
                under_migration = true;
                DeserializeWriteReplyMCRPkt(buf_reply, mcr_begin, mcr_size);
                num_mcr += 1;
                UpdateMCRBegin(mcr_begin);
            }
        }

        for (int i = 0; i < d1; i++) {
            recvlen = sockets.at(1)->Recv(buf_reply);
            if (recvlen == -1) {
                pkt_loss++;
                continue;
            }
            pkts_recv++;
            DeserializeMgHdr(buf_reply, &mg_hdr_reply);
            if (mg_hdr_reply.op != _MG_WRITE_REPLY) {
                continue;
            }
            DeserializeWriteReplyPkt(buf_reply, &pkt_write_reply);
            assert(pkt_write_reply.bitmap == 0x1); // single op will not change bitmap in switch   
            
            if (under_migration && (pkt_write_reply.ver & 16)) {
                // update MCR based on piggybacked P_i, use fourth bit of version to indicate P_i field
                DeserializeWriteReplyMCRPkt(buf_reply, mcr, mcr_size);
                // std::cout << "Update: " << (uint32_t)pkt_write_reply.ver <<  " " << recvlen << std::endl;
                num_mcr += 1;
                UpdateMCR(mcr);
            }
            if (under_migration && (pkt_write_reply.ver & 32)) { 
                // update SPH based on piggybacked hash values
                struct sph_metadata sph_md;
                DeserializeWriteReplySPHPkt(buf_reply, &sph_md);
                num_sph += 1;
                UpdateSPH(&sph_md);
            }
        }
        
        uint64_t extra_bandwidth = num_mcr * sizeof(uint32_t) * mcr_size + \
                          num_sph * sizeof(struct sph_metadata);
        return std::make_tuple(pipeline - pkt_loss, extra_bandwidth);
    }

    DB::Status KVDB::Insert(const std::string &table, const std::string &key,
                  std::vector<Field> &values, uint32_t thread_id, bool doload){

        uint64_t num_mcr = 0, num_sph = 0;

        uint32_t mcr[MAX_THREAD_NUM];
        uint32_t mcr_begin[MAX_THREAD_NUM];

        // Randomly choose a redis instance, uniformly
        uint32_t redis_idx = 0; //thread_id % src_redis_ports.size(); // assume client thread_num is #redis multiplies
        uint16_t dst_redis_port = dst_redis_ports[redis_idx];

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
        mg_hdr.dbPort = dst_redis_port;
        pkt_write.bitmap = 0x1; // single write
        size_t len = redis_key.size();
        int pkt_idx = 0;
        for (size_t idx = len - KEY_SIZE; idx < len; idx++) {
            pkt_write.key[pkt_idx++] = redis_key[idx];
            assert(pkt_write.key[pkt_idx] != 0);
        }
        for (auto &val: values) {
            redis_val += val.name + val.value;
        }
        
        val_len = std::min(VAL_SIZE, (int)redis_val.size());
        strncpy(pkt_write.val, redis_val.c_str(), val_len);

        SerializeMgHdr(buf, &mg_hdr);
        SerializeWritePkt(buf, &pkt_write);

        while (retry_times -- && recvlen < 0) {
            sockets.at(1)->Send(buf, HDR_SIZE + sizeof(struct mg_write));
                
            if ((recvlen = sockets.at(1)->Recv(buf_reply)) < 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }
            DeserializeMgHdr(buf_reply, &mg_hdr_reply);
            DeserializeWriteReplyPkt(buf_reply, &pkt_write_reply);
            assert(pkt_write_reply.bitmap == 0x1); // single op will not change bitmap in switch 
         
            if (pkt_write_reply.ver & 64) {
                under_migration = true;
                DeserializeWriteReplyMCRPkt(buf_reply, mcr_begin, mcr_size);
                num_mcr += 1;
                UpdateMCRBegin(mcr_begin);
                return DB::Status::kError;
            }
            if (under_migration && (pkt_write_reply.ver & 16)) {
                // update MCR based on piggybacked P_i, use fourth bit of version to indicate P_i field
                DeserializeWriteReplyMCRPkt(buf_reply, mcr, mcr_size);
                std::cout << "Insert: " << std::endl;
                num_mcr += 1;
                UpdateMCR(mcr);
            }
            if (under_migration && (pkt_write_reply.ver & 32)) { 
                // update SPH based on piggybacked hash values
                struct sph_metadata sph_md;
                DeserializeWriteReplySPHPkt(buf_reply, &sph_md);
                num_sph += 1;
                UpdateSPH(&sph_md);
            }
            return (pkt_write_reply.ver | 1) ? DB::Status::kOK : DB::Status::kNotFound;
        }   
        return DB::Status::kError;
    }

    DB::Status KVDB::Delete(const std::string &table, const std::string &key, uint32_t thread_id){
        
        uint64_t num_mcr = 0, num_sph = 0;

        uint32_t mcr[MAX_THREAD_NUM];
        uint32_t mcr_begin[MAX_THREAD_NUM];

        // Randomly choose a redis instance, uniformly
        uint32_t redis_idx = 0; //thread_id % src_redis_ports.size(); // assume client thread_num is #redis multiplies
        uint16_t dst_redis_port = dst_redis_ports[redis_idx];

        std::string redis_key = table + "_" + key;

        // construct DELETE packet
        struct mg_hdr_t mg_hdr;
        struct mg_delete pkt_delete;
        char buf[BUFSIZE];
        
        struct mg_hdr_t mg_hdr_reply;
        struct mg_delete_reply pkt_delete_reply;
        char buf_reply[BUFSIZE];
        int recvlen = -1;
        int retry_times = MAX_RETRY_TIMES;

        mg_hdr.op = _MG_DELETE;
        mg_hdr.seq = seq++;
        mg_hdr.dbPort = dst_redis_port;
        pkt_delete.bitmap = 0x1; // single delete
        size_t len = redis_key.size();
        int pkt_idx = 0;
        for (size_t idx = len - KEY_SIZE; idx < len; idx++) {
            pkt_delete.key[pkt_idx++] = redis_key[idx];
            assert(pkt_delete.key[pkt_idx] != 0);
        }

        SerializeMgHdr(buf, &mg_hdr);
        SerializeDeletePkt(buf, &pkt_delete);
        
        while (retry_times -- && recvlen < 0) {
            sockets.at(1)->Send(buf, HDR_SIZE + sizeof(struct mg_delete));

            if ((recvlen = sockets.at(1)->Recv(buf_reply)) < 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }
            DeserializeMgHdr(buf_reply, &mg_hdr_reply);
            DeserializeDeleteReplyPkt(buf, &pkt_delete_reply);
            assert(pkt_delete_reply.bitmap == 0x1); // single op will not change bitmap in switch   

            if (pkt_delete_reply.ver & 64) {
                under_migration = true;
                DeserializeDeleteReplyMCRPkt(buf_reply, mcr_begin, mcr_size);
                num_mcr += 1;
                UpdateMCRBegin(mcr_begin);
                return DB::Status::kError;
            }
            if (under_migration && (pkt_delete_reply.ver & 16)) {
                // update MCR based on piggybacked P_i, use fourth bit of version to indicate P_i field
                DeserializeDeleteReplyMCRPkt(buf_reply, mcr, mcr_size);

                // std::cout << "Delete: " << std::endl;
                num_mcr += 1;
                UpdateMCR(mcr);
            }
            if (under_migration && (pkt_delete_reply.ver & 32)) { 
                // update SPH based on piggybacked hash values
                struct sph_metadata sph_md;
                DeserializeDeleteReplySPHPkt(buf_reply, &sph_md);
                num_sph += 1;
                UpdateSPH(&sph_md);
            }
            return (pkt_delete_reply.ver | 1) ? DB::Status::kOK : DB::Status::kNotFound;
        }
        return DB::Status::kError;
    }

    DB *NewKVDB() {
        return new KVDB;
    }

    const bool registered = DBFactory::RegisterDB("KV", NewKVDB);

} // ycsbc