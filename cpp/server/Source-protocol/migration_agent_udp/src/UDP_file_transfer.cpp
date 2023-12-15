#include "UDP_file_transfer.h"

void set_seq(char * buf, uint64_t seq) {
    seq = htonll(seq);
    memcpy(buf, &seq, SEQ_SIZE);
}

uint64_t get_seq(const char * buf) {
    uint64_t seq;
    memcpy(&seq, buf, SEQ_SIZE);
    seq = ntohll(seq);
    return seq;
}

void Sender::SendFileInfo(std::string file_path, uint64_t file_size) {
    
    this->file_path = file_path;
    for (uint32_t i = 0; i < thread_num; i++) {
        file_streams.at(i).open(file_path, std::ifstream::in);
    }

    char buf[BUFSIZE], buf_reply[BUFSIZE];
    set_seq(buf, file_size);
    sockets.at(0)->Send(buf, SEQ_SIZE);
    sockets.at(0)->Recv(buf_reply);
}

void Sender::SendFileThread(uint32_t thread_id, uint64_t begin_offset, uint64_t end_offset) {
    file_streams.at(thread_id).seekg(begin_offset, std::ios::beg);

    uint64_t offset = begin_offset, size = 0;
    char buffer[BUFSIZE], buf_reply[BUFSIZE];
    uint64_t seq = 0, seq_reply = 0;

    while (offset < end_offset) {
        size = std::min((uint64_t)(BUFSIZE - SEQ_SIZE), end_offset - offset); // right open interval
        if (file_streams.at(thread_id).read(buffer + SEQ_SIZE, size)) {
            set_seq(buffer, seq);
            seq_reply = -1;
            while (seq_reply != seq) {
                sockets.at(thread_id)->Send(buffer, size + SEQ_SIZE);
                int recvlen = sockets.at(thread_id)->Recv(buf_reply);
                if (recvlen == -1) 
                    continue;
                seq_reply = get_seq(buf_reply);
            }
        }
        offset += size;
        seq += size;
    }  
}

void Sender::SendFileComplete() {
    for (uint32_t i = 0; i < thread_num; i++) {
        file_streams.at(i).close();
    }
}

void Sender::SendFile(std::string file_path, uint64_t begin_offset, uint64_t end_offset) {
    uint64_t tot_file_size = end_offset - begin_offset; 
    SendFileInfo(file_path, tot_file_size);

    for (uint32_t i = 0; i < thread_num; i++) {
        seg_file_size[i] = tot_file_size / thread_num;
    }

    for (uint32_t i = 0; i < tot_file_size % thread_num; i++) {
        seg_file_size[i] += 1;
    }

    begin_off[0] = begin_offset;
    for (uint32_t i = 1; i < thread_num; i++) {
        begin_off[i] = begin_off[i-1] + seg_file_size[i-1];
        end_off[i-1] = begin_off[i];
    }
    end_off[thread_num - 1] = end_offset;
    
    thread_send.clear();
    for (uint32_t i = 0; i < thread_num; i++) {
        thread_send.emplace_back(&Sender::SendFileThread, this, i, begin_off[i], end_off[i]);
    }

    for (uint32_t i = 0; i < thread_num; i++) {
        thread_send.at(i).join();
    }

    SendFileComplete();
}



void Receiver::ReceiveFileInfo(std::string & file_path, uint64_t & file_size) {
    for (uint32_t i = 0; i < thread_num; i++) {
        file_streams.at(i).open(file_path, std::ofstream::out);
        recv_term[i] = false;
    }

    char buf[BUFSIZE];
    sockets.at(0)->Recv(buf);
    file_size = get_seq(buf);
    sockets.at(0)->Send(buf, SEQ_SIZE);
}

void Receiver::ReceiveFileThread(uint32_t thread_id, uint64_t begin_offset, uint64_t seg_file_size) {
    
    uint64_t recv_size = 0, size = 0;
    char buf[BUFSIZE], buf_reply[BUFSIZE];
    int recvlen = -1;
    uint64_t seq = 0;
    buf_q recv_buf_q;

    while (recv_size < seg_file_size) {
        recvlen = sockets.at(thread_id)->Recv(buf);
        seq = get_seq(buf);
        if (recv_size == seq) {
            recv_size += recvlen - SEQ_SIZE;
            
            recv_buf_q.len = recvlen - SEQ_SIZE;
            memcpy(recv_buf_q.buf, buf + SEQ_SIZE, recv_buf_q.len);
            write_buf[thread_id].enqueue(recv_buf_q);
            set_seq(buf_reply, seq);
            sockets.at(thread_id)->Send(buf_reply, SEQ_SIZE);
        }
        // else drop the packet
    }
    recv_term[thread_id] = true;
}

void Receiver::WriteFileThread(uint32_t thread_id, uint64_t begin_offset) {
    uint64_t q_size = write_buf[thread_id].size_approx();
    uint64_t dequeue_count = 0;
    buf_q write_buf_q;

    file_streams.at(thread_id).seekp(begin_offset);
    while (!recv_term[thread_id] || q_size) {
        bool res = write_buf[thread_id].try_dequeue(write_buf_q);
        if (res) {
            
            file_streams.at(thread_id).write(write_buf_q.buf, write_buf_q.len);
        }
        q_size = write_buf[thread_id].size_approx();
    }
}

void Receiver::ReceiveFileComplete() {
    for (uint32_t i = 0; i < thread_num; i++) {
        file_streams.at(i).close();
    }
}

void Receiver::ReceiveFile(std::string file_path, uint64_t & recv_file_size) {
    
    uint64_t tot_file_size = 0;
    ReceiveFileInfo(file_path, tot_file_size);
    recv_file_size = tot_file_size;

    for (uint32_t i = 0; i < thread_num; i++) {
        seg_file_size[i] = tot_file_size / thread_num;
    }

    for (uint32_t i = 0; i < tot_file_size % thread_num; i++) {
        seg_file_size[i] += 1;
    }

    begin_off[0] = 0;
    for (uint32_t i = 1; i < thread_num; i++) {
        begin_off[i] = begin_off[i-1] + seg_file_size[i-1];
    }
    

    thread_recv.clear();
    for (uint32_t i = 0; i < thread_num; i++) {
        thread_recv.emplace_back(&Receiver::ReceiveFileThread, this, i, begin_off[i], seg_file_size[i]);
    }

    thread_write.clear();
    for (uint32_t i = 0; i < thread_num; i++) {
        thread_write.emplace_back(&Receiver::WriteFileThread, this, i, begin_off[i]);
    }

    for (uint32_t i = 0; i < thread_num; i++) {
        thread_recv.at(i).join();
        thread_write.at(i).join();
    }

    ReceiveFileComplete();

}