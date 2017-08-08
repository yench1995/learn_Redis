#ifndef __REDIS_RIO_H
#define __REDIS_RIO_H

#include <stdio.h>
#include <stdint.h>
#include "sds.h"

//RIO API接口和状态
struct _rio {
    //API
    size_t (*read)(struct _rio *, void *buf, size_t len);
    size_t (*write)(struct _rio *, const void *buf, size_t len);
    off_t (*tell)(struct _rio *);

    //检验和计算函数，每次有写入/读取新数据时都要计算一次
    void (*update_cksum)(struct _rio *, const void *buf, size_t len);

    //当前检验和
    uint64_t cksum;

    //已读取或写入的字节数
    size_t processed_bytes;

    //最大单词读取或写入块
    size_t max_processing_chunk;

    //Backend-specific vars
    union {
        struct {
            //缓存指针
            sds ptr;
            //偏移量
            off_t pos;
        } buffer;

        struct {
            //被打开文件的指针
            FILE *fp;
            //最近一次fsync以来，写入的字节量
            off_t buffered;
            //写入多少字节以后，才会自动执行一次fsync
            off_t autosync;
        } file;
    } io;

};

typedef struct _rio rio;

/*
 * 将buf中的len字节写入到r中
 * 写入成功返回实际写入的字节数，写入失败返回-1
 */
static inline size_t rioWrite(rio *r, const void *buf, size_t len) {
    while (len) {
        size_t bytes_to_write = (r->max_processing_chunk && r->max_processing_chunk < len) ? r->max_processing_chunk : len;
        if (r->update_cksum) r->update_cksum(r, buf, bytes_to_write);
        if (r->write(r, buf, bytes_to_write) == 0)
            return 0;
        buf = (char *)buf + bytes_to_write;
        len -= bytes_to_write;
        r->processed_bytes += bytes_to_write;
    }
    return 1;
}

/*
 * 从r中读取len字节，并将内容保存到buf中
 * 读取成功放那会1,失败返回0
 */
static inline size_t rioRead(rio *r, void *buf, size_t len) {
    while (len) {
        size_t bytes_to_read= (r->max_processing_chunk && r->max_processing_chunk < len) ? r->max_processing_chunk : len;
        if (r->read(r, buf, bytes_to_read) == 0)
            return 0;
        if (r->update_cksum) r->update_cksum(r, buf, bytes_to_read);
        buf = (char*)buf + bytes_to_read;
        len -= bytes_to_read;
        r->processed_bytes += bytes_to_read;
    }
    return 1;
}

//返回r的当前偏移量
static inline off_t rioTell(rio *r) {
    return r->tell(r);
}

void rioInitWithFile(rio *r, FILE *fp);
void rioInitWithBuffer(rio *r, sds s);

size_t rioWriteBulkCount(rio *r, char prefix, int count);
size_t rioWriteBulkString(rio *r, const char *buf, size_t len);
size_t rioWriteBulkLongLong(rio *r, long long l);
size_t rioWriteBulkDouble(rio *r, double d);

void rioGenericUpdateChecksum(rio *r, const void *buf, size_t len);
void rioSetAutoSync(rio *r, off_t bytes);

#endif
