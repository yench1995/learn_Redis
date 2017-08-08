#include "redis.h"
#include "bio.h"
#include "rio.h"

#include <signal.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>

void aofUpdateCurrentSize(void);

/* ----------------------------------------------------------------------------
 * AOF rewrite buffer implementation.
 *
 * AOF 重写缓存的实现。
 *
 * The following code implement a simple buffer used in order to accumulate
 * changes while the background process is rewriting the AOF file.
 *
 * 以下代码实现了一个简单的缓存，
 * 它可以在 BGREWRITEAOF 执行的过程中，累积所有修改数据集的命令。
 *
 * We only need to append, but can't just use realloc with a large block
 * because 'huge' reallocs are not always handled as one could expect
 * (via remapping of pages at OS level) but may involve copying data.
 *
 * For this reason we use a list of blocks, every block is
 * AOF_RW_BUF_BLOCK_SIZE bytes.
 *
 * 程序需要不断对这个缓存执行 append 操作，
 * 因为分配一个非常大的空间并不总是可能的，也可能产生大量的复制工作，
 * 所以这里使用多个大小为 AOF_RW_BUF_BLOCK_SIZE 的空间来保存命令。
 *
 * ------------------------------------------------------------------------- */

// 每个缓存块的大小
#define AOF_RW_BUF_BLOCK_SIZE (1024*1024*10)    /* 10 MB per block */

typedef struct aofrwblock {
    
    // 缓存块已使用字节数和可用字节数
    unsigned long used, free;

    // 缓存块
    char buf[AOF_RW_BUF_BLOCK_SIZE];

} aofrwblock;

/*
 * 释放旧的 AOF 重写缓存，并初始化一个新的 AOF 缓存。
 * 这个函数也可以单纯地用于 AOF 重写缓存的初始化。
 */
void aofRewriteBufferReset(void) {

    // 释放旧有的缓存（链表）
    if (server.aof_rewrite_buf_blocks)
        listRelease(server.aof_rewrite_buf_blocks);

    // 初始化新的缓存（链表）
    server.aof_rewrite_buf_blocks = listCreate();
    listSetFreeMethod(server.aof_rewrite_buf_blocks,zfree);
}

//返回AOF重写缓存当前的大小
unsigned long aofRewriteBufferSize(void) {

    // 取出链表中最后的缓存块
    listNode *ln = listLast(server.aof_rewrite_buf_blocks);
    aofrwblock *block = ln ? ln->value : NULL;

    // 没有缓存被使用
    if (block == NULL) return 0;

    // 总缓存大小 = （缓存块数量-1） * AOF_RW_BUF_BLOCK_SIZE + 最后一个缓存块的大小
    unsigned long size =
        (listLength(server.aof_rewrite_buf_blocks)-1) * AOF_RW_BUF_BLOCK_SIZE;
    size += block->used;

    return size;
}

/*
 * 将字符数组 s 追加到 AOF 缓存的末尾，
 * 如果有需要的话，分配一个新的缓存块。
 */
void aofRewriteBufferAppend(unsigned char *s, unsigned long len) {

    // 指向最后一个缓存块
    listNode *ln = listLast(server.aof_rewrite_buf_blocks);
    aofrwblock *block = ln ? ln->value : NULL;

    while(len) {
        /* If we already got at least an allocated block, try appending
         * at least some piece into it. 
         *
         * 如果已经有至少一个缓存块，那么尝试将内容追加到这个缓存块里面
         */
        if (block) {
            unsigned long thislen = (block->free < len) ? block->free : len;
            if (thislen) {  /* The current block is not already full. */
                memcpy(block->buf+block->used, s, thislen);
                block->used += thislen;
                block->free -= thislen;
                s += thislen;
                len -= thislen;
            }
        }

        // 如果 block != NULL ，那么这里是创建另一个缓存块买容纳 block 装不下的内容
        // 如果 block == NULL ，那么这里是创建缓存链表的第一个缓存块
        if (len) { /* First block to allocate, or need another block. */
            int numblocks;

            // 分配缓存块
            block = zmalloc(sizeof(*block));
            block->free = AOF_RW_BUF_BLOCK_SIZE;
            block->used = 0;

            // 链接到链表末尾
            listAddNodeTail(server.aof_rewrite_buf_blocks,block);

            /* Log every time we cross more 10 or 100 blocks, respectively
             * as a notice or warning. 
             *
             * 每次创建 10 个缓存块就打印一个日志，用作标记或者提醒
             */
            numblocks = listLength(server.aof_rewrite_buf_blocks);
            if (((numblocks+1) % 10) == 0) {
                int level = ((numblocks+1) % 100) == 0 ? REDIS_WARNING :
                                                         REDIS_NOTICE;
                redisLog(level,"Background AOF buffer size: %lu MB",
                    aofRewriteBufferSize()/(1024*1024));
            }
        }
    }
}

/*
 * 将重写缓存中的所有内容（可能由多个块组成）写入到给定 fd 中。
 *
 * 如果没有 short write 或者其他错误发生，那么返回写入的字节数量，
 * 否则，返回 -1 。
 */
ssize_t aofRewriteBufferWrite(int fd) {
    listNode *ln;
    listIter li;
    ssize_t count = 0;

    // 遍历所有缓存块
    listRewind(server.aof_rewrite_buf_blocks,&li);
    while((ln = listNext(&li))) {
        aofrwblock *block = listNodeValue(ln);
        ssize_t nwritten;

        if (block->used) {

            // 写入缓存块内容到 fd
            nwritten = write(fd,block->buf,block->used);
            if (nwritten != block->used) {
                if (nwritten == 0) errno = EIO;
                return -1;
            }

            // 积累写入字节
            count += nwritten;
        }
    }

    return count;
}

//在另一个线程中，对给定的描述符fd(指向AOF文件)执行一个后台fsync()操作
void aof_background_fsync(int fd) {
    bioCreateBackgroundJob(REDIS_BIO_AOF_FSYNC,(void*)(long)fd,NULL,NULL);
}

//在用户通过CONFIG命令在运行时关闭AOF持久化调用
void stopAppendOnly(void) {

    // AOF 必须正在启用，才能调用这个函数
    redisAssert(server.aof_state != REDIS_AOF_OFF);

    // 将 AOF 缓存的内容写入并冲洗到 AOF 文件中
    // 参数 1 表示强制模式
    flushAppendOnlyFile(1);

    // 冲洗 AOF 文件
    aof_fsync(server.aof_fd);

    // 关闭 AOF 文件
    close(server.aof_fd);

    // 清空 AOF 状态
    server.aof_fd = -1;
    server.aof_selected_db = -1;
    server.aof_state = REDIS_AOF_OFF;

    /* rewrite operation in progress? kill it, wait child exit 
    *
    * 如果 BGREWRITEAOF 正在执行，那么杀死它
    * 并等待子进程退出
    */
    if (server.aof_child_pid != -1) {
        int statloc;

        redisLog(REDIS_NOTICE,"Killing running AOF rewrite child: %ld",
            (long) server.aof_child_pid);

        // 杀死子进程
        if (kill(server.aof_child_pid,SIGUSR1) != -1)
            wait3(&statloc,0,NULL);

        /* reset the buffer accumulating changes while the child saves 
         * 清理未完成的 AOF 重写留下来的缓存和临时文件
         */
        aofRewriteBufferReset();
        aofRemoveTempFile(server.aof_child_pid);
        server.aof_child_pid = -1;
        server.aof_rewrite_time_start = -1;
    }
}

/*
 * 当用户在运行时使用 CONFIG 命令，
 * 从 appendonly no 切换到 appendonly yes 时执行
 */
int startAppendOnly(void) {

    // 将开始时间设为 AOF 最后一次 fsync 时间 
    server.aof_last_fsync = server.unixtime;

    // 打开 AOF 文件
    server.aof_fd = open(server.aof_filename,O_WRONLY|O_APPEND|O_CREAT,0644);

    redisAssert(server.aof_state == REDIS_AOF_OFF);

    // 文件打开失败
    if (server.aof_fd == -1) {
        redisLog(REDIS_WARNING,"Redis needs to enable the AOF but can't open the append only file: %s",strerror(errno));
        return REDIS_ERR;
    }

    if (rewriteAppendOnlyFileBackground() == REDIS_ERR) {
        // AOF 后台重写失败，关闭 AOF 文件
        close(server.aof_fd);
        redisLog(REDIS_WARNING,"Redis needs to enable the AOF but can't trigger a background AOF rewrite operation. Check the above logs for more info about the error.");
        return REDIS_ERR;
    }

    /* We correctly switched on AOF, now wait for the rerwite to be complete
     * in order to append data on disk. 
     *
     * 等待重写执行完毕
     */
    server.aof_state = REDIS_AOF_WAIT_REWRITE;

    return REDIS_OK;
}

/*
 * 当用户在运行时使用 CONFIG 命令，
 * 从 appendonly no 切换到 appendonly yes 时执行
 */
int startAppendOnly(void) {

    // 将开始时间设为 AOF 最后一次 fsync 时间 
    server.aof_last_fsync = server.unixtime;

    // 打开 AOF 文件
    server.aof_fd = open(server.aof_filename,O_WRONLY|O_APPEND|O_CREAT,0644);

    redisAssert(server.aof_state == REDIS_AOF_OFF);

    // 文件打开失败
    if (server.aof_fd == -1) {
        redisLog(REDIS_WARNING,"Redis needs to enable the AOF but can't open the append only file: %s",strerror(errno));
        return REDIS_ERR;
    }

    if (rewriteAppendOnlyFileBackground() == REDIS_ERR) {
        // AOF 后台重写失败，关闭 AOF 文件
        close(server.aof_fd);
        redisLog(REDIS_WARNING,"Redis needs to enable the AOF but can't trigger a background AOF rewrite operation. Check the above logs for more info about the error.");
        return REDIS_ERR;
    }

    /* We correctly switched on AOF, now wait for the rerwite to be complete
     * in order to append data on disk. 
     *
     * 等待重写执行完毕
     */
    server.aof_state = REDIS_AOF_WAIT_REWRITE;

    return REDIS_OK;
}

/* Write the append only file buffer on disk.
 *
 * 将 AOF 缓存写入到文件中。
 *
 * Since we are required to write the AOF before replying to the client,
 * and the only way the client socket can get a write is entering when the
 * the event loop, we accumulate all the AOF writes in a memory
 * buffer and write it on disk using this function just before entering
 * the event loop again.
 *
 * 因为程序需要在回复客户端之前对 AOF 执行写操作。
 * 而客户端能执行写操作的唯一机会就是在事件 loop 中，
 * 因此，程序将所有 AOF 写累积到缓存中，
 * 并在重新进入事件 loop 之前，将缓存写入到文件中。
 *
 * About the 'force' argument:
 *
 * 关于 force 参数：
 *
 * When the fsync policy is set to 'everysec' we may delay the flush if there
 * is still an fsync() going on in the background thread, since for instance
 * on Linux write(2) will be blocked by the background fsync anyway.
 *
 * 当 fsync 策略为每秒钟保存一次时，如果后台线程仍然有 fsync 在执行，
 * 那么我们可能会延迟执行冲洗（flush）操作，
 * 因为 Linux 上的 write(2) 会被后台的 fsync 阻塞。
 *
 * When this happens we remember that there is some aof buffer to be
 * flushed ASAP, and will try to do that in the serverCron() function.
 *
 * 当这种情况发生时，说明需要尽快冲洗 aof 缓存，
 * 程序会尝试在 serverCron() 函数中对缓存进行冲洗。
 *
 * However if force is set to 1 we'll write regardless of the background
 * fsync. 
 *
 * 不过，如果 force 为 1 的话，那么不管后台是否正在 fsync ，
 * 程序都直接进行写入。
 */
#define AOF_WRITE_LOG_ERROR_RATE 30 /* Seconds between errors logging. */
void flushAppendOnlyFile(int force) {
    ssize_t nwritten;
    int sync_in_progress = 0;

    // 缓冲区中没有任何内容，直接返回
    if (sdslen(server.aof_buf) == 0) return;

    // 策略为每秒 FSYNC 
    if (server.aof_fsync == AOF_FSYNC_EVERYSEC)
        // 是否有 SYNC 正在后台进行？
        sync_in_progress = bioPendingJobsOfType(REDIS_BIO_AOF_FSYNC) != 0;

    // 每秒 fsync ，并且强制写入为假
    if (server.aof_fsync == AOF_FSYNC_EVERYSEC && !force) {

        /* With this append fsync policy we do background fsyncing.
         *
         * 当 fsync 策略为每秒钟一次时， fsync 在后台执行。
         *
         * If the fsync is still in progress we can try to delay
         * the write for a couple of seconds. 
         *
         * 如果后台仍在执行 FSYNC ，那么我们可以延迟写操作一两秒
         * （如果强制执行 write 的话，服务器主线程将阻塞在 write 上面）
         */
        if (sync_in_progress) {

            // 有 fsync 正在后台进行 。。。

            if (server.aof_flush_postponed_start == 0) {
                /* No previous write postponinig, remember that we are
                 * postponing the flush and return. 
                 *
                 * 前面没有推迟过 write 操作，这里将推迟写操作的时间记录下来
                 * 然后就返回，不执行 write 或者 fsync
                 */
                server.aof_flush_postponed_start = server.unixtime;
                return;

            } else if (server.unixtime - server.aof_flush_postponed_start < 2) {
                /* We were already waiting for fsync to finish, but for less
                 * than two seconds this is still ok. Postpone again. 
                 *
                 * 如果之前已经因为 fsync 而推迟了 write 操作
                 * 但是推迟的时间不超过 2 秒，那么直接返回
                 * 不执行 write 或者 fsync
                 */
                return;

            }

            /* Otherwise fall trough, and go write since we can't wait
             * over two seconds. 
             *
             * 如果后台还有 fsync 在执行，并且 write 已经推迟 >= 2 秒
             * 那么执行写操作（write 将被阻塞）
             */
            server.aof_delayed_fsync++;
            redisLog(REDIS_NOTICE,"Asynchronous AOF fsync is taking too long (disk is busy?). Writing the AOF buffer without waiting for fsync to complete, this may slow down Redis.");
        }
    }

    /* If you are following this code path, then we are going to write so
     * set reset the postponed flush sentinel to zero. 
     *
     * 执行到这里，程序会对 AOF 文件进行写入。
     *
     * 清零延迟 write 的时间记录
     */
    server.aof_flush_postponed_start = 0;

    /* We want to perform a single write. This should be guaranteed atomic
     * at least if the filesystem we are writing is a real physical one.
     *
     * 执行单个 write 操作，如果写入设备是物理的话，那么这个操作应该是原子的
     *
     * While this will save us against the server being killed I don't think
     * there is much to do about the whole server stopping for power problems
     * or alike 
     *
     * 当然，如果出现像电源中断这样的不可抗现象，那么 AOF 文件也是可能会出现问题的
     * 这时就要用 redis-check-aof 程序来进行修复。
     */
    nwritten = write(server.aof_fd,server.aof_buf,sdslen(server.aof_buf));
    if (nwritten != (signed)sdslen(server.aof_buf)) {

        static time_t last_write_error_log = 0;
        int can_log = 0;

        /* Limit logging rate to 1 line per AOF_WRITE_LOG_ERROR_RATE seconds. */
        // 将日志的记录频率限制在每行 AOF_WRITE_LOG_ERROR_RATE 秒
        if ((server.unixtime - last_write_error_log) > AOF_WRITE_LOG_ERROR_RATE) {
            can_log = 1;
            last_write_error_log = server.unixtime;
        }

        /* Lof the AOF write error and record the error code. */
        // 如果写入出错，那么尝试将该情况写入到日志里面
        if (nwritten == -1) {
            if (can_log) {
                redisLog(REDIS_WARNING,"Error writing to the AOF file: %s",
                    strerror(errno));
                server.aof_last_write_errno = errno;
            }
        } else {
            if (can_log) {
                redisLog(REDIS_WARNING,"Short write while writing to "
                                       "the AOF file: (nwritten=%lld, "
                                       "expected=%lld)",
                                       (long long)nwritten,
                                       (long long)sdslen(server.aof_buf));
            }

            // 尝试移除新追加的不完整内容
            if (ftruncate(server.aof_fd, server.aof_current_size) == -1) {
                if (can_log) {
                    redisLog(REDIS_WARNING, "Could not remove short write "
                             "from the append-only file.  Redis may refuse "
                             "to load the AOF the next time it starts.  "
                             "ftruncate: %s", strerror(errno));
                }
            } else {
                /* If the ftrunacate() succeeded we can set nwritten to
                 * -1 since there is no longer partial data into the AOF. */
                nwritten = -1;
            }
            server.aof_last_write_errno = ENOSPC;
        }

        /* Handle the AOF write error. */
        // 处理写入 AOF 文件时出现的错误
        if (server.aof_fsync == AOF_FSYNC_ALWAYS) {
            /* We can't recover when the fsync policy is ALWAYS since the
             * reply for the client is already in the output buffers, and we
             * have the contract with the user that on acknowledged write data
             * is synched on disk. */
            redisLog(REDIS_WARNING,"Can't recover from AOF write error when the AOF fsync policy is 'always'. Exiting...");
            exit(1);
        } else {
            /* Recover from failed write leaving data into the buffer. However
             * set an error to stop accepting writes as long as the error
             * condition is not cleared. */
            server.aof_last_write_status = REDIS_ERR;

            /* Trim the sds buffer if there was a partial write, and there
             * was no way to undo it with ftruncate(2). */
            if (nwritten > 0) {
                server.aof_current_size += nwritten;
                sdsrange(server.aof_buf,nwritten,-1);
            }
            return; /* We'll try again on the next call... */
        }
    } else {
        /* Successful write(2). If AOF was in error state, restore the
         * OK state and log the event. */
        // 写入成功，更新最后写入状态
        if (server.aof_last_write_status == REDIS_ERR) {
            redisLog(REDIS_WARNING,
                "AOF write error looks solved, Redis can write again.");
            server.aof_last_write_status = REDIS_OK;
        }
    }

    // 更新写入后的 AOF 文件大小
    server.aof_current_size += nwritten;

    /* Re-use AOF buffer when it is small enough. The maximum comes from the
     * arena size of 4k minus some overhead (but is otherwise arbitrary). 
     *
     * 如果 AOF 缓存的大小足够小的话，那么重用这个缓存，
     * 否则的话，释放 AOF 缓存。
     */
    if ((sdslen(server.aof_buf)+sdsavail(server.aof_buf)) < 4000) {
        // 清空缓存中的内容，等待重用
        sdsclear(server.aof_buf);
    } else {
        // 释放缓存
        sdsfree(server.aof_buf);
        server.aof_buf = sdsempty();
    }

    /* Don't fsync if no-appendfsync-on-rewrite is set to yes and there are
     * children doing I/O in the background. 
     *
     * 如果 no-appendfsync-on-rewrite 选项为开启状态，
     * 并且有 BGSAVE 或者 BGREWRITEAOF 正在进行的话，
     * 那么不执行 fsync 
     */
    if (server.aof_no_fsync_on_rewrite &&
        (server.aof_child_pid != -1 || server.rdb_child_pid != -1))
            return;

    /* Perform the fsync if needed. */

    // 总是执行 fsnyc
    if (server.aof_fsync == AOF_FSYNC_ALWAYS) {
        /* aof_fsync is defined as fdatasync() for Linux in order to avoid
         * flushing metadata. */
        aof_fsync(server.aof_fd); /* Let's try to get this data on the disk */

        // 更新最后一次执行 fsnyc 的时间
        server.aof_last_fsync = server.unixtime;

    // 策略为每秒 fsnyc ，并且距离上次 fsync 已经超过 1 秒
    } else if ((server.aof_fsync == AOF_FSYNC_EVERYSEC &&
                server.unixtime > server.aof_last_fsync)) {
        // 放到后台执行
        if (!sync_in_progress) aof_background_fsync(server.aof_fd);
        // 更新最后一次执行 fsync 的时间
        server.aof_last_fsync = server.unixtime;
    }

    // 其实上面无论执行 if 部分还是 else 部分都要更新 fsync 的时间
    // 可以将代码挪到下面来
    // server.aof_last_fsync = server.unixtime;
}

//根据传入的命令和命令参数，将它们还原成协议格式。
sds catAppendOnlyGenericCommand(sds dst, int argc, robj **argv) {
    char buf[32];
    int len, j;
    robj *o;

    // 重建命令的个数，格式为 *<count>\r\n
    // 例如 *3\r\n
    buf[0] = '*';
    len = 1+ll2string(buf+1,sizeof(buf)-1,argc);
    buf[len++] = '\r';
    buf[len++] = '\n';
    dst = sdscatlen(dst,buf,len);

    // 重建命令和命令参数，格式为 $<length>\r\n<content>\r\n
    // 例如 $3\r\nSET\r\n$3\r\nKEY\r\n$5\r\nVALUE\r\n
    for (j = 0; j < argc; j++) {
        o = getDecodedObject(argv[j]);

        // 组合 $<length>\r\n
        buf[0] = '$';
        len = 1+ll2string(buf+1,sizeof(buf)-1,sdslen(o->ptr));
        buf[len++] = '\r';
        buf[len++] = '\n';
        dst = sdscatlen(dst,buf,len);

        // 组合 <content>\r\n
        dst = sdscatlen(dst,o->ptr,sdslen(o->ptr));
        dst = sdscatlen(dst,"\r\n",2);

        decrRefCount(o);
    }

    // 返回重建后的协议内容
    return dst;
}

/* Create the sds representation of an PEXPIREAT command, using
 * 'seconds' as time to live and 'cmd' to understand what command
 * we are translating into a PEXPIREAT.
 *
 * 创建 PEXPIREAT 命令的 sds 表示，
 * cmd 参数用于指定转换的源指令， seconds 为 TTL （剩余生存时间）。
 *
 * This command is used in order to translate EXPIRE and PEXPIRE commands
 * into PEXPIREAT command so that we retain precision in the append only
 * file, and the time is always absolute and not relative.
 *
 * 这个函数用于将 EXPIRE 、 PEXPIRE 和 EXPIREAT 转换为 PEXPIREAT 
 * 从而在保证精确度不变的情况下，将过期时间从相对值转换为绝对值（一个 UNIX 时间戳）。
 *
 * （过期时间必须是绝对值，这样不管 AOF 文件何时被载入，该过期的 key 都会正确地过期。）
 */
sds catAppendOnlyExpireAtCommand(sds buf, struct redisCommand *cmd, robj *key, robj *seconds) {
    long long when;
    robj *argv[3];

    /* Make sure we can use strtol 
     *
     * 取出过期值
     */
    seconds = getDecodedObject(seconds);
    when = strtoll(seconds->ptr,NULL,10);

    /* Convert argument into milliseconds for EXPIRE, SETEX, EXPIREAT 
     *
     * 如果过期值的格式为秒，那么将它转换为毫秒
     */
    if (cmd->proc == expireCommand || cmd->proc == setexCommand ||
        cmd->proc == expireatCommand)
    {
        when *= 1000;
    }

    /* Convert into absolute time for EXPIRE, PEXPIRE, SETEX, PSETEX 
     *
     * 如果过期值的格式为相对值，那么将它转换为绝对值
     */
    if (cmd->proc == expireCommand || cmd->proc == pexpireCommand ||
        cmd->proc == setexCommand || cmd->proc == psetexCommand)
    {
        when += mstime();
    }

    decrRefCount(seconds);

    // 构建 PEXPIREAT 命令
    argv[0] = createStringObject("PEXPIREAT",9);
    argv[1] = key;
    argv[2] = createStringObjectFromLongLong(when);

    // 追加到 AOF 缓存中
    buf = catAppendOnlyGenericCommand(buf, 3, argv);

    decrRefCount(argv[0]);
    decrRefCount(argv[2]);

    return buf;
}
