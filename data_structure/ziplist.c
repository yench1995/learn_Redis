/*
 * Ziplist是为了尽可能地节约内存而设计的特殊编码双端链表
 *
 * Ziplist可以储存字符串值和整数值
 * 其中，整数值可以被保存为实际的整数，而不是字符数组
 *
 * Ziplist允许在列表的两端进行O(1)的push和pop操作
 * 但整个操作都需要对ziplist进行内存重分配，所以实际复杂度和其占用内存有关
 *
 *
 * Ziplist的整体布局：
 *
 * <zlbytes><zltail><zllen><entry><...><entry><zlend>
 *
 * zlbytes是一个无符号整数，保存着ziplist使用的内存数量(程序可以直接对ziplist的内存大小调整)
 * zltail保存着达到列表最后一个节点(entry)的偏移量(对表尾的pop操作无需遍历整个链表)
 * zllen保存着列表中的节点数量
 * zlend的长度为1字节，值为255，标识列表的末尾
 *
 * 每个ziplist节点(entry)的前面都带有一个header,这个header包含两部分信息
 * 1.前置节点的长度，在程序从后向前遍历时使用
 * 2.当前节点所保存的值的类型和长度
 *
 * <previous_entry_length><encoding><content>
 *
 * header 另一部分的内容和节点所保存的值有关。
 *
 * 1) 如果节点保存的是字符串值，
 *    那么这部分 header 的头 2 个位将保存编码字符串长度所使用的类型，
 *    而之后跟着的内容则是字符串的实际长度。
 *
 * |00pppppp| - 1 byte
 *      String value with length less than or equal to 63 bytes (6 bits).
 *      字符串的长度小于或等于 63 字节。
 * |01pppppp|qqqqqqqq| - 2 bytes
 *      String value with length less than or equal to 16383 bytes (14 bits).
 *      字符串的长度小于或等于 16383 字节。
 * |10______|qqqqqqqq|rrrrrrrr|ssssssss|tttttttt| - 5 bytes
 *      String value with length greater than or equal to 16384 bytes.
 *      字符串的长度大于或等于 16384 字节。
 *
 * 2) 如果节点保存的是整数值，
 *    那么这部分 header 的头 2 位都将被设置为 1 ，
 *    而之后跟着的 2 位则用于标识节点所保存的整数的类型。
 *
 * |11000000| - 1 byte
 *      Integer encoded as int16_t (2 bytes).
 *      节点的值为 int16_t 类型的整数，长度为 2 字节。
 * |11010000| - 1 byte
 *      Integer encoded as int32_t (4 bytes).
 *      节点的值为 int32_t 类型的整数，长度为 4 字节。
 * |11100000| - 1 byte
 *      Integer encoded as int64_t (8 bytes).
 *      节点的值为 int64_t 类型的整数，长度为 8 字节。
 * |11110000| - 1 byte
 *      Integer encoded as 24 bit signed (3 bytes).
 *      节点的值为 24 位（3 字节）长的整数。
 * |11111110| - 1 byte
 *      Integer encoded as 8 bit signed (1 byte).
 *      节点的值为 8 位（1 字节）长的整数。
 * |1111xxxx| - (with xxxx between 0000 and 1101) immediate 4 bit integer.
 *      Unsigned integer from 0 to 12. The encoded value is actually from
 *      1 to 13 because 0000 and 1111 can not be used, so 1 should be
 *      subtracted from the encoded 4 bit value to obtain the right value.
 *      节点的值为介于 0 至 12 之间的无符号整数。
 *      因为 0000 和 1111 都不能使用，所以位的实际值将是 1 至 13 。
 *      程序在取得这 4 个位的值之后，还需要减去 1 ，才能计算出正确的值。
 *      比如说，如果位的值为 0001 = 1 ，那么程序返回的值将是 1 - 1 = 0 。
 * |11111111| - End of ziplist.
 *      ziplist 的结尾标识
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <limits.h>
#include "zmalloc.h"
#include "util.h"
#include "ziplist.h"
#include "endianconv.h"
#include "redisassert.h"

//ziplist末端标识符，以及5字节长长度标识符
#define ZIP_END 255
#define ZIP_BIGLEN 254 //previous_entry_length中使用

//字符串编码和整数编码的掩码
#define ZIP_STR_MASK 0xc0 //11000000
#define ZIP_INT_MASK 0x30 //00110000

//字符串编码类型
#define ZIP_STR_06B (0 << 6)
#define ZIP_STR_14B (1 << 6)
#define ZIP_STR_32B (2 << 6)

//整数编码类型
#define ZIP_INT_16B (0xc0 | 0<<4)
#define ZIP_INT_32B (0xc0 | 1<<4)
#define ZIP_INT_64B (0xc0 | 2<<4)
#define ZIP_INT_24B (0xc0 | 3<<4)
#define ZIP_INT_8B 0xfe

//四位整数编码的掩码和类型
#define ZIP_INT_IMM_MASK 0x0f
#define ZIP_INT_IMM_MIN 0xf1    /* 11110001 */
#define ZIP_INT_IMM_MAX 0xfd    /* 11111101 */
#define ZIP_INT_IMM_VAL(v) (v & ZIP_INT_IMM_MASK)

//24 位整数的最大值和最小值
#define INT24_MAX 0x7fffff
#define INT24_MIN (-INT24_MAX - 1)

//查看给点编码enc是否字符串编码
#define ZIP_IS_STR(enc) (((enc) & ZIP_STR_MASK) < ZIP_STR_MASK)

// 定位到 ziplist 的 bytes 属性，该属性记录了整个 ziplist 所占用的内存字节数
// 用于取出 bytes 属性的现有值，或者为 bytes 属性赋予新值
#define ZIPLIST_BYTES(zl)       (*((uint32_t*)(zl)))
// 定位到 ziplist 的 offset 属性，该属性记录了到达表尾节点的偏移量
// 用于取出 offset 属性的现有值，或者为 offset 属性赋予新值
#define ZIPLIST_TAIL_OFFSET(zl) (*((uint32_t*)((zl)+sizeof(uint32_t))))
// 定位到 ziplist 的 length 属性，该属性记录了 ziplist 包含的节点数量
// 用于取出 length 属性的现有值，或者为 length 属性赋予新值
#define ZIPLIST_LENGTH(zl)      (*((uint16_t*)((zl)+sizeof(uint32_t)*2)))
// 返回 ziplist 表头的大小
#define ZIPLIST_HEADER_SIZE     (sizeof(uint32_t)*2+sizeof(uint16_t))
// 返回指向 ziplist 第一个节点（的起始位置）的指针
#define ZIPLIST_ENTRY_HEAD(zl)  ((zl)+ZIPLIST_HEADER_SIZE)
// 返回指向 ziplist 最后一个节点（的起始位置）的指针
#define ZIPLIST_ENTRY_TAIL(zl)  ((zl)+intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)))
// 返回指向 ziplist 末端 ZIP_END （的起始位置）的指针
#define ZIPLIST_ENTRY_END(zl)   ((zl)+intrev32ifbe(ZIPLIST_BYTES(zl))-1)

/*
空白 ziplist 示例图
area        |<---- ziplist header ---->|<-- end -->|
size          4 bytes   4 bytes 2 bytes  1 byte
            +---------+--------+-------+-----------+
component   | zlbytes | zltail | zllen | zlend     |
            |         |        |       |           |
value       |  1011   |  1010  |   0   | 1111 1111 |
            +---------+--------+-------+-----------+
                                       ^
                                       |
                               ZIPLIST_ENTRY_HEAD
                                       &
address                        ZIPLIST_ENTRY_TAIL
                                       &
                               ZIPLIST_ENTRY_END
非空 ziplist 示例图
area        |<---- ziplist header ---->|<----------- entries ------------->|<-end->|
size          4 bytes  4 bytes  2 bytes    ?        ?        ?        ?     1 byte
            +---------+--------+-------+--------+--------+--------+--------+-------+
component   | zlbytes | zltail | zllen | entry1 | entry2 |  ...   | entryN | zlend |
            +---------+--------+-------+--------+--------+--------+--------+-------+
                                       ^                          ^        ^
address                                |                          |        |
                                ZIPLIST_ENTRY_HEAD                |   ZIPLIST_ENTRY_END
                                                                  |
                                                        ZIPLIST_ENTRY_TAIL
*/

//增加ziplist的节点数
#define ZIPLIST_INCR_LENGTH(zl,incr) { \
    if (ZIPLIST_LENGTH(zl) < UINT16_MAX) \
        ZIPLIST_LENGTH(zl) = intrev16ifbe(intrev16ifbe(ZIPLIST_LENGTH(zl))+incr); \
}

//保存ziplist节点信息的结构
typedef struct zlentry {
    //prevrawlensize:编码prevlawlen所需的字节大小
    //prevrawlen:前置节点的长度
    unsigned int prevrawlensize, prevrawlen;

    //len:当前节点值的长度
    //lensize:编码len所需的字节大小
    unsigned int lensize, len;

    //当前节点header的大小
    //等于prevrawlensize+lensize
    unsigned int headersize;

    //当前节点值所使用的编码类型;
    unsigned char encoding;

    //指向当前节点的指针
    unsigned char *p;
} zlentry;

//从ptr中取出节点值的编码类型
#define ZIP_ENTRY_ENCODING(ptr, encoding) do { \
    (encoding) = (ptr[0]); \
    if ((encoding) < ZIP_STR_MASK)(encoding) &= ZIP_STR_MASK; \
} while (0)

//返回保存encoding编码的值所需的字节数量
static unsigned int zipIntSize(unsigned char encoding) {
    switch(encoding) {
        case ZIP_INT_8B: return 1;
	    case ZIP_INT_16B: return 2;
    	case ZIP_INT_24B: return 3;
    	case ZIP_INT_32B: return 4;
    	case ZIP_INT_64B: return 8;
    	default: return 0; /* 4 bit immediate */
    }
    assert(NULL);
    return 0;
}

/*
 * 将编码节点长度值l写入p中，然后返回编码l所需的字节数量
 * 如果p为NULL,那么仅返回编码l所需的字节数量，不进行写入
 */
static unsigned int zipEncodeLength(unsigned char *p, unsigned char encoding, unsigned int rawlen) {
    unsigned char len = 1, buf[5];

    // 编码字符串
    if (ZIP_IS_STR(encoding)) {
        /* Although encoding is given it may not be set for strings,
         * so we determine it here using the raw length. */
        if (rawlen <= 0x3f) {
            if (!p) return len;
            buf[0] = ZIP_STR_06B | rawlen;
        } else if (rawlen <= 0x3fff) {
            len += 1;
            if (!p) return len;
            buf[0] = ZIP_STR_14B | ((rawlen >> 8) & 0x3f);
            buf[1] = rawlen & 0xff;
        } else {
            len += 4;
            if (!p) return len;
            buf[0] = ZIP_STR_32B;
            buf[1] = (rawlen >> 24) & 0xff;
            buf[2] = (rawlen >> 16) & 0xff;
            buf[3] = (rawlen >> 8) & 0xff;
            buf[4] = rawlen & 0xff;
        }

    // 编码整数
    } else {
        /* Implies integer encoding, so length is always 1. */
        if (!p) return len;
        buf[0] = encoding;
    }

    /* Store this length at p */
    // 将编码后的长度写入 p
    memcpy(p,buf,len);

    // 返回编码所需的字节数
    return len;
}

/*
 *
 * 解码 ptr 指针，取出列表节点的相关信息，并将它们保存在以下变量中：
 *
 * - encoding 保存节点值的编码类型。
 *
 * - lensize 保存编码节点长度所需的字节数。
 *
 * - len 保存节点的长度。
 *
 * T = O(1)
 */

#define ZIP_DECODE_LENGTH(ptr, encoding, lensize, len) do {                    \
                                                                               \
    /* 取出值的编码类型 */                                                     \
    ZIP_ENTRY_ENCODING((ptr), (encoding));                                     \
                                                                               \
    /* 字符串编码 */                                                           \
    if ((encoding) < ZIP_STR_MASK) {                                           \
        if ((encoding) == ZIP_STR_06B) {                                       \
            (lensize) = 1;                                                     \
            (len) = (ptr)[0] & 0x3f;                                           \
        } else if ((encoding) == ZIP_STR_14B) {                                \
            (lensize) = 2;                                                     \
            (len) = (((ptr)[0] & 0x3f) << 8) | (ptr)[1];                       \
        } else if (encoding == ZIP_STR_32B) {                                  \
            (lensize) = 5;                                                     \
            (len) = ((ptr)[1] << 24) |                                         \
                    ((ptr)[2] << 16) |                                         \
                    ((ptr)[3] <<  8) |                                         \
                    ((ptr)[4]);                                                \
        } else {                                                               \
            assert(NULL);                                                      \
        }                                                                      \
                                                                               \
    /* 整数编码 */                                                             \
    } else {                                                                   \
        (lensize) = 1;                                                         \
        (len) = zipIntSize(encoding);                                          \
    }                                                                          \
} while(0);

/*
 * 对前置节点的长度 len 进行编码，并将它写入到 p 中，
 * 然后返回编码 len 所需的字节数量。
 *
 * 如果 p 为 NULL ，那么不进行写入，仅返回编码 len 所需的字节数量。
 *
 * T = O(1)
 */
static unsigned int zipPrevEncodeLength(unsigned char *p, unsigned int len) {

    // 仅返回编码 len 所需的字节数量
    if (p == NULL) {
        return (len < ZIP_BIGLEN) ? 1 : sizeof(len)+1;

    // 写入并返回编码 len 所需的字节数量
    } else {

        // 1 字节
        if (len < ZIP_BIGLEN) {
            p[0] = len;
            return 1;

        // 5 字节
        } else {
            // 添加 5 字节长度标识
            p[0] = ZIP_BIGLEN;
            // 写入编码
            memcpy(p+1,&len,sizeof(len));
            // 如果有必要的话，进行大小端转换
            memrev32ifbe(p+1);
            // 返回编码长度
            return 1+sizeof(len);
        }
    }
}

//将原本只需要一个字节来保存的前置节点长度len编码至一个5字节长的header中
static void zipPrevEncodeLengthForceLarge(unsigned char *p, unsigned int len) {
    if (p == NULL) return;

    //设置5字节长度标识
    p[0] = ZIP_BIGLEN;

    //写入len
    memcpy(p+1, &len, sizeof(len));
    memrev32ifbe(p+1);
}


/*
 * 解码 ptr 指针，
 * 取出编码前置节点长度所需的字节数，并将它保存到 prevlensize 变量中。
 *
 * T = O(1)
 */
#define ZIP_DECODE_PREVLENSIZE(ptr, prevlensize) do {                          \
    if ((ptr)[0] < ZIP_BIGLEN) {                                               \
        (prevlensize) = 1;                                                     \
    } else {                                                                   \
        (prevlensize) = 5;                                                     \
    }                                                                          \
} while(0);

/* Decode the length of the previous element, from the perspective of the entry
 * pointed to by 'ptr'.
 *
 * 解码 ptr 指针，
 * 取出编码前置节点长度所需的字节数，
 * 并将这个字节数保存到 prevlensize 中。
 *
 * 然后根据 prevlensize ，从 ptr 中取出前置节点的长度值，
 * 并将这个长度值保存到 prevlen 变量中。
 *
 * T = O(1)
 */
#define ZIP_DECODE_PREVLEN(ptr, prevlensize, prevlen) do {                     \
                                                                               \
    /* 先计算被编码长度值的字节数 */                                           \
    ZIP_DECODE_PREVLENSIZE(ptr, prevlensize);                                  \
                                                                               \
    /* 再根据编码字节数来取出长度值 */                                         \
    if ((prevlensize) == 1) {                                                  \
        (prevlen) = (ptr)[0];                                                  \
    } else if ((prevlensize) == 5) {                                           \
        assert(sizeof((prevlensize)) == 4);                                    \
        memcpy(&(prevlen), ((char*)(ptr)) + 1, 4);                             \
        memrev32ifbe(&prevlen);                                                \
    }                                                                          \
} while(0);

/*
 * 计算编码新的前置节点长度len所需的字节数
 * 减去编码p原来的前置节点长度，得到所需的字节数之差
 */
static int zipPrevLenByteDiff(unsigned char *p, unsigned int len) {
    unsigned int prevlensize;
    //取出编码原来的前置节点长度所需的字节数
    ZIP_DECODE_PREVLENSIZE(p, prevlensize);
    //计算编码len所需的字节数，然后进行减法运算
    return zipPrevEncodeLength(NULL, len) - prevlensize;
}

//返回指针p所指向的节点占用的字节数总和
static unsigned int zipRawEntryLength(unsigned char *p) {
    unsigned int prevlensize, encoding, lensize, len;

    //取出编码前置节点的长度所需的字节数
    ZIP_DECODE_PREVLENSIZE(p, prevlensize);

    //取出当前节点值的编码类型，编码节点值长度所需的字节数，以及节点值的长度
    ZIP_DECODE_LENGTH(p+prevlensize, encoding, lensize, len);

    return prevlensize + lensize + len;
}

/*
 * 检查entry中指向的字符串能否被编码为整数
 * 如果可以的话，将编码后的整数保存在指针v的值中，并将编码的方式保存在指针encoding的值中
 */
static int zipTryEncoding(unsigned char *entry, unsigned int entrylen,
                        long long *v, unsigned char *encoding) {
    long long value;

    //忽略太长或太短的字符串
    if (entrylen >= 32 || entrylen == 0) return 0;

    //尝试转换
    if (string2ll((char*)entry, entrylen, &value)) {
        //转换成功，以从小到大的顺序检查适合值value的编码方式
        if (value >= 0 && value <= 12) {
            *encoding = ZIP_INT_IMM_MIN + value;
        } else if (value >= INT8_MIN && value <= INT8_MAX) {
            *encoding = ZIP_INT_8B;
        } else if (value >= INT16_MIN && value <= INT16_MAX) {
            *encoding = ZIP_INT_16B;
        } else if (value >= INT24_MIN && value <= INT24_MAX) {
            *encoding = ZIP_INT_24B;
        } else if (value >= INT32_MIN && value <= INT32_MAX) {
            *encoding = ZIP_INT_32B;
        } else {
            *encoding = ZIP_INT_64B;
        }

        // 记录值到指针
        *v = value;

        // 返回转换成功标识
        return 1;
    }

    // 转换失败
    return 0;
}

//以encodign指定的编码方式，将整数值value写入到p
static void zipSaveInteger(unsigned char *p, int64_t value, unsigned char encoding) {
    int16_t i16;
    int32_t i32;
    int64_t i64;

    if (encoding == ZIP_INT_8B) {
        ((int8_t*)p)[0] = (int8_t)value;
    } else if (encoding == ZIP_INT_16B) {
        i16 = value;
        memcpy(p,&i16,sizeof(i16));
        memrev16ifbe(p);
    } else if (encoding == ZIP_INT_24B) {
        i32 = value<<8;
        memrev32ifbe(&i32);
        memcpy(p,((uint8_t*)&i32)+1,sizeof(i32)-sizeof(uint8_t));
    } else if (encoding == ZIP_INT_32B) {
        i32 = value;
        memcpy(p,&i32,sizeof(i32));
        memrev32ifbe(p);
    } else if (encoding == ZIP_INT_64B) {
        i64 = value;
        memcpy(p,&i64,sizeof(i64));
        memrev64ifbe(p);
    } else if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX) {
        /* Nothing to do, the value is stored in the encoding itself. */
    } else {
        assert(NULL);
    }
}

//以encoding指定的编码方式，读取并返回指针p中的整数值
static int64_t zipLoadInteger(unsigned char *p, unsigned char encoding) {
    int16_t i16;
    int32_t i32;
    int64_t i64, ret = 0;

    if (encoding == ZIP_INT_8B) {
        ret = ((int8_t*)p)[0];
    } else if (encoding == ZIP_INT_16B) {
        memcpy(&i16,p,sizeof(i16));
        memrev16ifbe(&i16);
        ret = i16;
    } else if (encoding == ZIP_INT_32B) {
        memcpy(&i32,p,sizeof(i32));
        memrev32ifbe(&i32);
        ret = i32;
    } else if (encoding == ZIP_INT_24B) {
        i32 = 0;
        memcpy(((uint8_t*)&i32)+1,p,sizeof(i32)-sizeof(uint8_t));
        memrev32ifbe(&i32);
        ret = i32>>8;
    } else if (encoding == ZIP_INT_64B) {
        memcpy(&i64,p,sizeof(i64));
        memrev64ifbe(&i64);
        ret = i64;
    } else if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX) {
        ret = (encoding & ZIP_INT_IMM_MASK)-1;
    } else {
        assert(NULL);
    }

    return ret;
}

//将p所指向的列表节点的信息全部保存到zlentry中，并返回该zlentry
static zlentry zipEntry(unsigned char *p) {
    zlentry e;

    // e.prevrawlensize 保存着编码前一个节点的长度所需的字节数
    // e.prevrawlen 保存着前一个节点的长度
    // T = O(1)
    ZIP_DECODE_PREVLEN(p, e.prevrawlensize, e.prevrawlen);

    // p + e.prevrawlensize 将指针移动到列表节点本身
    // e.encoding 保存着节点值的编码类型
    // e.lensize 保存着编码节点值长度所需的字节数
    // e.len 保存着节点值的长度
    // T = O(1)
    ZIP_DECODE_LENGTH(p + e.prevrawlensize, e.encoding, e.lensize, e.len);

    // 计算头结点的字节数
    e.headersize = e.prevrawlensize + e.lensize;

    // 记录指针
    e.p = p;

    return e;
}

//创建并返回一个新的ziplist
unsigned char *ziplistNew(void) {
    //ZIPLIST_HEADER_SIZE是ziplist表头的大小
    //1字节是表末端ZIP_END的大小
    unsigned int bytes = ZIPLIST_HEADER_SIZE + 1;

    unsigned char *zl = zmalloc(bytes);

    //初始化链表属性
    ZIPLIST_BYTES(zl) = intrev32ifbe(bytes);
    ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(ZIPLIST_HEADER_SIZE);
    ZIPLIST_LENGTH(zl) = 0;

    zl[bytes-1] = ZIP_END;

    return zl;
}

/*
 * 调整ziplist的大小为len字节
 * 当ziplist原有的大小小于len时，扩展ziplist不会改变ziplist原有的元素
 */
static unsigned char *ziplistResize(unsigned char *zl, unsigned int len) {
    zl = zrealloc(zl, len);
    //更新bytes属性
    ZIPLIST_BYTES(zl) = intrev32ifbe(len);
    //重新设置表末端
    zl[len-1] = ZIP_END;

    return zl;
}

static unsigned char *__ziplistCascadeUpdate(unsigned char *zl, unsigned char *p) {
    size_t curlen = intrev32ifbe(ZIPLIST_BYTES(zl)), rawlen, rawlensize;
    size_t offset, noffset, extra;
    unsigned char *np;
    zlentry cur, next;

    while (p[0] != ZIP_END) {

        //将p所指向的节点的信息保存到cur结构中
        cur = zipEntry(p);
        //当前节点的长度
        rawlen = cur.headersize + cur.len;
        //计算后一个节点编码当前节点的长度所需要的字节数
        rawlensize = zipPrevEncodeLength(NULL, rawlen);

        //如果已经没有后续空间需要更新了，跳出
        if (p[rawlen] == ZIP_END) break;

        //取出后续节点的信息，保存到next结构中
        next = zipEntry(p+rawlen);

        //后续节点编码当前节点的空间已经足够，无须进行更多处理，跳出
        //可以证明，只要遇到一个空间足够的节点，那么这个节点之后的所有节点都是空间足够的
        if (next.prevrawlen == rawlen) break;

        if (next.prevrawlen < rawlensize) {

            //记录p的偏移量
            offset = p-zl;
            //计算需要增加的节点数量
            extra = rawlensize-next.prevrawlensize;
            //扩展zl的大小
            zl = ziplistResize(zl, curlen+extra);
            //还原指针p
            p = zl+offset;

            //记录下一节点的偏移量
            np = p+rawlen;
            noffset = np-zl;
            //当next不是表尾节点时，更新列表到表尾节点的偏移量
            if ((zl+intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))) != np) {
                ZIPLIST_TAIL_OFFSET(zl) =
                    intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))+extra);
            }

            //向后移动从next.prevrawlensize之后的数据，为新的header腾出空间
            memmove(np+rawlensize,
                    np+next.prevrawlensize,
                    curlen-noffset-next.prevrawlensize-1);

            //将新的前一节点长度值编码进新的next节点的header
            zipPrevEncodeLength(np, rawlen);

            //移动指针，继续处理下个节点
            p += rawlen;
            curlen += extra;
        } else {
            if (next.prevrawlensize > rawlensize) {
                //程序不会对next进行缩小
                //只将rawlen写入5字节的header中
                zipPrevEncodeLengthForceLarge(p+rawlen, rawlen);
            } else {
                zipPrevEncodeLength(p+rawlen, rawlen);
            }

            break;
        }
    }

    return zl;
}

//从位置p开始，连续删除num个节点
static unsigned char *__ziplistDelete(unsigned char *zl, unsigned char *p, unsigned int num) {
    unsigned int i, totlen, deleted = 0;
    size_t offset;
    int nextdiff = 0;
    zlentry first, tail;

    //计算被删除节点总共占用的内存字节数，以及被删除节点的总个数
    first = zipEntry(p);
    for (i = 0; p[0] != ZIP_END && i < num; ++i) {
        p += zipRawEntryLength(p);
        deleted++;
    }

    //totlen是所有被删除节点总共占用的内存字节数
    totlen = p-first.p;
    if (totlen > 0) {
        if (p[0] != ZIP_END) {

            //执行这里，表示被删除节点之后仍然有节点存在
            //计算新旧前置节点之间的字节数差
            nextdiff = zipPrevLenByteDiff(p, first.prevrawlen);
            //如果有需要的话，将指针p后退nextdiff字节，为新header腾出空间
            p -= nextdiff;
            zipPrevEncodeLength(p, first.prevrawlen);

            //更新到达表尾的偏移量
            ZIPLIST_TAIL_OFFSET(zl) =
                intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)) - totlen);

            //如果被删除节点之后，有多于一个节点
            //那么需要将nextdiff记录的字节数页计算到尾表偏移量中
            tail = zipEntry(p);
            if (p[tail.headersize+tail.len] != ZIP_END) {
                ZIPLIST_TAIL_OFFSET(zl) =
                    intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)) + nextdiff);
            }

            //从表尾向表头移动数据，覆盖被删除节点的数据
            memmove(first.p, p, intrev32ifbe(ZIPLIST_BYTES(zl))-(p-zl)-1);
        } else {

            //执行这里，表示被删除节点之后已经没有其他节点了
            ZIPLIST_TAIL_OFFSET(zl) =
                intrev32ifbe((first.p-zl)-first.prevrawlen);
        }

        //缩小并更新ziplist的长度
        offset = first.p-zl;
        zl = ziplistResize(zl, intrev32ifbe(ZIPLIST_BYTES(zl))-totlen+nextdiff);
        ZIPLIST_INCR_LENGTH(zl, -deleted);
        p = zl + offset;

        //如果p所指向的节点的大小已经变更，那么进行联级更新
        //检查p之后的所有节点是否符合ziplist的编码要求
        if (nextdiff != 0)
            zl = __ziplistCascadeUpdate(zl, p);
    }
    return zl;
}

//根据指针p所指定的位置，将长度为slen的字符串s插入到zl中
static unsigned char *__ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen) {
    //记录当前ziplist的长度
    size_t curlen = intrev32ifbe(ZIPLIST_BYTES(zl)), reqlen, prevlen = 0;
    size_t offset;
    int nextdiff = 0;
    unsigned char encoding = 0;
    long long value = 123456789;

    zlentry entry, tail;

    if (p[0] != ZIP_END) {
        //如果p[0]不指向列表末端，说明列表非空，并且p正指向列表的其中一个节点
        //那么取出p所指向节点的信息，并将它保存到entry结构中
        //然后用prevlen变量记录前置节点的长度
        entry = zipEntry(p);
        prevlen = entry.prevrawlen;
    } else {
        unsigned char *ptail = ZIPLIST_ENTRY_TAIL(zl);
        if (ptail[0] != ZIP_END) {
            //取出表尾节点的长度
            prevlen = zipRawEntryLength(ptail);
        }
    }

    //尝试能否将输入字符串转换为整数
    //reqlen保存节点值content的长度
    if (zipTryEncoding(s, slen, &value, &encoding)) {
        reqlen = zipIntSize(encoding);
    } else {
        reqlen = slen;
    }

    //计算编码前置节点的长度所需的大小
    reqlen += zipPrevEncodeLength(NULL, prevlen);
    //计算编码当前节点所需的大小
    reqlen += zipEncodeLength(NULL, encoding, slen);

    //若新节点不是添加到末端，那么p所指向的节点的header需要被检测是否能编码新节点的长度
    //nextdiff记录了新旧编码之间的字节大小差，如果大于0,则需要扩展
    nextdiff = (p[0] != ZIP_END) ? zipPrevLenByteDiff(p, reqlen) : 0;

    offset = p-zl;
    zl = ziplistResize(zl, curlen+reqlen+nextdiff);
    p = zl+offset;

    if (p[0] != ZIP_END) {

        //移动现有元素，为新元素的插入空间腾出位置
        memmove(p+reqlen, p-nextdiff, curlen-offset-1+nextdiff);
        //将新节点的长度编码至后置节点
        zipPrevEncodeLength(p+reqlen, reqlen);

        //更新到达表尾的偏移量，将新节点的长度也算上
        ZIPLIST_TAIL_OFFSET(zl) =
            intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)) + reqlen);

        tail = zipEntry(p+reqlen);
        if (p[reqlen+tail.headersize+tail.len] != ZIP_END) {
            ZIPLIST_TAIL_OFFSET(zl) =
                intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)) + nextdiff);
        }
    } else {
        //新元素是新的尾表节点
        ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(p-zl);
    }

    //当nextdiff != 0时，新节点的后继节点的header部分长度被改变
    //所以需要联级地更新后继节点
    if (nextdiff != 0) {
        offset = p-zl;
        zl = __ziplistCascadeUpdate(zl, p+reqlen);
        p = zl+offset;
    }

    //将前置节点的长度写入新节点的header
    p += zipPrevEncodeLength(p, prevlen);
    //将节点值的长度写入
    p += zipEncodeLength(p, encoding, slen);
    //写入节点值
    if (ZIP_IS_STR(encoding)) {
        memcpy(p, s, slen);
    } else {
        zipSaveInteger(p, value, encoding);
    }

    //更新列表的节点数量计数器
    ZIPLIST_INCR_LENGTH(zl, 1);

    return zl;
}

//将长度为slen的字符串s推入到zl中
unsigned char *ziplistPush(unsigned char *zl, unsigned char *s, unsigned int slen, int where) {

    //根据where参数的值，决定将值推入到表头还是表尾
    unsigned char *p;
    p = (where == ZIPLIST_HEAD) ? ZIPLIST_ENTRY_HEAD(zl) : ZIPLIST_ENTRY_END(zl);

    return __ziplistInsert(zl, p, s, slen);
}

//根据给定索引，遍历列表，并返回索引指定节点的指针
unsigned char *ziplistIndex(unsigned char *zl, int index) {
    unsigned char *p;
    zlentry entry;

    //处理负的索引，从表尾向表头遍历，负数索引从-1开始
    if (index < 0) {
        index = (-index)-1;

        //定位到表尾节点
        p = ZIPLIST_ENTRY_TAIL(zl);

        if (p[0] != ZIP_END) {
            entry = zipEntry(p);
            while (entry.prevrawlen > 0 && index--) {
                p -= entry.prevrawlen;
                entry = zipEntry(p);
            }
        }
    } else {
        p = ZIPLIST_ENTRY_HEAD(zl);

        while (p[0] != ZIP_END && index--) {
            p += zipRawEntryLength(p);
        }
    }

    return (p[0] == ZIP_END || index > 0) ? NULL : p;
}

//返回p所指向节点的后置节点
unsigned char *ziplistNext(unsigned char *zl, unsigned char *p) {
    ((void) zl);

    if (p[0] == ZIP_END)
        return NULL;

    p += zipRawEntryLength(p);
    if (p[0] == ZIP_END)
        return NULL;

    return p;
}

//返回p锁指向节点的前置节点
unsigned char *ziplistPrev(unsigned char *zl, unsigned char *p) {
    zlentry entry;

    //如果p指向列表末端，那么尝试取出列表尾端节点
    if (p[0] == ZIP_END) {
        p = ZIPLIST_ENTRY_TAIL(zl);
        return (p[0] == ZIP_END) ? NULL : p;
    } else if (p == ZIPLIST_ENTRY_HEAD(zl)) {
        return NULL;
    } else {
        entry = zipEntry(p);
        assert(entry.prevrawlen > 0);
        return p-entry.prevrawlen;
    }
}

/*
 * 取出p所指向节点的值：
 * - 如果节点保存的是字符串，那么将字符串指针保存到*sstr中，字符串长度保存到*slen
 * - 如果节点保存的是整数，那么将整数保存到*sval
 *
 *   程序可以通过检查 *sstr是否为NULL来检测值是字符串还是整数
 */
unsigned int ziplistGet(unsigned char *p, unsigned char **sstr, unsigned int *slen,  long long *sval) {
    zlentry entry;
    if (p == NULL || p[0] == ZIP_END) return 0;
    if (sstr) *sstr = NULL;

    entry = zipEntry(p);

    //节点的值为字符串
    if (ZIP_IS_STR(entry.encoding)) {
        if (sstr) {
            *slen = entry.len;
            *sstr = p+entry.headersize;
        }
    } else {
        if (sval)
            *sval = zipLoadInteger(p+entry.headersize, entry.encoding);
    }

    return 1;
}

/*
 * 将包含给定值s的新节点插入到给定的位置p中
 * 如果p指向一个节点，那么新节点将放置在原有节点的前面
 */
unsigned char *ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen) {
    return __ziplistInsert(zl, p, s, slen);
}

/*
 * 从zl中删除*p所指向的节点
 * 并且原地更新*p所指向的位置，使得可以在迭代列表的过程中对节点进行删除
 */
unsigned char *ziplistDelete(unsigned char *zl, unsigned char **p) {

    //因为__ziplistDelete时会对zl进行内存重分配
    //所以要记录到达*p的偏移量
    size_t offset = *p-zl;
    zl = __ziplistDelete(zl, *p, 1);

    *p = zl+offset;
    return zl;
}

//从index索引指定的节点开始，连续地从zl中删除num个节点
unsigned char *ziplistDeleteRange(unsigned char *zl, unsigned int index, unsigned int num) {
    unsigned char *p = ziplistIndex(zl, index);

    return (p == NULL) ? zl : __ziplistDelete(zl, p, num);
}

/*
 * 将p所指向的节点的值和sstr进行对比
 * 如果节点值和sstr的值相等，返回1, 不相等则返回0
 */
unsigned int ziplistCompare(unsigned char *p, unsigned char *sstr, unsigned int slen) {
    zlentry entry;
    unsigned char sencoding;
    long long zval, sval;
    if (p[0] == ZIP_END) return 0;

    entry = zipEntry(p);
    if (ZIP_IS_STR(entry.encoding)) {
        //节点值为字符串，进行字符串对比
        if (entry.len == slen) {
            return memcmp(p+entry.headersize, sstr, slen) == 0;
        } else {
            return 0;
        }
    } else {
        //节点值为整数，进行整数对比
        if (zipTryEncoding(sstr, slen, &sval, &sencoding)) {
            zval = zipLoadInteger(p+entry.headersize, entry.encoding);
            return zval == sval;
        }
    }
    return 0;
}

/*
 * 寻找节点值和vstr相等列表节点，并返回该节点的指针
 * 每次比对之前都跳过skip个节点
 * 如果找不到相应的节点
 */
unsigned char *ziplistFind(unsigned char *p, unsigned char *vstr, unsigned int vlen, unsigned int skip) {
    int skipcnt = 0;
    unsigned char vencoding = 0;
    long long vll = 0;

    //只要未到达列表末端，就一直迭代
    while (p[0] != ZIP_END) {
        unsigned int prevlensize, encoding, lensize, len;
        unsigned char *q;

        ZIP_DECODE_PREVLENSIZE(p, prevlensize);
        ZIP_DECODE_LENGTH(p+prevlensize, encoding, lensize, len);

        q = p+prevlensize+lensize;
        if (skipcnt == 0) {
            //对比字符串值
            if (ZIP_IS_STR(encoding)) {
                if (len == vlen && memcmp(q, vstr, vlen) == 0) {
                    return p;
                }
            } else {
                //因为传入值有可能被编码，所以第一次传入值会进行解码
                if (vencoding == 0) {
                    if (!zipTryEncoding(vstr, vlen, &vll, &vencoding)) {
                        vencoding = UCHAR_MAX;
                    }
                    assert(vencoding);
                }

                if (vencoding != UCHAR_MAX) {
                    long long ll = zipLoadInteger(q, encoding);
                    if (ll == vll) {
                        return p;
                    }
                }
            }

            skip = skip;
        } else {
            skipcnt--;
        }
        //后移指针，指向后置节点
        p = q + len;
    }
    return NULL;
}

//返回ziplist中的节点个数
unsigned int ziplistLen(unsigned char *zl) {
    unsigned int len = 0;

    //节点数小于UINT16_MAX
    if (intrev16ifbe(ZIPLIST_LENGTH(zl)) < UINT16_MAX) {
        len = intrev16(ZIPLIST_LENGTH(zl));

    //节点数大于UINT16_MAX时，需要遍历整个列表
    } else {
        unsigned char *p = zl+ZIPLIST_HEADER_SIZE;
        while (*p != ZIP_END) {
            p += ziprawEntryLength(p);
            len++;
        }

        if (len < UINT16_MAX) ZIPLIST_LENGTH(zl) = intrev16ifbe(len);
    }

    return len;
}

//返回整个ziplist占用的内存字节数
size_t ziplistBlobLen(unsigned char *zl) {
    return intrev32ifbe(ZIPLIST_BYTES(zl));
}

