#include "redis.h"
#include <math.h>
#include <ctype.h>

//typedef struct redisObject {
//
//    //类型
//    unsigned type:4;
//
//    //编码
//    unsigned encoding:4;
//
//    //对象最后一次被访问的时间
//    unsigned lru:REDIS_LRU_BITS;
//
//    //引用记数
//    int refcount;
//
//    //指向实际值的指针
//    void *ptr;
//} robj;

//创建一个新robj对象
robj *createObject(int type, void *ptr) {
    robj *o = zmalloc(sizeof(*o));

    o->type = type;
    o->encoding = REDIS_ENCODING_RAW;
    o->ptr = ptr;
    o->refcount = 1;

    //把lru设置为当前时钟时间
    o->lru = LRU_CLOCK();
    return 0;
}

//创建一个REDIS_ENCODING_RAW编码的字符串对象
//对象的指针指向一个sds结构
robj *createRawStringObject(char *ptr, size_t len) {
    return createObject(REDIS_STRING, sdsnewlen(ptr, len));
}

//创建一个REDIS_ENCODING_EMBSTR编码的字符对象
//这个字符串对象中的sds会和字符串对象的redisObject结构一起分配
//sds紧邻在redisObject后
robj *createEmbeddedStringObject(char *ptr, size_t len) {
    robj *o = zmalloc(sizeof(robj)+sizeof(struct sdshdr)+len+1);
    struct sdshdr *sh = (void*)(o+1);

    o->type = REDIS_STRING;
    o->encoding = REDIS_ENCODING_EMBSTR;
    o->ptr = sh+1;
    o->refcount = 1;
    o->lru = LRU_CLOCK();

    sh->len = len;
    sh->free = 0;
    if (ptr) {
        memcpy(sh->buf, ptr, len);
        sh->buf[len] = '\0';
    } else {
        memset(sh->buf, 0, len+1);
    }
    return o;
}

//根据string的长度来确定创建RAW还是EMBSTR
#define REDIS_ENCODING_EMBSTR_SIZE_LIMIT 39
robj *createStringObject(char *ptr, size_t len) {
    if (len <= REDIS_ENCODING_EMBSTR_SIZE_LIMIT)
        return createEmbeddedStringObject(ptr, len);
    else
        return createRawStringObject(ptr, len);
}

/*
 * 根据传入的整数值
 *
 * 这个字符串的对象保存的可以使INT编码的long值
 * 也可以是RAW编码的，被转换成字符串的long long 值
 */
robj *createStringObjectFromLongLong(long long value) {
    robj *o;

    //value的大小符合REDIS共享整数的范围，返回一个共享对象
    if (value >= 0 && value < REDIS_SHARED_INTEGERS) {
        incrRefCount(shared.integers[value]);
        o = shared.integers[value];

    //不符合共享范围，创建一个新的整数
    } else {
        //值可以用long类型保存
        if (value >= LONG_MIN && value <= LONG_MAX) {
            o = createObject(REDIS_STRING, NULL);
            o->encoding = REDIS_ENCODING_INT;
            o->ptr = (void*)((long)value);
        } else {
            o = createObject(REDIS_STRING, sdsfromlonglong(value));
        }
    }

    return 0;
}

//根据传入的long double值，为它创建一个字符串对象
robj *createStringObjectFromLongDouble(long double value) {
    char buf[256];
    int len;

    //使用17位小数精度，这种精度可以在大部分机器上被rounding而不改变
    len = snprintf(buf, sizeof(buf), "%.17Lf", value);

    //移除尾部的0
    if (strchr(buf, '.') != NULL) {
        char *p = buf+len-1;
        while (*p == '0') {
            p--;
            len--;
        }
        //如果不需要小数点，那么移除它
        if (*p == '.') len--;
    }

    return createStringObject(buf, len);
}

/*
 * 复制一个字符串对象，复制出的对象和输入对象拥有相同编码
 * 这个函数在复制一个包含整数值的字符串对象时，总是产生一个非共享的对象
 */
robj *dupStringObject(robj *o) {
    robj *d;

    redisAssert(o->type == REDIS_STRING);

    switch(o->encoding) {

        case REDIS_ENCODING_RAW:
            return createRawStringObject(o->ptr, sdslen(o->ptr));
        case REDIS_ENCODING_EMBSTR:
            return createEmbeddedStringObject(o->ptr, sdslen(o->ptr));
        case REDIS_ENCODING_INT:
            d = createObject(REDIS_STRING, NULL);
            d->encoding = REDIS_ENCODING_INT;
            d->ptr = o->ptr;
            return d;
        default:
            redisPanic("Wrong encoding.");
            break;
    }
}

//创建一个LINKEDLIST编码的列表对象
robj *createListObject(void) {
    list *l = listCreate();
    robj *o = createObject(REDIS_LIST, l);
    listSetFreeMethod(l, decrRefCountVoid);
    o->encoding = REDIS_ENCODING_LINKEDLIST;
    return o;
}

//创键一个ZIPLIST编码的列表对象
robj *createZiplistObject(void) {
    unsigned char *zl = ziplistNew();
    robj *o = createObject(REDIS_LIST, zl);
    o->encoding = REDIS_ENCODING_ZIPLIST;
    return o;
}

//创建一个SET编码的集合对象
robj *createSetObject(void) {
    dict *d = dictCreate(&setDictType, NULL);
    robj *o = createObject(REDIS_SET, d);
    o->encoding = REDIS_ENCODING_HT;
    return o;
}

//创建一个INTSET编码的集合对象
robj *createIntsetObject(void) {
    intset *is = intsetNew();
    robj *o = createObject(REDIS_SET, is);
    o->encoding = REDIS_ENCODING_INTSET;
    return o;
}

//创建一个ZIPLIST编码的哈希对象
robj *createHashObject(void) {
    unsigned char *zl = ziplistNew();
    robj *o = createObject(REDIS_HASH, zl);
    o->encoding = REDIS_ENCODING_ZIPLIST;
    return o;
}

//创建一个SKIPLIST编码的有序集合
robj *createZsetObject(void) {
    zset *zs = zmalloc(sizeof(*zs));
    robj *o;

    zs->dict = dictCreate(&zsetDictType, NULL);
    zs->zsl = zslCreate();

    o = createObject(REDIS_ZSET, zs);
    o->encoding = REDIS_ENCODING_SKIPLIST;
    return o;
}

//创建一个ZIPLIST编码的有序集合
robj *createZsetZiplistObject(void) {
    unsigned char *zl = ziplistNew();
    robj *o = createObject(REDIS_ZSET, zl);
    o->encoding = REDIS_ENCODING_ZIPLIST;
    return o;
}

//释放字符串对象
void freeStringObject(robj *o) {
    if (o->encoding == REDIS_ENCODING_RAW)
        sdsfree(o->ptr);
}

//释放列表对象
void freeListObject(robj *o) {
    switch (o->encoding) {
        case REDIS_ENCODING_LINKEDLIST:
            listRelease((list*) o->ptr);
            break;

        case REDIS_ENCODING_ZIPLIST:
            zfree(o->ptr);
            break;

        default:
            redisPanic("Unknown list encoding type");
    }
}

//释放集合对象
void freeSetObject(robj *o) {
    switch (o->encoding) {
        case REDIS_ENCODING_HT:
            dictRelease((dict*) o->ptr);
            break;

        case REDIS_ENCODING_INTSET:
            zfree(o->ptr);
            break;

        default:
            redisPanic("Unknown set encoding type");
    }
}

//释放有序集合对象
void freeZsetObject(robj *o) {
    zset *zs;
    switch (o->encoding) {
        case REDIS_ENCODING_SKIPLIST:
            zs = o->ptr;
            dictRelease(zs->dict);
            zslFree(zs->zsl);
            zfree(zs);
            break;

        case REDIS_ENCODING_ZIPLIST:
            zfree(o->ptr);
            break;

        default:
            redisPanic("Unknown sorted set encoding");
    }
}

//释放哈希对象
void freeHashObject(robj *o) {
    switch (o->encoding) {
        case REDIS_ENCODING_HT:
            dictRelease((dict*) o->ptr);
            break;
        case REDIS_ENCODING_ZIPLIST:
            zfree(o->ptr);
            break;
        default:
            redisPanic("Unknown hash encoding type");
            break;
    }
}

/*
 * 为对象的引用计数增一
 */
void incrRefCount(robj *o) {
    o->refcount++;
}

/*
 * 为对象的引用计数减一
 *
 * 当对象的引用计数降为 0 时，释放对象。
 */
void decrRefCount(robj *o) {

    if (o->refcount <= 0) redisPanic("decrRefCount against refcount <= 0");

    // 释放对象
    if (o->refcount == 1) {
        switch(o->type) {
        case REDIS_STRING: freeStringObject(o); break;
        case REDIS_LIST: freeListObject(o); break;
        case REDIS_SET: freeSetObject(o); break;
        case REDIS_ZSET: freeZsetObject(o); break;
        case REDIS_HASH: freeHashObject(o); break;
        default: redisPanic("Unknown object type"); break;
        }
        zfree(o);

    // 减少计数
    } else {
        o->refcount--;
    }
}

//作用于特定数据结构的释放函数包装
void decrRefCountVoid(void *o) {
    decrRefCount(o);
}

//这个函数将对象的引用记数设为0,但并不释放对象
robj *resetRefCount(robj *obj) {
    obj->refcount = 0;
    return obj;
}

/*
 * 检查对象 o 的类型是否和 type 相同：
 *
 *  - 相同返回 0
 *
 *  - 不相同返回 1 ，并向客户端回复一个错误
 */
int checkType(redisClient *c, robj *o, int type) {

    if (o->type != type) {
        addReply(c,shared.wrongtypeerr);
        return 1;
    }

    return 0;
}

/*
 * 检查对象 o 中的值能否表示为 long long 类型：
 *
 *  - 可以则返回 REDIS_OK ，并将 long long 值保存到 *llval 中。
 *
 *  - 不可以则返回 REDIS_ERR
 */
int isObjectRepresentableAsLongLong(robj *o, long long *llval) {

    redisAssertWithInfo(NULL,o,o->type == REDIS_STRING);

    // INT 编码的 long 值总是能保存为 long long
    if (o->encoding == REDIS_ENCODING_INT) {
        if (llval) *llval = (long) o->ptr;
        return REDIS_OK;

    // 如果是字符串的话，那么尝试将它转换为 long long
    } else {
        return string2ll(o->ptr,sdslen(o->ptr),llval) ? REDIS_OK : REDIS_ERR;
    }
}

// 尝试对字符串对象进行编码，以节约内存。
robj *tryObjectEncoding(robj *o) {
    long value;

    sds s = o->ptr;
    size_t len;

    /* Make sure this is a string object, the only type we encode
     * in this function. Other types use encoded memory efficient
     * representations but are handled by the commands implementing
     * the type. */
    redisAssertWithInfo(NULL,o,o->type == REDIS_STRING);


    // 只在字符串的编码为 RAW 或者 EMBSTR 时尝试进行编码
    if (!sdsEncodedObject(o)) return o;

    // 不对共享对象进行编码
    if (o->refcount > 1) return o;

    // 对字符串进行检查
    // 只对长度小于或等于 21 字节，并且可以被解释为整数的字符串进行编码
    len = sdslen(s);
    if (len <= 21 && string2l(s,len,&value)) {
        if (server.maxmemory == 0 &&
            value >= 0 &&
            value < REDIS_SHARED_INTEGERS)
        {
            decrRefCount(o);
            incrRefCount(shared.integers[value]);
            return shared.integers[value];
        } else {
            if (o->encoding == REDIS_ENCODING_RAW) sdsfree(o->ptr);
            o->encoding = REDIS_ENCODING_INT;
            o->ptr = (void*) value;
            return o;
        }
    }

    // 尝试将 RAW 编码的字符串编码为 EMBSTR 编码
    if (len <= REDIS_ENCODING_EMBSTR_SIZE_LIMIT) {
        robj *emb;

        if (o->encoding == REDIS_ENCODING_EMBSTR) return o;
        emb = createEmbeddedStringObject(s,sdslen(s));
        decrRefCount(o);
        return emb;
    }

    // 这个对象没办法进行编码，尝试从 SDS 中移除所有空余空间
    if (o->encoding == REDIS_ENCODING_RAW &&
        sdsavail(s) > len/10)
    {
        o->ptr = sdsRemoveFreeSpace(o->ptr);
    }

    /* Return the original object. */
    return o;
}

/*
 * 以新对象的形式，返回一个输入对象的解码版本(RAW编码）
 * 如果对象已经是RAW编码，那么对输入对象的引用记数增1
 * 然后返回输入对象
 */
robj *getDecodeObject(robj *o) {
    robj *dec;

    if (sdsEncodedObject(o)) {
        incrRefCount(o);
        return o;
    }

    //解码对象，将对象的值从整数转换为字符串
    if (o->type == REDIS_STRING && o->encoding == REDIS_ENCODING_INT) {
        char buf[32];

        ll2string(buf, 32, (long)o->ptr);
        dec = createStringObject(buf, strlen(buf));
        return dec;
    } else {
        redisPanic("Unknown encoding type");
    }

}

/*
 * 根据flags的值，决定是使用strcmp()或者strcoll()来对比字符串对象
 * 如果字符串对象保存的是整数值，那么要先将整数转换为字符串，再对比两个字符串
 * 当flags为REDIS_COMPARE_BINARY时，以二进制安全的方式进行
 */
#define REDIS_COMPARE_BINARY (1<<0)
#define REDIS_COMPARE_COLL (1<<1)

int compareStringObjectsWithFlags(robj *a, robj *b, int flags) {
    redisAssertWithInfo(NULL, a, a->type == REDIS_STRING && b->type == REDIS_STRING);

    char bufa[128],bufb[128],*astr, *bstr;
    size_t alen, blen, minlen;

    if (a == b) return 0;

    //指向字符串值，并在有需要时，将整数转换为字符串a
    if (sdsEncodedObject(a)) {
        astr = a->ptr;
        alen = saslen(astr);
    } else {
        alen = ll2string(bufa, sizeof(bufa), (long)a->ptr);
        astr = bufa;
    }

    //同样处理字符串b
    if (sdsEncodedObject(b)) {
        bstr = b->ptr;
        blen = sdslen(bstr);
    } else {
        blen = ll2string(bufb, sizeof(bufb), (long)b->ptr);
        bstr = bufb;
    }

    //对比
    if (flags & REDIS_COMPARE_COLL) {
        return strcoll(astr, bstr);
    } else {
        int cmp;

        minlen = (alen < blen) ? alen : blen;
        cmp = memcmp(astr, bstr, minlen);
        if (cmp == 0) return alen-blen;
        return cmp;
    }
}

//包裹函数
int compareStringObjects(robj *a, robj *b) {
    return compareStringObjectsWithFlags(a, b, REDIS_COMPARE_BINARY);
}

int collateStringObjects(rob *a, robj *b) {
    return compareStringObjectsWithFlags(a, b, REDIS_COMPARE_COLL);
}

//两个对象的值在字符串的形式上相等，那么返回1,否则返回0
int equalStringObjects(robj *a, robj *b) {
    if (a->encoding == REDIS_ENCODING_INT &&
            b->encoding == REDIS_ENCODING_INT){
        return a->ptr == b->ptr;
    } else {
        return compareStringObjects(a, b) == 0;
    }
}

//返回字符串对象中字符串值的长度
size_t stringObjectLen(robj *o) {
    redisAssertWithInfo(NULL, o, o->type == REDIS_STRING);

    if (sdsEncodeObject(o)) {
        return sdslen(o->ptr);

    //INT编码，计算将这个值转换为字符串要多少字节
    } else {
        char buf[32];
        return ll2string(buf, 32, (long)o->ptr);
    }
}

/*
 * 尝试从对象中取出double值
 * - 转换成功则将值保存在 *target中，函数返回REDIS_OK
 * - 否则，函数返回REDIS_ERR
 */
int getDoubleFromObject(robj *o, double *target) {
    double value;
    char *eptr;

    if (o == NULL) {
        value = 0;
    } else {
        redisAssertWithInfo(NULL, o, o->type == REDIS_STRING);

        //尝试从字符串中转换double值
        if (sdsEncodedObject(o)) {
            errno = 0;
            value = strtod(o->ptr, &eptr);
            if (isspace(((char*)o->ptr[0]) ||
                        eptr[0] != '\0' ||
                        (errno == ERANGE &&
                         (value == HUGE_VAL || value == -HUGE_VAL || value == 0)) ||
                        errno == EINVAL ||
                        isnan(value)))
                return REDIS_ERR;

        //INT编码
        } else if (o->encoding == REDIS_ENCODING_INT) {
            value = (long)o->ptr;
        } else {
            redisPanic("Unknown string encoding");
        }
    }

    *target = value;
    return REDIS_OK;
}

/*
 * 尝试从对象o中取出double值：
 * - 如果尝试失败的话，就返回指定的回复msg给客户端，函数返回REDIS_ERR
 * - 取出成功的话，将值保存在*target中，函数返回REDIS_OK
 */
int getDoubleFromObjectOrReply(redisClient *c, robj *o, double *target, const char *msg) {
    double value;

    if (getDoubleFromObject(o, &value) != REDIS_OK) {
        if (msg != NULL) {
            addReplyError(c, (char*)msg);
        } else {
            addReplyError(c, "value is not a valid float");
        }
        return REDIS_ERR;
    }

    *target = value;
    return REDIS_OK;
}

/*
 * 尝试从对象中取出long double值
 * - 转换成功则将值保存在 *target中，函数返回REDIS_OK
 * - 否则，函数返回REDIS_ERR
 */
int getLongDoubleFromObject(robj *o, long double *target) {
    long double value;
    char *eptr;

    if (o == NULL) {
        value = 0;
    } else {
        redisAssertWithInfo(NULL, o, o->type == REDIS_STRING);

        //RAW编码，尝试从字符串转换long double
        if (sdsEncodeObject(o)) {
            errno = 0;
            value = strtold(o->ptr, &eptr);
            if (isspace*(((char *)o->ptr)[0]) || eptr[0] != '\0' ||
                    errno == ERANGE || isnan(value))
                return REDIS_ERR;

        INT编码，直接保存
        } else if (o->encoding == REDIS_ENCODING_INT) {
            value = (long)0->ptr;
        } else {
            redisPanic("Unknown string encoding");
        }
    }

    *target = value;
    return REDIS_OK;
}

/*
 * 尝试从对象 o 中取出 long double 值：
 *
 *  - 如果尝试失败的话，就返回指定的回复 msg 给客户端，函数返回 REDIS_ERR 。
 *
 *  - 取出成功的话，将值保存在 *target 中，函数返回 REDIS_OK 。
 */
int getLongDoubleFromObjectOrReply(redisClient *c, robj *o, long double *target, const char *msg) {

    long double value;

    if (getLongDoubleFromObject(o, &value) != REDIS_OK) {
        if (msg != NULL) {
            addReplyError(c,(char*)msg);
        } else {
            addReplyError(c,"value is not a valid float");
        }
        return REDIS_ERR;
    }

    *target = value;
    return REDIS_OK;
}

/*
 * 尝试从对象 o 中取出整数值，
 * 或者尝试将对象 o 所保存的值转换为整数值，
 * 并将这个整数值保存到 *target 中。
 *
 * 如果 o 为 NULL ，那么将 *target 设为 0 。
 *
 * 如果对象 o 中的值不是整数，并且不能转换为整数，那么函数返回 REDIS_ERR 。
 *
 * 成功取出或者成功进行转换时，返回 REDIS_OK 。
 *
 * T = O(N)
 */
int getLongLongFromObject(robj *o, long long *target) {
    long long value;
    char *eptr;

    if (o == NULL) {
        // o 为 NULL 时，将值设为 0 。
        value = 0;
    } else {

        // 确保对象为 REDIS_STRING 类型
        redisAssertWithInfo(NULL,o,o->type == REDIS_STRING);
        if (sdsEncodedObject(o)) {
            errno = 0;
            // T = O(N)
            value = strtoll(o->ptr, &eptr, 10);
            if (isspace(((char*)o->ptr)[0]) || eptr[0] != '\0' ||
                errno == ERANGE)
                return REDIS_ERR;
        } else if (o->encoding == REDIS_ENCODING_INT) {
            // 对于 REDIS_ENCODING_INT 编码的整数值
            // 直接将它的值保存到 value 中
            value = (long)o->ptr;
        } else {
            redisPanic("Unknown string encoding");
        }
    }

    // 保存值到指针
    if (target) *target = value;

    // 返回结果标识符
    return REDIS_OK;
}

/*
 * 尝试从对象 o 中取出整数值，
 * 或者尝试将对象 o 中的值转换为整数值，
 * 并将这个得出的整数值保存到 *target 。
 *
 * 如果取出/转换成功的话，返回 REDIS_OK 。
 * 否则，返回 REDIS_ERR ，并向客户端发送一条出错回复。
 *
 * T = O(N)
 */
int getLongLongFromObjectOrReply(redisClient *c, robj *o, long long *target, const char *msg) {

    long long value;

    // T = O(N)
    if (getLongLongFromObject(o, &value) != REDIS_OK) {
        if (msg != NULL) {
            addReplyError(c,(char*)msg);
        } else {
            addReplyError(c,"value is not an integer or out of range");
        }
        return REDIS_ERR;
    }

    *target = value;

    return REDIS_OK;
}

/*
 * 尝试从对象 o 中取出 long 类型值，
 * 或者尝试将对象 o 中的值转换为 long 类型值，
 * 并将这个得出的整数值保存到 *target 。
 *
 * 如果取出/转换成功的话，返回 REDIS_OK 。
 * 否则，返回 REDIS_ERR ，并向客户端发送一条 msg 出错回复。
 */
int getLongFromObjectOrReply(redisClient *c, robj *o, long *target, const char *msg) {
    long long value;

    // 先尝试以 long long 类型取出值
    if (getLongLongFromObjectOrReply(c, o, &value, msg) != REDIS_OK) return REDIS_ERR;

    // 然后检查值是否在 long 类型的范围之内
    if (value < LONG_MIN || value > LONG_MAX) {
        if (msg != NULL) {
            addReplyError(c,(char*)msg);
        } else {
            addReplyError(c,"value is out of range");
        }
        return REDIS_ERR;
    }

    *target = value;
    return REDIS_OK;
}

/*
 * 返回编码的字符串表示
 */
char *strEncoding(int encoding) {

    switch(encoding) {

    case REDIS_ENCODING_RAW: return "raw";
    case REDIS_ENCODING_INT: return "int";
    case REDIS_ENCODING_HT: return "hashtable";
    case REDIS_ENCODING_LINKEDLIST: return "linkedlist";
    case REDIS_ENCODING_ZIPLIST: return "ziplist";
    case REDIS_ENCODING_INTSET: return "intset";
    case REDIS_ENCODING_SKIPLIST: return "skiplist";
    case REDIS_ENCODING_EMBSTR: return "embstr";
    default: return "unknown";
    }
}

/* Given an object returns the min number of milliseconds the object was never
 * requested, using an approximated LRU algorithm. */
// 使用近似 LRU 算法，计算出给定对象的闲置时长
unsigned long long estimateObjectIdleTime(robj *o) {
    unsigned long long lruclock = LRU_CLOCK();
    if (lruclock >= o->lru) {
        return (lruclock - o->lru) * REDIS_LRU_CLOCK_RESOLUTION;
    } else {
        return (lruclock + (REDIS_LRU_CLOCK_MAX - o->lru)) *
                    REDIS_LRU_CLOCK_RESOLUTION;
    }
}

/* This is a helper function for the OBJECT command. We need to lookup keys
 * without any modification of LRU or other parameters.
 *
 * OBJECT 命令的辅助函数，用于在不修改 LRU 时间的情况下，尝试获取 key 对象
 */
robj *objectCommandLookup(redisClient *c, robj *key) {
    dictEntry *de;

    if ((de = dictFind(c->db->dict,key->ptr)) == NULL) return NULL;
    return (robj*) dictGetVal(de);
}

/*
 * 在不修改 LRU 时间的情况下，获取 key 对应的对象。
 *
 * 如果对象不存在，那么向客户端发送回复 reply 。
 */
robj *objectCommandLookupOrReply(redisClient *c, robj *key, robj *reply) {
    robj *o = objectCommandLookup(c,key);

    if (!o) addReply(c, reply);
    return o;
}

/* Object command allows to inspect the internals of an Redis Object.
 * Usage: OBJECT <verb> ... arguments ... */
void objectCommand(redisClient *c) {
    robj *o;

    // 返回对戏哪个的引用计数
    if (!strcasecmp(c->argv[1]->ptr,"refcount") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.nullbulk))
                == NULL) return;
        addReplyLongLong(c,o->refcount);

    // 返回对象的编码
    } else if (!strcasecmp(c->argv[1]->ptr,"encoding") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.nullbulk))
                == NULL) return;
        addReplyBulkCString(c,strEncoding(o->encoding));

    // 返回对象的空闲时间
    } else if (!strcasecmp(c->argv[1]->ptr,"idletime") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.nullbulk))
                == NULL) return;
        addReplyLongLong(c,estimateObjectIdleTime(o)/1000);
    } else {
        addReplyError(c,"Syntax error. Try OBJECT (refcount|encoding|idletime)");
    }
}
