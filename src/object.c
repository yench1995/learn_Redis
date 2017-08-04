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
