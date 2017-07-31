#include <stdint.h>

#ifndef __DICT_H_
#define __DICT_H_

//字典操作成功
#define DICT_OK 0
//操作失败
#define DICT_ERR 1

//如果字典的私有数据不使用时，用这个宏来避免编译器错误
#define DICT_NOTUSED(V) ((void) V)

//哈希表节点
typedef struct dictEntry {
    void *key;

    union {
        void *val;
        uint64_t u64;
        int64_t s64;
    } v;

    //指向下个哈希表节点，形成链表解决键值冲突问题
    struct dictEntry *next;
} dictEntry;

//字典类型特定函数
typedef struct dictType {

    //计算哈希值的函数
    unsigned int (*hashFunction)(const void *key);

    //复制键的函数
    void *(*keyDup)(void *privdata, const void *key);

    //复制值的函数
    void *(*valDup)(void *privdata, const void *obj);

    //对比键的函数
    int (*keyCompare)(void *privdata, const void *key1, const void *key2);

    //销毁键的函数
    void (*keyDestructor)(void *privdata, void *key);

    //销毁值的函数
    void (*valDestructor)(void *privdata, void *obj);
} dictType;


//哈希表
typedef struct dictht {

    //哈希表数组
    dictEntry **table;

    //哈希表大小
    unsigned long size;

    //哈希表大小掩码，用于计算索引值，总是等于size-1
    unsigned long sizemask;

    //该哈希表已有节点的数量
    unsigned long used;
} dictht;


//字典
typedef struct dict {

    //类型特定函数
    dictType *type;

    //私有数据, 保存了需要传给特定函数的参数
    void *privdata;

    //哈希表
    //一般情况下，字典只使用ht[0]哈希表，ht[1]在堆哈希表进行rehash时使用
    dictht ht[2];

    //rehash索引，当rehash不在进行时，值为-1
    int rehashidx;

    //目前正在运行的安全迭代器的数量
    int iterators;
} dict;

/*
 * Redis计算哈希值和索引值的方法如下
 *
 * #使用字典设置的哈希函数，计算键key的哈希值
 * #目前Redis使用MurmurHash2算法来计算键的哈希值
 * hash = dict->type->hashFunction(key);
 *
 * #使用哈希表的sizemask属性和哈希值，计算出索引值
 * #ht[x]可以使ht[0]或ht[1]
 * index = hash & dict->ht[x].sizemask;
 */

/*
 * 哈希表渐进式rehash
 * 1.为ht[1]分配空间，让字典同时持有ht[0]和ht[1]两个哈希表
 * 2.在字典中维持一个索引计数器变量rehashids，并置为0
 * 3.每次对字典执行增删改查时，会将ht[0]哈希表在rehashidx上的所有键值对
 *      rehash到ht[1]中，每次完成后，rehashidx++(增加操作永远在ht[1]上）
 * 4.最后全部完成后，将rehashidx值设为-1
 */

/*
 * 字典迭代器
 *
 * 如果safe的值为1,那么在迭代进行的过程中，
 * 程序仍然可以执行dictAdd, dictFind和其他函数，对字典进行修改
 *
 * 如果safe不为1, 那么程序只会调用dictNext对字典进行迭代，而不能修改
 */
typedef struct dictIterator {

    //被迭代的字典
    dict *d;

    //table:正在被迭代的哈希表号码
    //index:迭代器当前所指向的哈希表索引位置
    int table, index, safe;

    //entry:当前迭代到的节点的指针
    //nextEntry:当前迭代的下一个节点，因为在安全迭代的时候，entry所指向的节点
    //          可能会被修改，所以需要额外的指针
    dictEntry *entry, *nextEntry;

    long long fingerprint;
} dictIterator;

typedef void (dictScanFunction)(void *privdata, const dictEntry *de);

//哈希表的初始大小
//大小扩容或者收缩都为2^n
#define DICT_HT_INITIAL_SIZE   4



/* ------------------------------- Macros ------------------------------------*/
// 释放给定字典节点的值
#define dictFreeVal(d, entry) \
    if ((d)->type->valDestructor) \
        (d)->type->valDestructor((d)->privdata, (entry)->v.val)

// 设置给定字典节点的值
#define dictSetVal(d, entry, _val_) do { \
    if ((d)->type->valDup) \
        entry->v.val = (d)->type->valDup((d)->privdata, _val_); \
    else \
        entry->v.val = (_val_); \
} while(0)

// 将一个有符号整数设为节点的值
#define dictSetSignedIntegerVal(entry, _val_) \
    do { entry->v.s64 = _val_; } while(0)

// 将一个无符号整数设为节点的值
#define dictSetUnsignedIntegerVal(entry, _val_) \
    do { entry->v.u64 = _val_; } while(0)

// 释放给定字典节点的键
#define dictFreeKey(d, entry) \
    if ((d)->type->keyDestructor) \
        (d)->type->keyDestructor((d)->privdata, (entry)->key)

// 设置给定字典节点的键
#define dictSetKey(d, entry, _key_) do { \
    if ((d)->type->keyDup) \
        entry->key = (d)->type->keyDup((d)->privdata, _key_); \
    else \
        entry->key = (_key_); \
} while(0)

// 比对两个键
#define dictCompareKeys(d, key1, key2) \
    (((d)->type->keyCompare) ? \
        (d)->type->keyCompare((d)->privdata, key1, key2) : \
        (key1) == (key2))

// 计算给定键的哈希值
#define dictHashKey(d, key) (d)->type->hashFunction(key)
// 返回获取给定节点的键
#define dictGetKey(he) ((he)->key)
// 返回获取给定节点的值
#define dictGetVal(he) ((he)->v.val)
// 返回获取给定节点的有符号整数值
#define dictGetSignedIntegerVal(he) ((he)->v.s64)
// 返回给定节点的无符号整数值
#define dictGetUnsignedIntegerVal(he) ((he)->v.u64)
// 返回给定字典的大小
#define dictSlots(d) ((d)->ht[0].size+(d)->ht[1].size)
// 返回字典的已有节点数量
#define dictSize(d) ((d)->ht[0].used+(d)->ht[1].used)
// 查看字典是否正在 rehash
#define dictIsRehashing(ht) ((ht)->rehashidx != -1)

/* API */
dict *dictCreate(dictType *type, void *privDataPtr);
int dictExpand(dict *d, unsigned long size);
int dictAdd(dict *d, void *key, void *val);
dictEntry *dictAddRaw(dict *d, void *key);
int dictReplace(dict *d, void *key, void *val);
dictEntry *dictReplaceRaw(dict *d, void *key);
int dictDelete(dict *d, const void *key);
int dictDeleteNoFree(dict *d, const void *key);
void dictRelease(dict *d);
dictEntry * dictFind(dict *d, const void *key);
void *dictFetchValue(dict *d, const void *key);
int dictResize(dict *d);
dictIterator *dictGetIterator(dict *d);
dictIterator *dictGetSafeIterator(dict *d);
dictEntry *dictNext(dictIterator *iter);
void dictReleaseIterator(dictIterator *iter);
dictEntry *dictGetRandomKey(dict *d);
int dictGetRandomKeys(dict *d, dictEntry **des, int count);
void dictPrintStats(dict *d);
unsigned int dictGenHashFunction(const void *key, int len);
unsigned int dictGenCaseHashFunction(const unsigned char *buf, int len);
void dictEmpty(dict *d, void(callback)(void*));
void dictEnableResize(void);
void dictDisableResize(void);
int dictRehash(dict *d, int n);
int dictRehashMilliseconds(dict *d, int ms);
void dictSetHashFunctionSeed(unsigned int initval);
unsigned int dictGetHashFunctionSeed(void);
unsigned long dictScan(dict *d, unsigned long v, dictScanFunction *fn, void *privdata);

/* Hash table types */
extern dictType dictTypeHeapStringCopyKey;
extern dictType dictTypeHeapStrings;
extern dictType dictTypeHeapStringCopyKeyValue;

#endif /* __DICT_H */

