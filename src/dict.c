#include "fmacros.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <limits.h>
#include <sys/time.h>
#include <ctype.h>

#include "dict.h"
#include "zmalloc.h"
#include "redisassert.h"

//指示字典是否启用rehash标识
static int dict_can_resize = 1;
//强制rehash的比例
static unsigned int dict_force_resize_ratio = 5;

/* -------------------------- private prototypes ---------------------------- */

static int _dictExpandIfNeeded(dict *ht);
static unsigned long _dictNextPower(unsigned long size);
static int _dictKeyIndex(dict *ht, const void *key);
static int _dictInit(dict *ht, dictType *type, void *privDataPtr);

/* -------------------------- hash functions -------------------------------- */

/* Thomas Wang's 32 bit Mix Function */
unsigned int dictIntHashFunction(unsigned int key)
{
    key += ~(key << 15);
    key ^=  (key >> 10);
    key +=  (key << 3);
    key ^=  (key >> 6);
    key += ~(key << 11);
    key ^=  (key >> 16);
    return key;
}

/* Identity hash function for integer keys */
unsigned int dictIdentityHashFunction(unsigned int key)
{
    return key;
}

static uint32_t dict_hash_function_seed = 5381;

void dictSetHashFunctionSeed(uint32_t seed) {
    dict_hash_function_seed = seed;
}

uint32_t dictGetHashFunctionSeed(void) {
    return dict_hash_function_seed;
}

/* MurmurHash2, by Austin Appleby
 * Note - This code makes a few assumptions about how your machine behaves -
 * 1. We can read a 4-byte value from any address without crashing
 * 2. sizeof(int) == 4
 *
 * And it has a few limitations -
 *
 * 1. It will not work incrementally.
 * 2. It will not produce the same results on little-endian and big-endian
 *    machines.
 */
unsigned int dictGenHashFunction(const void *key, int len) {
    /* 'm' and 'r' are mixing constants generated offline.
     They're not really 'magic', they just happen to work well.  */
    uint32_t seed = dict_hash_function_seed;
    const uint32_t m = 0x5bd1e995;
    const int r = 24;

    /* Initialize the hash to a 'random' value */
    uint32_t h = seed ^ len;

    /* Mix 4 bytes at a time into the hash */
    const unsigned char *data = (const unsigned char *)key;

    while(len >= 4) {
        uint32_t k = *(uint32_t*)data;

        k *= m;
        k ^= k >> r;
        k *= m;

        h *= m;
        h ^= k;

        data += 4;
        len -= 4;
    }

    /* Handle the last few bytes of the input array  */
    switch(len) {
    case 3: h ^= data[2] << 16;
    case 2: h ^= data[1] << 8;
    case 1: h ^= data[0]; h *= m;
    };

    /* Do a few final mixes of the hash to ensure the last few
     * bytes are well-incorporated. */
    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;

    return (unsigned int)h;
}

/* And a case insensitive hash function (based on djb hash) */
unsigned int dictGenCaseHashFunction(const unsigned char *buf, int len) {
    unsigned int hash = (unsigned int)dict_hash_function_seed;

    while (len--)
        hash = ((hash << 5) + hash) + (tolower(*buf++)); /* hash * 33 + c */
    return hash;
}

/* ----------------------------- API implementation ------------------------- */

//重置或初始化给定哈希表的各项属性值
static void _dictReset(dictht *ht)
{
    ht->table = NULL;
    ht->size = 0;
    ht->sizemask = 0;
    ht->used = 0;
}

//创建一个新的字典
dict *dictCreate(dictType *type, void *privDataPtr)
{
    dict *d = (dict *)zmalloc(sizeof(*d));

    _dictInit(d, type, privDataPtr);
    return d;
}

//初始化哈希表
int _dictInit(dict *d, dictType *type, void *privDataPtr)
{
    _dictReset(&d->ht[0]);
    _dictReset(&d->ht[1]);

    d->type = type;
    d->privdata = privDataPtr;
    d->rehashidx = -1;
    d->iterators = 0;

    return DICT_OK;
}

//缩小给定字典，让它的已用节点数和字典大小之间的比率接近1：1
//返回DICT_ERR表示字典已经在rehash,或者dict_can_resize为假
int dictResize(dict *d)
{
    int minimal;

    if (!dict_can_resize || dictIsRehashing(d)) return DICT_ERR;

    minimal = d->ht[0].used;
    if (minimal < DICT_HT_INITIAL_SIZE)
        minimal = DICT_HT_INITIAL_SIZE;

    return dictExpand(d, minimal);
}

/*
 * 创建一个新的哈希表，并根据字典的情况，选择一下其中一个动作进行：
 * 1. 如果字典的0号哈希表为空，那么将新哈希表设置为0号哈希表
 * 2. 如果字典的0号哈希表非空，那么将新哈希表设置为1号哈希表,
 *    并打开字典的rehash标识，使得程序可以开始对字典进行rehash
 *
 * size参数不够大，或者rehash已经在进行时，返回DICT_ERR
 */
int dictExpand(dict *d, unsigned long size)
{
    dictnt n;
    //根据size参数，计算哈希表的大小
    unsigned long realsize = _dictNextPower(size);

    if (dictIsRehashing(d) || d->ht[0].used > size)
        return DICT_ERR;

    //为哈希表分配空间，并将所有指针指向NULL
    n.size = realsize;
    n.sizemask = realsize-1;
    n.table = zcalloc(realsize * sizeof(dictEntry*));
    n.used = 0;

    //如果0号哈希表为空，那么这是一次初始化
    if (d->ht[0].table == NULL) {
        d->ht[0] = n;
        return DICT_OK;
    }

    //如果0号哈希表非空，那么这是一次rehash
    d->ht[1] = n;
    d->rehashidx = 0;
    return DICT_OK;
}

/*
 * 执行N步渐进式rehash
 *
 * 返回1表示仍需要从0号哈希表迁移到1号哈希表
 * 返回0则表示所有键已迁移完毕
 */
int dictRehash(dict *d, int n)
{
    if (!dictIsRehashing(d)) return 0;

    while (n--) {
        dictEntry *de, *nextde;

        if (d->ht[0].used == 0) {
            zfree(d->ht[0].table);
            d->ht[0] = d->ht[1];
            _dictReset(&d->ht[1]);
            d->rehashidx = -1;
            return 0;
        }

        assert(d->ht[0].size > (unsigned)d->rehashidx);

        //找到下一个非空索引
        while (d->ht[0].table[d->rehashidx] == NULL) d->rehashidx+;

        //指向该索引的链表表头节点
        de = d->ht[0].table[d->rehashidx];

        while (de) {
            unsigned int h;

            nextde = de->next;
            //计算新哈希表的哈希值，以及节点插入的索引位置
            h = dictHashKey(d, de->key) & d->ht[1].sizemask;

            //为了效率，插入到新哈希表索引位置的头部
            de->next = d->ht[1].table[h];
            d->ht[1].table[h] = de;

            d->ht[0].used--;
            d->ht[1].used++;

            de = nextde;
        }

        d->ht[0].table[d->rehashidx] = NULL;
        d->rehashidx++;
    }

    return 1;
}

//返回以毫秒为单位的UNIX时间戳
long long timeInMilliseconds(void) {
    struct timeval tv;

    gettimeofday(&tv, NULL);
    return (((long long)tv.tv_sec)*1000) + (tv.tv_usec/1000);
}

//在给定毫秒数内，以100步为单位，对字典进行rehash
int dictRehashMilliseconds(dict *d, int ms)
{
    long long start = timeInMilliseconds();
    int rehashes = 0;

    while (dictRehash(d, 100)) {
        rehashes += 100;
        if (timeInMilliseconds()-start > ms)
            break;
    }
}

//在字典不存在安全迭代器的情况下，对字典进行单步rehash
//字典有安全迭代器的情况下不能进行rehash
static void _dictRehashStep(dict *d) {
    if (d->iterators == 0) dictRehash(d, 1);
}

//尝试将给定键值添加到字典中
int dictAdd(dict *d, void *key, void &val)
{
    dictEntry *entry = dictAddRaw(d, key);
    if (!entry) return DICT_ERR;
    dictSetVal(d, entry, val);
    return DICT_OK;
}

//尝试将键插入到字典中
dictEntry *dictAddRaw(dict *d, void *key)
{
    int index;
    dictEntry *entry;
    dictht *ht;

    //如果条件允许的话，执行单步rehash
    if (dictIsRehashing(d)) _dictRehashStep(d);

    //计算键在哈希表中的索引值，若为-1,则已存在
    if ((index = _dictKeyIndex(d, key)) == -1)
        return NULL;

    //如果正在rehash,则添加到ht[1]
    ht = dictIsRehashing(d)? &d->ht[1] : &d->ht[0];
    entry = zmalloc(sizeof(*entry));
    entry->next = ht->table[index];
    ht->table[index] = entry;
    ht->used++;

    dictSetKey(d, entry, key);
    return entry;
}

/*
 * 将给定的键值对添加到字典中，如果键已经存在，那么删除旧有的键值对
 * 如果键值为全新添加，那么返回1
 * 如果是更新得来，那么返回0
 */
int dictReplace(dict *d, void *key, void *val)
{
    dictEntry *entry, auxentry;

    if (dictAdd(d, key, val) == DICT_OK)
        return 1;

    entry = dictFind(d, key);
    //先保存原有的值，再设置新的值
    //这很重要因为两者有可能相同的值，如引用记数
    auxentry = *entry;
    dictSetVal(d, entry, val);
    dictFreeVal(d, &auxentry);

    return 0;
}

dictEntry *dictReplaceRaw(dict *d, void *key)
{
    dictEntry *entry = dictFind(d, key);

    //如果节点找到了直接返回节点，否则添加并返回一个新节点
    return entry ? entry:dictAddRaw(d, key);
}

/*
 * 查找并删除给定键的节点
 *
 * 参数 nofree 决定是否调用键和值的释放函数
 * 0表示调用，1表示不调用
 */
static int dictGenericDelete(dict *d, const void *key, int nofree)
{
    unsigned int h, idx;
    dictEntry *he, *prevHe;
    int table;

    //计算哈希值
    h = dictHashKey(d, key);

    for (table = 0; table <= 1; table++) {
        idx = h & d->ht[table].sizemask;
        he = d->ht[table].table[idx];
        prevHe = NULL;

        while (he) {
            if (dictCompareKeys(d, key, he->key)) {
                if (prevHe)
                    prevHe->next = he->next;
                else
                    d->ht[table].table[idx] = he->next;

                if (!nofree) {
                    dictFreeKey(d, he);
                    dictFreeVal(d, he);
                }

                zfree(he);

                d->ht[table].used--;
                return DICT_OK;
            }

            prevHe = he;
            he = he->next;
        }

        //如果执行到这里，说明0号找不到
        //根据字典是否在rehash, 决定是否查找1号
        if (!dictIsRehashing(d)) break;
    }

    return DICT_ERR;
}

//从字典中删除包含给定键的节点
int dictDelete(dict *ht, const void *key) {
    return dictGenericDelete(ht, key, 0);
}

int dictDeleteNoFree(dict *ht, const void *key)
{
    return dictGenericDelete(ht, key, 1);
}

//删除哈希表上的所有节点，并重置哈希表的各项属性
int _dictClear(dict *d, dictht *ht, void(callback)(void *)) {
    unsigned long i;

    for (i = 0; i < ht->size && ht->used>0; ++i) {
        dictEntry *he, *nextHe;

        //销毁私有数据
        if (callback && (i&65535) == 0) callback(d->privdata);

        if ((he = ht->table[i]) == NULL) continue;

        while (he) {
            nextHe = he->next;
            dictFreeKey(d, he);
            dictFreeVal(d, he);
            zfree(he);

            ht->used--;
            he = nextHe;
        }
    }
    zfree(ht->table);
    _dictReset(ht);
    return DICT_OK;
}

//删除并释放整个字典
void dictRelease(dict *d)
{
    _dictClear(d, &d->ht[0], NULL);
    _dictClear(d, &d->ht[1], NULL);
    zfree(d);
}

//返回字典中包含键key的节点
dictEntry *dictFind(dict *d, const void *key)
{
    dictEntry *he;
    unsigned int h, idx, table;

    if (d->ht[0].size == 0) return NULL;

    if (dictIsRehashing(d)) _dictRehashStep(d);
    h = dictHashKey(d, key);
    for (table = 0; table <= 1; table++) {
        idx = h & d->ht[table].sizemask;

        he = d->ht[table].table[idx];
        while (he) {
            if (dictCompareKeys(d, key, he->key))
                return he;

            he = he->next;
        }

        if (!dictIsRehashing(d)) return NULL;
    }
    return NULL;
}

//获取包含给定键的节点的值
void *dictFetchValue(dict *d, const void *key)
{
    dictEntry *he;
    he = dictFind(d, key);
    return he ? dictGetVal(he):NULL;
}

/*
 *  一个fingerprint为一个64位数值,用来表示某个时刻dict的状态，它由dict的一些属性通过位操作计算得到。
 *  当一个非安全迭代器初始后, 会产生一个fingerprint值。 在该迭代器被释放时会重新检查这个fingerprint值。
 *  如果前后两个fingerprint值不一致,说明在迭代字典时iterator执行了某些非法操作。
 */
long long dictFingerprint(dict *d) {
    long long integers[6], hash = 0;
    int j;

    integers[0] = (long) d->ht[0].table;
    integers[1] = d->ht[0].size;
    integers[2] = d->ht[0].used;
    integers[3] = (long) d->ht[1].table;
    integers[4] = d->ht[1].size;
    integers[5] = d->ht[1].used;

    /* We hash N integers by summing every successive integer with the integer
     * hashing of the previous sum. Basically:
     *
     * Result = hash(hash(hash(int1)+int2)+int3) ...
     *
     * This way the same set of integers in a different order will (likely) hash
     * to a different number. */
    for (j = 0; j < 6; j++) {
        hash += integers[j];
        /* For the hashing step we use Tomas Wang's 64 bit integer hash. */
        hash = (~hash) + (hash << 21); // hash = (hash << 21) - hash - 1;
        hash = hash ^ (hash >> 24);
        hash = (hash + (hash << 3)) + (hash << 8); // hash * 265
        hash = hash ^ (hash >> 14);
        hash = (hash + (hash << 2)) + (hash << 4); // hash * 21
        hash = hash ^ (hash >> 28);
        hash = hash + (hash << 31);
    }
    return hash;
}

//创建并返回给定字典的不安全迭代器
dictIterator *dictGetIterator(dict *d)
{
    dictIterator *iter = zmalloc(sizeof(*iter));

    iter->d = d;
    iter->table = 0;
    iter->index = -1;
    iter->safe = 0;
    iter->entry = NULL;
    iter->nextEntry = NULL;

    return iter;
}

//创建并返回给定字典的安全迭代器
dictIterator *dictGetSafeIterator(dict *d) {
    dictIterator *i = dictGetIterator(d);
    i->safe = 1;

    return i;
}

//返回迭代器的当前节点
dictEntry *dictNext(dictIterator *iter)
{
    while (1) {

        //进入这个循环有两种可能
        //1是迭代器第一次运行
        //2是索引链表中的节点已经迭代完
        if (iter->entry == NULL) {
            dictht *ht = &iter->d->ht[iter->table];

            if (iter->index == -1 && iter->table == 0) {
                if (iter->safe)
                    iter->d->iterators++;
                //如果是不安全迭代器，那么计算指纹
                else
                    iter->fingerprint = dictFingerprint(iter->d);
            }
            //更新索引
            iter->index++;

            if (iter->index >= (signed)ht->size) {
                if (dictIsRehashing(iter->d) && iter->table == 0) {
                    iter->table++;
                    iter->index = 0;
                    ht = &iter->d->ht[1];
                } else {
                    break;
                }
            }

            iter->entry = ht->table[iter->index];
        } else {
            iter->entry = iter->nextEntry;
        }

        //如果当前节点不为空，那么也记录下该节点的下个节点
        //因为安全迭代器有可能会将迭代器返回的当前节点删除
        if (iter->entry) {
            iter->nextEntry = iter->entry->next;
            return iter->entry;
        }
    }
    return NULL;
}

//释放给定字典迭代器
void dictReleaseIterator(dictIterator *iter)
{
    if (!(iter->index == -1 && iter->table == 0)) {
        //释放安全迭代器时，安全迭代器计数器-1
        if (iter->safe)
            iter->d->iterators--;
        //释放不安全迭代器时，验证指纹是否有变化
        else
            assert(iter->fingerprint == dictFingerprint(iter->d));
    }
    zfree(iter);
}

/*
 * 随机返回字典中任意一个节点
 *
 * 可用于实现随机化算法
 */
dictEntry *dictGetRandomKey(dict *d)
{
    dictEntry *he, *orighe;
    unsigned int h;
    int listlen, listele;

    if (dictSize(d) == 0) return NULL;

    if (dictIsRehashing(d)) _dictRehashStep(d);

    //如果正在rehash,那么将1号哈希表也作为随机查找的目标
    if (dictIsRehashing(d)) {
        do {
            h = random() % (d->ht[0].size + d->ht[1].size);
            he = (h > d->ht[0].size) ? d->ht[1].table[h-d->ht[0].size] :
                                       d->ht[0].table[h];
        } while (he == NULL);
    } else {
        do {
            h = random() & d->ht[0].sizemask;
            he = d->ht[0].table[h];
        } while (he == NULL);
    }

    //目前he已经指向一个非空的节点链表
    //程序将从这个链表随机返回一个节点
    listlen = 0;
    orighe = he;
    while (he) {
        he = he->next;
        listlen++;
    }

    listele = random() % listlen;
    he = orighe;
    while (listele--) he = he->next;

    return he;
}

//反转二进制位
static unsigned long rev(unsigned long v) {
    unsigned long s = 8 * sizeof(v); // bit size; must be power of 2
    unsigned long mask = ~0;
    while ((s >>= 1) > 0) {
        mask ^= (mask << s);
        v = ((v >> s) & mask) | ((v << s) & ~mask);
    }
    return v;
}

//用于迭代给定字典中的元素
unsigned long dictScan(dict *d,
                       unsigned long v,
                       dictScanFunction *fn,
                       void *privdata)
{
    dictht *t0, *t1;
    const dictEntry *de;
    unsigned long m0, m1;

    // 跳过空字典
    if (dictSize(d) == 0) return 0;

    // 迭代只有一个哈希表的字典
    if (!dictIsRehashing(d)) {

        // 指向哈希表
        t0 = &(d->ht[0]);

        // 记录 mask
        m0 = t0->sizemask;

        /* Emit entries at cursor */
        // 指向哈希桶
        de = t0->table[v & m0];
        // 遍历桶中的所有节点
        while (de) {
            fn(privdata, de);
            de = de->next;
        }

    // 迭代有两个哈希表的字典
    } else {

        // 指向两个哈希表
        t0 = &d->ht[0];
        t1 = &d->ht[1];

        /* Make sure t0 is the smaller and t1 is the bigger table */
        // 确保 t0 比 t1 要小
        if (t0->size > t1->size) {
            t0 = &d->ht[1];
            t1 = &d->ht[0];
        }

        // 记录掩码
        m0 = t0->sizemask;
        m1 = t1->sizemask;

        /* Emit entries at cursor */
        // 指向桶，并迭代桶中的所有节点
        de = t0->table[v & m0];
        while (de) {
            fn(privdata, de);
            de = de->next;
        }

        /* Iterate over indices in larger table that are the expansion
         * of the index pointed to by the cursor in the smaller table */
        // Iterate over indices in larger table             // 迭代大表中的桶
        // that are the expansion of the index pointed to   // 这些桶被索引的 expansion 所指向
        // by the cursor in the smaller table               //
        do {
            /* Emit entries at cursor */
            // 指向桶，并迭代桶中的所有节点
            de = t1->table[v & m1];
            while (de) {
                fn(privdata, de);
                de = de->next;
            }

            /* Increment bits not covered by the smaller mask */
            v = (((v | m0) + 1) & ~m0) | (v & m0);

            /* Continue while bits covered by mask difference is non-zero */
        } while (v & (m0 ^ m1));
    }

    /* Set unmasked bits so incrementing the reversed cursor
     * operates on the masked bits of the smaller table */
    v |= ~m0;

    /* Increment the reverse cursor */
    v = rev(v);
    v++;
    v = rev(v);

    return v;
}


/* ------------------------- private functions ------------------------------ */

//根据需要，初始化字典，或者对字典进行扩展
static int _dictExpandIfNeeded(dict *d)
{
    if (dictIsRehashing(d)) reutrn DICT_OK;

    //如果字典的0号哈希表为空，那么创建并返回初始化大小的0号哈希表
    if (d->ht[0].size == 0) return dictExpand(d, DICT_HT_INITIAL_SIZE);

    //下面两个条件为真时，对字典进行扩展
    //字典已使用节点数和字典大小之间的比率接近1：1，并且dict_can_resize为真
    //已使用节点数和字典大小之间的比率超过dict_force_resize_ratio
    if (d->ht[0].used >= d->ht[0].size && (dict_can_resize ||
                d->ht[0].used/d->ht[0].size > dict_force_resize_ratio))
    {
        return dictExpand(d, d->ht[0].used * 2);
    }

    return DICT_OK;
}

//计算第一个大于等于size的2的N此方，用作哈希表的值
static unsigned long _dictNextPower(unsigned long size)
{
    unsigned long i = DICT_HT_INITIAL_SIZE;

    if (size >= LONG_MAX) return LONG_MAX;
    while (1) {
        if (i >= size)
            return i;
        i *= 2;
    }
}

/*
 * 返回可以将key插入到哈希表的索引位置
 * 如果key已经存在于哈希表，那么返回-1
 *
 * 注意，如果字典正在进行rehash,那么总是放那会1号哈希表的索引
 * 因为在字典进行rehash时，新节点总是插入到1号哈希表
 */
static int _dictKeyIndex(dict *d, const void *key)
{
    unsigned int h, idx, table;
    dictEntry *he;

    if (_dictExpandIfNeeded(d) == DICT_ERR)
        return -1;

    h = dictHashKey(d, key);
    for (table = 0; table <= 1; table++) {
        idx = h & d->ht[table].sizemask;
        he = d->ht[table].table[idx];
        while (he) {
            if (dictCompareKeys(d, key, he->key))
                return -1;
            he = he->next;
        }

        if (!dictIsRehashing(d)) break;
    }

    return idx;
}

//清空字典上的所有哈希表节点，并重置字典属性
void dictEmpty(dict *d, void(callback)(void *))
{
    _dictClear(d, &d->ht[0], callback);
    _dictClear(d, &d->ht[1], callback);
    d->rehashidx = -1;
    d->iterators = 0;
}

//开启自动rehash
void dictEnableResize(void) {
    dict_can_resize = 1;
}

//关闭自动rehash
void dictDisableResize(void) {
    dict_can_resize = 0;
}

#if 0
/*debugging part*/
#endif
