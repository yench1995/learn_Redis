#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>
#include "zmalloc.h"
#include "sds.h"


//O(1)时间返回保存的字符串的长度
static inline size_t sdslen(const sds s){
    struct sdshdr *sh = (void*)(s-(sizeof(struct sdshdr)));
    return sh->len;
}

//返回sds可用空间的长度
static inline size_t sdsavail(const sds s) {
    struct sdshdr *sh = (void *)(s-(sizeof(struct sdshdr)));
    return sh->free;
}

//根据给定的初始化字符串init和字符串长度initlen
//创建一个新的shshdr,并返回sds
sds sdsnewlen(const void *init, size_t initlen)
{
    struct sdshdr *sh;

    if (init) {
        //zmalloc不初始化分配的内存
        sh = zmalloc(sizeof(struct sdshdr) + initlen + 1);
    } else {
        //zcalloc将分配的内存全部初始化为0
        sh = zcalloc(sizeof(struct sdshdr) + initlen + 1);
    }

    if (sh == NULL) return NULL;

    sh->len = initlen;
    sh->free = 0;
    if (initlen && init)
        memcpy(sh->buf, init, initlen);
    //以\0结尾
    sh->buf[initlen] = '\0';

    return (char *)sh->buf;
}

sds sdsnew(const char *init) {
    size_t initlen = (init == NULL) ? 0 : strlen(init);
    return sdsnewlen(init, initlen);
}

void sdsfree(sds s) {
    if (s == NULL) return;
    zfree(s-sizeof(struct sdshdr));
}

/*
 * 在不释放SDS的字符串空间的情况下
 * 重置SDS所保存的字符串为空字符串
 */
void sdsclear(sds s) {
    struct sdshdr *sh = (void *) (s-(sizeof(struct sdshdr)));

    sh->free += sh->len;
    sh->len = 0;

    sh->buf[0] = '\0';
}

/*
 * 对sds中buf的长度进行扩展，确保在函数执行之后
 * buf至少会有addlen + 1长度的空余空间
 * 额外的一字节是为\0准备的
 */
sds sdsMakeRoomFor(sds s, size_t addlen)
{
    struct sdshdr *sh, *newsh;
    size_t free = sdsavail(s);

    size_t len, newlen;
    if (free >= addlen) return s;

    len = sdslen(s);
    sh = (void *)(s-(sizeof(struct sdshdr)));
    newlen = (len+addlen);

    if (newlen < SDS_MAX_PREALLOC)
        newlen *= 2;
    else
        newlen += SDS_MAX_PREALLOC;
    newsh = zrealloc(sh, sizeof(struct sdshdr) + newlen + 1);

    if (newsh == NULL) return NULL;

    newsh->free = newlen-len;

    return newsh->buf;
}

//回收sds中的空闲空间
sds sdsRemoveFreeSpace(sds s)
{
    struct sdshdr *sh;
    sh = (void *) (s-(sizeof(struct sdshdr)));

    sh = zrealloc(sh, sizeof(struct sdshdr)+sh->len+1);

    sh->free = 0;

    return sh->buf;
}

//在调用sdsmakeroomfor()函数后对字符串进行扩展
//正确更新free和len属性
void sdsIncrLen(sds s, int incr) {
    struct sdshdr *sh = (void *) (s-(sizeof(struct sdshdr)));

    assert(sh->free >= incr);

    sh->len += incr;
    sh->free -= incr;

    assert(sh->free >= 0);
    s[sh->len] = '\0';
}

//将sds扩充至指定长度，未使用的空间以0字节填充
sds sdsgrowzero(sds s, size_t len) {
    struct sdshdr *sh = (void *) (s-(sizeof(struct shshdr)));
    size_t totlen, curlen = sh->len;

    if (len <= curlen) return s;
    s = sdsMakeRoomFor(s, len-curlen);
    if (s == NULL) return NULL;

    sh = (void *) (s-(sizeof(struct shdhdr)));
    memset(s+curlen, 0, (len-curlen+1)); //算上最后字符串结束的'\0'
    totlen = sh->len + sh->free;
    sh->len = len;
    sh->free = totlen-sh->len;

    return s;
}

//将长度为len的字符串t追加到sds的字符串末尾
sds sdscatlen(sds s, const void *t, size_t len)
{
    struct sdshdr *sh;
    size_t curlen = sdslen(s);

    s = sdsMakeRoomFor(s, len);

    if (s == NULL) return NULL;

    sh = (void *)(s-(sizeof(struct shshdr)));
    memcpy(s+curlen, t, len);

    sh->len = curlen + len;
    sh->free = sh->free-len;

    s[curlen+len] = '\0';
    return s;
}

//将字符串t追加到sds的末尾
sds sdscat(sds s, const char *t)
{
    return sdscatlen(s, t, strlen(t));
}

//将字符串t的前len个字符复制到sds s当中
sds sdscpylen(sds s, const char *t, size_t len)
{
    struct sdshdr *sh = (void *) (s - (sizeof(struct sdshdr)));
    size_t totlen = sh->free+sh->len;

    if (totlen < len) {
        s = sdsMakeRoomFor(s, len-sh->len);
        if (s == NULL) return NULL;
        sh = (void *) (s-(sizeof(struct sdshdr)));
        totlen = sh->free + sh->len;
    }

    memcpy(s, t, len);
    s[len] = '\0';

    sh->len = len;
    sh->free = totlen-len;

    return s;
}

sds sdscpy(sds s, const char *t)
{
    return sdscpylen(s, t, strlen(t));
}

//对sds左右两端进行修剪，清除其中cast指定的所有字符
sds sdstrim(sds s, const char *cset)
{
    struct sdshdr *sh = (void *) (s-(sizeof(struct sdshdr)));
    char *start, *end, *sp, *ep;
    size_t len;

    ep = start = s;
    ep = end = s+sdslen(s)-1;

    while (sp <= end && strchr(cset, *sp)) sp++;
    while (ep > start && strchr(cset, *sp)) ep--;

    len = (sp > ep) ? 0:((ep-sp)+1);

    if (sh->buf != sp) memmove(sh->buf, sp, len);
    sh->buf[len] = '\0';

    sh->free = sh->free + (sh->len-len);
    sh->len = len;

    return s;
}

//按索引对截取sds字符串中的其中一段
//start和end都是闭区间
//索引可以是负数，sdslen(s)-1 == -1
void sdsrange(sds s, int start, int end)
{
    struct sdshdr *sh = (void *) (s-(sizeof(struct sdshdr)));
    size_t newlen, len = sdslen(s);

    if (len == 0) return;
    if (start < 0) {
        start = len + start;
        if (start < 0) start = 0;
    }
    if (end < 0) {
        end = len + end;
        if (end < 0) end = 0;
    }

    newlen = (start > end) ? 0 : (end-start)+1;
    if (newlen != 0) {
        if (start >= (signed)len) {
            newlen = 0;
        } else if (end >= (signed)len) {
            end = len-1;
            newlen = (start > end) ? 0 : (end-start)+1;
        }
    } else {
        start = 0;
    }

    if (start && newlen) memmove(sh->buf, sh->buf+start, newlen);

    sh->buf[newlen] = 0;

    sh->free = sh->free+(sh->len-newlen);
    sh->len = newlen;
}

//对比两个sds
int sdscmp(const sds s1, const sds s2) {
    size_t l1, l2, minlen;
    int cmp;

    l1 = sdslen(s1);
    l2 = sdslen(s2);
    minlen = (l1 < l2) ? l1 : l2;
    cmp = memcmp(s1, s2, minlen);

    if (cmp == 0) return l1-l2;

    return cmp;
}
