#ifndef __INESET_H
#define __INTSET_H
#include <stdint.h>

//数据结构的优势
//提升灵活性，可以有int16_t, int32_t, int64_t三种不同的编码方式添加进数组中
//节约内存，确保升级操作只会在需要的时候进行

typedef struct intset {

    //编码方式
    uint32_t encoding;
    //集合包含的元素数量
    uint32_t length;
    //保存元素的数组
    int8_t contents[];
} intset;

intset *intsetNew(void);
intset *intsetAdd(intset *is, int64_t value, uint8_t *success);
intset *intsetRemove(intset *is, int64_t value, int *success);
uint8_t intsetFind(intset *is, int64_t value);
int64_t intsetRandom(intset* is);
uint8_t intsetGet(intset *is, uint32_t pos, int64_t *value);
uint32_t intsetLen(intset *is);
size_t intsetBloblen(intset *is);

#endif
