#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "intset.h"
#include "zmalloc.h"
#include "endianconv.h"

//intset的编码方式
#define INTSET_ENC_INT16 (sizeof(int16_t))
#define INTSET_ENC_INT32 (sizeof(int32_t))
#define INTSET_ENC_INT64 (sizeof(int64_t))

//返回适用于传入值V的编码方式
static uint8_t _insetValueEncoding(int64_t v) {
    if (v < INT32_MIN || v > INT32_MAX)
        return INTSET_ENC_INT64;
    else if (v < INT16_MIN || v > INT16_MAX)
        return INTSET_ENC_INT32;
    else
        return INTSET_ENC_INT16;
}

//根据给定的编码方式，返回集合的底层数组在pos索引上的元素
static int64_t _intsetGetEncoded(intset *is, int pos, uint8_t enc) {
    int64_t v64;
    int32_t v32;
    int16_t v16;

    //首先将数组转换回被编码的类型
    //计算出元素在数组中的正确位置
    //再从数组中拷贝处正确数量的字节
    //会对拷贝出的字节进行大小端转换
    //最后将值返回
    if (enc == INTSET_ENC_INT64) {
        memcpy(&v64, ((int64_t*)is->contents)+pos, sizeof(v64));
    }

}
