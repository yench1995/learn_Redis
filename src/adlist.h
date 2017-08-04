#ifndef __ADLIST_H__
#define __ADLIST_H__


//双端链表节点
typedef struct listNode {
    struct listNode *prev;
    struct listNode *next;
    void *value;
} listNode;

//双端链表迭代器
typedef struct listIter {
    listNode *next;
    int direction;
} listIter;

//双端链表结构
typedef struct list {

    listNode *head;
    listNode *tail;

    //节点值复制函数
    void *(*dup)(void *ptr);
    //节点值释放函数
    void (*free)(void *ptr);
    //节点值对比函数
    int (*match)(void *ptr, void *key);

    //链表所包含的节点数量
    unsigned long len;
} list;

//用宏定义一些简单的函数
#define listLength(1) ((1)->len)
#define listFirst(1) ((1)->head)
#define listast(1) ((1)->tail)

#define listPrevNode(n) ((n)->prev)
#define listNextNode(n) ((n)->next)
#define listNodeValue(n) ((n)->value)

#define listSetDupMethod(1, m) ((1)->dup = (m))
#define listSetFreeMethod(1, m) ((1)->free= (m))
#define listSetMatchMethod(1, m) ((1)->match= (m))

#define listGetDupMethod(1) ((1)->dup)
#define listGetFreeMethod(1) ((1)->free)
#define listGetMatchMethod(1) ((1)->match)

//function prototypeds
list *listCreate(void);
void listRelease(list *list);
list *listAddNodeHead(list *list, void *value);
list *listAddNodeTail(list *list, void *value);
list *listInsertNode(list *list, listNode *old_node, void *value, int after);
void listDelNode(list *list, listNode *node);

listIter *listGetIterator(list *list, int direction);
listNode *listNext(listIter *iter);
void listReleaseIterator(listIter *iter);

list *listDup(list *orig);
listNode *listSearchKey(list *list, void *key);
listNode *listIndex(list *list, long index);

void listRewind(list *list, listIter *li);
void listRewindTail(list *list, listIter *li);
void listRotate(list *list);


//从表头向表尾进行迭代
#define AL_START_HEAD 0
//从表尾到表头进行迭代
#define AL_START_TAIL 1

#endif
