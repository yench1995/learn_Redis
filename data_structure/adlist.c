#include <stdlib.h>
#include "adlist.h"
#include "zmalloc.h"

//创建一个新的链表
list *listCreate(void)
{
    struct list *list;

    if ((list = zmalloc(sizeof(*list))) == NULL)
        return NULL;

    list->head = list->tail = NULL;
    list->len = 0;
    list->dup = NULL;
    list->free = NULL;
    list->match = NULL;

    return list;
}

//释放整个链表，以及链表中所有节点
void listRelease(list *list)
{
    unsigned long len;
    listNode *current, *next;

    current = list->head;
    len = list->len;

    while (len--) {
        next = current->next;
        if (list->free)
            list->free(current->value);
        zfree(current);
        current = next;
    }

    zfree(list);
}

//将一个包含有给定值value的新节点添加到链表的表头
list *listAddNodeHead(list *list, void *value)
{
    listNode *node;

    if ((node = zmalloc(sizeof(*node))) == NULL)
        return NULL;

    node->value = value;

    if (list->len == 0) {
        list->head = list->tail = node;
        node->prev = node->next = NULL;
    } else {
        node->prev = NULL;
        node->next = list->head;
        list->head->prev = node;
        list->head = node;
    }

    list->len++;
    return list;
}

//将一个包含有给定值value的新节点添加到链表的表尾
list *listAddNodeTail(list *list, void *value)
{
    listNode *node;

    if ((node = zmalloc(sizeof(*node))) == NULL)
        return NULL;

    node->value = value;
    if (list->len == 0) {
        list->head = list->tail = node;
        node->prev = node->next = NULL;
    } else {
        node->prev = list->tail;
        node->next = NULL;
        node->prev->next = node;
        list->tail = node;
    }

    list->len++;
    return list;
}

/*
 * 创建一个包含值value的新节点，并将它插入到old_node的之前或之后
 * 如果after为0, 将新节点插入到old_node之前
 * 如果after为1, 将新节点插入到old_node之后
 */
list *listInsertNode(list *list, listNode *old_node, void *value, int after)
{
    listNode *node;
    if ((node = zmalloc(sizeof(*node))) == NULL)
        return NULL;

    node->value = value;

    if (after) {
        node->prev = old_node;
        node->next = old_node->next;
        if (list->tail == old_node)
            list->tail = node;
    } else {
        node->next = old_node;
        node->prev = old_node->prev;
        if (list->head == old_node)
            list->head = node;
    }

    if (node->prev != NULL)
        node->prev->next = node;
    if (node->next != NULL)
        node->next->prev = node;

    list->len++;
    return list;
}

//从链表list中删除给定节点node
void listDelNode(list *list, listNode *node)
{
    if (node->prev)
        node->prev->next = node->next;
    else
        list->head = node->next;

    if (node->next)
        node->next->prev = node->prev;
    else
        list->tail = node->prev;

    if (list->free)
        list->free(node->value);
    zfree(node);
    list->len--;
}

/*
 * 为给定链表创建一个迭代器
 * 之后每次对这个迭代器调用listNext都返回被迭代到的链表节点
 */
listIter *listGetIterator(list *list, int direction)
{
    listIter *iter;
    if ((iter = zmalloc(sizeof(*iter))) == NULL)
        return NULL;

    if (direction == AL_START_HEAD)
        iter->next = list->head;
    else
        iter->next = list->tail;

    iter->direction = direction;
    return iter;
}

//释放迭代器
void listReleaseIterator(listIter *iter) {
    zfree(iter);
}

//将迭代器的方向设置为AL_START_HEAD, 并将迭代指针重新指向表头节点
void listRewind(list *list, listIter *li)
{
    li->next = list->head;
    li->direction = AL_START_HEAD;
}

//将迭代器的方向设置为AL_START_TAIL, 并将迭代器指针重新指向表尾节点
void listRewindTail(list *list, listIter *li)
{
    li->next = list->tail;
    li->direction = AL_START_TAIL;
}


/*
 * 返回迭代器当前所指向的节点
 * 删除当前节点是允许的，但不能修改链表里的其他节点
 *
 * 常见的用法是
 * iter = listGetIterator(list, <direction>);
 * while ((node = listNext(iter)) != NULL)
 *      doSomethingWith(listNodeValue(node));
 */
listNode *listNext(listIter *iter)
{
    listNode *current = iter->next;

    if (current != NULL) {
        if (iter->direction == AL_START_HEAD)
            iter->next = current->next;
        else
            iter->next = current->prev;
    }

    return current;
}

/*
 * 复制整个链表
 *
 * 如果链表有设置值复制函数dup，那么对值的复制将使用复制函数进行
 * 否则，新节点将和旧节点共享同一个指针
 */
list *listDup(list *orig)
{
    list *copy;
    listIter *iter;
    listNode *node;

    if ((copy = listCreate()) == NULL)
        return NULL;

    copy->dup = orig->dup;
    copy->free = orig->free;
    copy->match = orig->match;

    iter = listGetIterator(orig, AL_START_HEAD);
    while((node = listNext(iter)) != NULL) {
        void *value;

        if (copy->dup) {
            value = copy->dup(node->value);
            if (value == NULL) {
                listRelease(copy);
                listReleaseIterator(iter);
                return NULL;
            }
        } else
            value = node->value;

        if (listAddNodeTail(copy, value) == NULL) {
            listRelease(copy);
            listReleaseIterator(iter);
            return NULL;
        }
    }

    listReleaseIterator(iter);
    return copy;
}

/*
 * 查找链表list中值和key匹配的节点
 *
 * 对比操作由链表的match函数负责进行
 * 如果没有match函数，则直接对比值的指针来决定是否匹配
 */
listNode *listSearchKey(list *list, void *key)
{
    listIter *iter;
    listNode *node;

    iter = listGetIterator(list, AL_START_HEAD);
    while ((node = listNext(iter)) != NULL) {
        if (list->match) {
            if (list->match(node->value, key)) {
                listReleaseIterator(iter);
                return node;
            }
        } else {
            if (key == node->value) {
                listReleaseIterator(iter);
                return node;
            }
        }
    }

    listReleaseIterator(iter);
    return NULL;
}

//返回链表在给定索引上的值
listNode *listIndex(list *list, long index) {
    listNode *n;

    if (index < 0) {
        index = (-index)-1;
        n = list->tail;
        while (index-- && n) n = n->prev;
    } else {
        n = list->tail;
        while (index-- && n) n = n->next;
    }

    return n;
}

//取出链表的表尾节点，并将它移动到表头，成为新的表头节点
void listRotate(list *list)
{
    listNode *tail = list->tail;

    if (listLength(list) <= 1) return;

    list->tail = tail->prev;
    list->tail->next = NULL;

    list->head->prev = tail;
    tail->prev = NULL;
    tail->next = list->head;
    list->head = tail;
}
