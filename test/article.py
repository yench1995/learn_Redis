#!/usr/bin/env python
# encoding: utf-8

import redis

ONE_WEEK_IN_SECONDS = 7*86400
VOTE_SCORE = 432

def article_vote(conn, user, article):
    cutoff = time.time() - ONE_WEEK_IN_SECONDS
    if conn.zscore('time:', article) < cutoff:
        return

    article_id = article.partition(':')[-1]
    if conn.sadd('voted:' + article_id, user): #若已经包含该用户，则函数返回0
        conn.zincrby('score:', article, VOTE_SCORE)
        conn.hincrby(article, 'votes', 1) #正确实现两者应该使用事务


def post_article(conn, user, title, link):
    article_id = str(conn.incr('article:')) #对计数器执行INCR操作

    voted = 'voted:' + article_id
    conn.sadd(voted, user)
    conn.expire(voted, ONE_WEEK_IN_SECONDS) #设置过期时间

    now = time.time()
    article = 'article:' + article_id
    conn.hmset(article, {
        'title':title,
        'link':link,
        'poster':user,
        'time':now,
        'votes':1,
    })

    conn.zadd('score:', article, now+VOTE_SCORE)
    conn.zadd('time:', article, now)
    return article_id

ARTICLES_PER_PAGE = 25
def get_articles(conn, page, order='score'):
    start = (page-1) * ARTICLES_PER_PAGE
    end = start + ARTICLES_PER_PAGE - 1

    ids = conn.zrevrange(order, start, end)
    articles = []
    for id in ids:
        article_data = conn.hgetall(id)
        article['id'] = id
        articles.append(article_data)

    return articles

def add_remove_groups(conn, article_id, to_add=[], to_remove=[]):
    article = 'article:' + article_id
    for group in to_add:
        conn.sadd('group:' + group, article)
    for group in to_remove:
        conn.srem('group:' + group, article)

def get_group_articles(conn, group, page, order='score:'):
    key = order + group
    if not conn.exists(key):
        #zinterstore接收多个集合和多个有效集合作为输入，找出并集，合并分值
        conn.zinterstore(key, ['group:' + group, order], aggregate='max',)
        #60秒后自动删除key
        conn.expire(key, 60)

    return get_articles(conn, page, key)

