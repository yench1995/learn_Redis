#!/usr/bin/env python
# encoding: utf-8

import redis

def check_token(conn, token):
    return conn.hget('login:', token)

def update_token(conn, token, user, item=None):
    timestamp = time.time()
    conn.hset('login:', token, user)
    conn.zadd('recent:', token, timestamp)
    if item:
        conn.zadd('viewed:' + token, item, timestamp)
        #移除旧的记录，只保留用户最近浏览过的25个商品
        conn.zremrangebyrank('viewed:' + token, 0, -26)
        #对浏览商品排序
        conn.zincrby('viewed:', item, -1)

QUIT = False
LIMIT = 1000000

def clean_session(conn):
    while not QUIT:
        sie = conn.zcard('recent:')
        if size <= LIMIT:
            time.sleep(1)
            continue
        #获取需要移除的令牌数量
        end_index = min(size - LIMIT, 100)
        #获取要移除的令牌ID
        tokens = conn.zrange('recent:', 0, end_index-1)

        session_keys = []
        for token in tokens:
            session_keys.append('viewed:' + token)
        conn.delete(*session_keys)
        conn.hdel('login:', *tokens)
        conn.zrem('recent:', *tokens)

def add_to_cart(conn, session, item, count):
    if count <= 0:
        #移除指定的商品
        conn.hrem('cart:' + session, item)
    else:
        #添加信息
        conn.hset('cart:' + session, item, count)

def clean_full_session(conn):
    while not QUIT:
        size = conn.zcart('recent:')
        if size <= LIMIT:
            time.sleep(1)
            continue
        end_index = min(size - LIMIT, 100)
        sessions = conn.zrange('recent:', 0, end_index-1)

        session_keys = []
        for sess in sessions:
            session_keys.append('viewed:' + sess)
            session_keys.append('cart:' + sess)
        conn.delete(*session_keys)
        conn.hdel('login:', *sessions)
        conn.zrem('recent:', *sessions)


#web页面缓存
def cache_request(conn, request, callback):
    if not can_cache(conn, request):
        return callback(request)

    page_key = 'cache:' + hash_request(request)
    content = conn.get(page_key)

    if not content:
        #如果页面没有被缓存，那么生成页面
        content = callback(request)
        onn.setex(page_key, content, 300)

    return content

#web数据行缓存
def schedule_row_cache(conn, row_id, delay):
    #先设置数据行的延迟值
    conn.zadd('delay:', row_id, delay)
    #对需要缓存的数据行进行调度
    conn.zadd('schedule:', row_id, time.time())

def cache_rows(conn):
    while not QUIT:
        next = conn.zrange('schedule:', 0, 0, withscores=True)
        now = time.time()
        #返回一个元组
        if not next or next[0][1] > now:
            time.sleep(.05)
            continue

        #元组的第一项的第一项为id
        row_id = next[0][0]
        delay = conn.zscore('delay:', row_id)
        if delay <= 0:
            conn.zrem('delay:', row_id)
            conn.zrem('schedule:', row_id)
            conn.delete('inv:' + row_id)
            continue
        row = Inventory.get(row_id)
        conn.zadd('schedule:', row_id, now+delay)
        #编码为json字典并存储
        conn.set('inv:' + row_id, json.dumps(row.to_dict()))

def rescale_viewed(conn):
    while not QUIT:
        #删除排在20000名后的商品
        conn.zremrangebyrank('viewed:', 0, -20001)
        #将浏览次数降低为原来一半
        conn.zinterstore('viewed:', {'viewed:': .5})
        time.sleep(300)

def can_cache(conn, request):
    item_id - extract_item_id(request)
    if not item_id or is_dynamic(request):
        return False
    rank = conn.zrank('viewed:', item_id)
    return rank is not None and rank < 10000
