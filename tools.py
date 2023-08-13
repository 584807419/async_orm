# coding=utf-8

import aiomysql
from pymysql.err import IntegrityError


def create_args_string(num):
    l_list = []
    for n in range(num):
        l_list.append('?')
    return ', '.join(l_list)


async def select(db_pool, sql, args, size=None):
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            # try:
            await cur.execute(sql.replace('?', '%s'), args or ())
            # print(cur.description)
            if size:
                rs = await cur.fetchmany(size)
            else:
                rs = await cur.fetchall()
                # (r,) = await cur.fetchone()
            await cur.close()
            # print('rows returned: %s' % len(rs))
            return rs
            # finally:
            #     if cur:
            #         await cur.close()
            #     # 释放掉conn,将连接放回到连接池中
            #     await db_pool.release(conn)
            #


async def execute(db_pool, sql, args, autocommit=True):
    async with db_pool.acquire() as conn:
        if not autocommit:
            await conn.begin()
        async with conn.cursor(aiomysql.DictCursor) as cur:
            try:
                await cur.execute(sql.replace('?', '%s'), args)
                last_id = cur.lastrowid
                affected = cur.rowcount
                await cur.close()
                if not autocommit:
                    await conn.commit()
            except IntegrityError:
                return f'IntegrityError', 0
            except BaseException as e:
                if not autocommit:
                    await conn.rollback()
                raise Exception(f"err_type:\n{e.__class__.__name__}\nlast_executed_sql:\n{cur._last_executed}\n error:\n{e}")
            return last_id, affected


async def _exec_select_sql(db_pool, sql, args, size=None):
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            # try:
            await cur.execute(sql.replace('?', '%s'), args or ())
            # print(cur.description)
            if size:
                rs = await cur.fetchmany(size)
            else:
                rs = await cur.fetchall()
                # (r,) = await cur.fetchone()
            await cur.close()
            # print('rows returned: %s' % len(rs))
            return rs
            # finally:
            #     if cur:
            #         await cur.close()
            #     # 释放掉conn,将连接放回到连接池中
            #     await db_pool.release(conn)
            #


async def _execute(db_pool, sql, args, autocommit=True):
    async with db_pool.acquire() as conn:
        if not autocommit:
            await conn.begin()
        async with conn.cursor(aiomysql.DictCursor) as cur:
            try:
                await cur.execute(sql.replace('?', '%s'), args)
                last_id = cur.lastrowid
                affected = cur.rowcount
                await cur.close()
                if not autocommit:
                    await conn.commit()
            except BaseException as e:
                _last_executed = cur._last_executed
                if not autocommit:
                    await conn.rollback()
                raise Exception(f"{e.__class__.__name__}\nlast_executed_sql:\n{_last_executed}\n error_content:\n{e}")
            return last_id, affected


def _create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)
