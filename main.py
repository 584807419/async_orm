import asyncio
import datetime
import aiomysql

from config import DEBUG
from config import DATABASE_HOST
from config import DATABASE_PORT
from config import DATABASE_USER
from config import DATABASE_PWD
from config import DATABASE_DB
from config import DATABASE_CHARSET
from config import DATABASE_AUTOCOMMIT
from config import DATABASE_MAX
from config import DATABASE_MIN

from models import Model
from data_type import StringField, TextField, IntegerField, DateTimeField

if not DEBUG:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())  # 使用 uvloop 来替换 asyncio 内部的事件循环。

class AsyncMysql:
    def __init__(self, *args, **kwargs):
        super(AsyncMysql, self).__init__(*args, **kwargs)
        print('database connection pool init...')

    async def create_async_orm_db_conn_pool(self):
        print(f'create 11 database connection pool {DATABASE_HOST} ...')
        return await aiomysql.create_pool(
            host=DATABASE_HOST,
            port=DATABASE_PORT,
            user=DATABASE_USER,
            password=DATABASE_PWD,
            db=DATABASE_DB,
            charset=DATABASE_CHARSET,
            autocommit=DATABASE_AUTOCOMMIT,
            maxsize=DATABASE_MAX,
            minsize=DATABASE_MIN,
            # pool_recycle=3600,
            loop=self.loop
        )
class Demo(AsyncMysql):
    def __init__(self, *args, **kwargs):
        super(Demo, self).__init__(*args, **kwargs)
        # 事件循环
        task_number = kwargs.get('TASK_NUMBER')
        loop = asyncio.get_event_loop()
        tasks = [asyncio.ensure_future(self.ready(**kwargs)) for _ in range(task_number)]
        if DEBUG:
            loop.set_debug(True)
        loop.run_until_complete(asyncio.wait(tasks))

    async def ready(self, **kwargs):
        # 1. mysql orm 示例
        # 创建连接池
        db_conn_pool = await self.create_async_orm_db_conn_pool()

        # 创建模型
        class BondPrices(Model):
            __db_conn_pool__ = db_conn_pool
            __table__ = "bond_prices"
            id = IntegerField(primary_key=True)
            trade_time = DateTimeField()
            trade_data = TextField()
            page_name = StringField(ddl='varchar(45)')
            is_valid = IntegerField()
            data_source = StringField(ddl='varchar(100)')
            verify_md5 = StringField(ddl='varchar(40)')
            clean_status = IntegerField()
            create_at = DateTimeField()
            update_at = DateTimeField()

        print('检查表是否存在')
        data_array = await BondPrices.table_exists('erqqwrwfdsa')
        if data_array == 1:
            print('表存在')
        else:
            print('表不存在')

        print('直接执行sql语句')
        data_array = await BondPrices.execute_sql('select * from bond_prices')
        if data_array:
            for data_hashmap in data_array:
                print(data_hashmap)
        print('增加')
        bond_price_data = BondPrices(trade_time=datetime.datetime.now(),
                                     trade_data='dadad',
                                     page_name='保存pagename',
                                     data_source='单顺荣',
                                     is_valid=1,
                                     verify_md5='123456789',
                                     clean_status=2,
                                     create_at=datetime.datetime.now(),
                                     update_at=datetime.datetime.now(),
                                     )
        insert_res = await bond_price_data.save_db_date()
        if insert_res:
            print(f'插入数据成功，主键是{insert_res}')

        print('修改')
        bond_price_data = BondPrices(id=4,
                                     trade_time=datetime.datetime.now(),
                                     trade_data='dadad',
                                     page_name='愤忿忿忿忿忿忿忿忿忿忿',
                                     data_source='单顺荣',
                                     is_valid=1,
                                     verify_md5='123456789',
                                     clean_status=2,
                                     create_at=datetime.datetime.now(),
                                     update_at=datetime.datetime.now(),
                                     )
        insert_res = await bond_price_data.update_db_date()
        if insert_res:
            print(f'修改数据成功，影响行数是{insert_res}')

        print('删除')
        data_array = await BondPrices.select_by_where(id=3)
        if data_array:
            remove_res = await data_array[0].remove_db_date()
            print(f'删除数据成功，影响行数是{remove_res}')

        print('查询')
        data_array = await BondPrices.select_by_where(id=3, page_name='报价行情', limit=1)
        if data_array:
            data_hashmap = dict(data_array[0])
            print(data_hashmap.get('verify_md5'))


if __name__ == '__main__':
    # 异步工具使用方法示例
    demo_ins = Demo(TASK_NUMBER=1)
    print('end')
