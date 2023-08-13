# coding=utf-8

import aiomysql
from datetime import datetime
from tools import create_args_string, _execute, _exec_select_sql
from data_type import Field, StringField, TextField, IntegerField, DateTimeField, BooleanField, FloatField

from config import DEBUG


class ModelMetaclass(type):

    def __new__(cls, name, bases, attrs):
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        table_name = attrs.get('__table__', None) or name
        cls.__table__ = table_name
        # print('found model: %s (table: %s)' % (name, table_name))
        mappings = dict()
        fields = []
        primary_key = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                # print('  found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    # 找到主键:
                    if primary_key:
                        raise Exception('Duplicate primary key for field: %s' % k)
                    primary_key = k
                else:
                    fields.append(k)
        if not primary_key:
            raise Exception('Primary key not found.')
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings  # 保存属性和列的映射关系
        attrs['__table__'] = table_name
        attrs['__primary_key__'] = primary_key  # 主键属性名
        attrs['__fields__'] = fields  # 除主键外的属性名
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (
            primary_key, ', '.join(escaped_fields), "__Placeholder_identifier__")
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (
            "__Placeholder_identifier__", ', '.join(escaped_fields), primary_key,
            create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (
            "__Placeholder_identifier__", ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)),
            primary_key)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % ("__Placeholder_identifier__", primary_key)
        return type.__new__(cls, name, bases, attrs)


class Model(dict, metaclass=ModelMetaclass):

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        field = self.__mappings__[key]
        if value is None:
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                setattr(self, key, value)
        else:
            if DEBUG:
                if isinstance(field, StringField):
                    if not (isinstance(value, str) or isinstance(value, bytes)):
                        raise TypeError(
                            f'表:{self.__table__}--字段:{key}--值:{value}==现类型:{type(value).__name__}与定义类型str不符')
                if isinstance(field, BooleanField):
                    if not (isinstance(value, bool) or isinstance(value, int)):
                        raise TypeError(
                            f'表:{self.__table__}--字段:{key}--值:{value}==现类型:{type(value).__name__}与定义类型bool、int不符')
                if isinstance(field, IntegerField):
                    if not isinstance(value, int):
                        raise TypeError(
                            f'表:{self.__table__}--字段:{key}--值:{value}==现类型:{type(value).__name__}与定义类型int不符')
                if isinstance(field, FloatField):
                    if not isinstance(value, float):
                        raise TypeError(
                            f'表:{self.__table__}--字段:{key}--值:{value}==现类型:{type(value).__name__}与定义类型float不符')
                if isinstance(field, TextField):
                    if not (isinstance(value, str) or isinstance(value, bytes)):
                        raise TypeError(
                            f'表:{self.__table__}--字段:{key}--值:{value}==现类型:{type(value).__name__}与定义类型str不符')
                if isinstance(field, DateTimeField):
                    if not (isinstance(value, datetime) or isinstance(value, str)):
                        raise TypeError(
                            f'表:{self.__table__}--字段:{key}--值:{value}==现类型:{type(value).__name__}与定义类型datetime不符')
        return value

    @classmethod
    async def execute_sql(cls, sql_statement=None):
        async with cls.__db_conn_pool__.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql_statement)
                rs = await cur.fetchall()
                await cur.close()
                return rs

    @classmethod
    async def select_by_pk(cls, db_pool, table_name, pk: int):
        """
        根据主键查找数据
        :param db_pool:
        :param pk: int
        :return: cls
        """
        ' find object by primary key. '
        rs = await _exec_select_sql(db_pool, '%s where `%s`=?' % (cls.__select__.replace("__Placeholder_identifier__", table_name), cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    @classmethod
    async def select_by_where(cls, db_pool=None, table_name=None, **kwargs) -> list:
        """
        where条件查询,适用于固定相等的筛选条件
        """
        if not table_name:
            table_name = cls.__table__
        if not db_pool:
            db_pool = cls.__db_conn_pool__
        sql = [cls.__select__.replace("__Placeholder_identifier__", table_name)]
        _temp_list = []
        index = 0
        for key, value in kwargs.items():
            index += 1
            if key not in ['order_by', 'limit']:
                if index == 1:
                    sql.append('where')
                if index > 1:
                    sql.append('and')
                sql.append(f'{key}=?')
                _temp_list.append(value)
            else:
                if key == 'order_by':
                    sql.append(f'order by {value}')
                    # _temp_list.remove(value)
                if key == 'limit':
                    sql.append('limit')
                    if isinstance(value, int):
                        sql.append('?')
                        _temp_list.append(value)
                    elif isinstance(value, tuple) and len(value) == 2:
                        sql.append('?, ?')
                        _temp_list.extend(value)
                    else:
                        raise ValueError('Invalid limit value: %s' % str(value))
        rs = await _exec_select_sql(db_pool, ' '.join(sql), _temp_list)
        return [cls(**r) for r in rs]

    async def save_db_date(self, table_name=None) -> int:
        if not table_name:
            table_name = self.__table__
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        temp_list = []
        for i in args:
            if i == []:
                temp_list.append(None)
            else:
                temp_list.append(i)
        data_pk, rows = await _execute(self.__db_conn_pool__,
                                       self.__insert__.replace("__Placeholder_identifier__", table_name),
                                       temp_list)
        if rows != 1:
            raise Exception('failed to insert record: affected rows: %s' % rows)
        return data_pk

    async def update_db_date(self, table_name=None):
        if not table_name:
            table_name = self.__table__
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        data_pk, rows = await _execute(self.__db_conn_pool__,
                                       self.__update__.replace("__Placeholder_identifier__", table_name),
                                       args)
        if rows != 1:
            print('failed to update by primary key: affected rows: %s' % rows)
        return rows

    async def remove_db_date(self, table_name=None):
        if not table_name:
            table_name = self.__table__
        args = [self.getValue(self.__primary_key__)]
        data_pk, rows = await _execute(self.__db_conn_pool__, self.__delete__.replace("__Placeholder_identifier__", table_name), args)
        if rows != 1:
            raise Exception('failed to remove by primary key: affected rows: %s' % rows)
        return rows

    @classmethod
    async def create_db_table(cls, db_pool, sql, autocommit=True):
        async with cls.__db_conn_pool__.acquire() as conn:
            if not autocommit:
                await conn.begin()
            async with conn.cursor(aiomysql.DictCursor) as cur:
                try:
                    await cur.execute(sql)
                    affected = cur.rowcount
                    await cur.close()
                    if not autocommit:
                        await conn.commit()
                except BaseException as e:
                    if not autocommit:
                        await conn.rollback()
                    raise e
                return affected

    @classmethod
    async def table_exists(cls, table_name=None, autocommit=True):
        if not table_name:
            table_name = cls.__table__
        sql = f"SHOW TABLES like '{table_name}';"
        async with cls.__db_conn_pool__.acquire() as conn:
            if not autocommit:
                await conn.begin()
            async with conn.cursor(aiomysql.DictCursor) as cur:
                try:
                    await cur.execute(sql)
                    affected = cur.rowcount
                    await cur.close()
                    if not autocommit:
                        await conn.commit()
                except BaseException as e:
                    if not autocommit:
                        await conn.rollback()
                    raise e
                return affected