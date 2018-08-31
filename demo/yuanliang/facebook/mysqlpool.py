import pymysql
from DBUtils.PooledDB import PooledDB


class ConPool():
    conn = None
    sql_cursor = None

    def __init__(self, db_config):
        self.creator = pymysql
        self.host = db_config['host']
        self.port = db_config['port']
        self.user = db_config['user']
        self.password = db_config['password']
        self.charset = db_config['charset']
        self.db = db_config['db']
        self.maxsize = db_config['maxsize']
        self.create_pool()

    def create_pool(self):
        try:
            self.pool = PooledDB(
                self.creator,
                self.maxsize,
                host=self.host,
                user=self.user,
                password=self.password,
                db=self.db,
                charset=self.charset)
            print('连接池创建成功')
            return self.pool
        except Exception as e:
            print(e)

    def get_one_conn(self):
        try:
            return self.pool.connection()
        except Exception as e:
            print(e)

    def execute_query(self, sql):
        try:
            conn = self.get_one_conn()
            cur = conn.cursor()
            cur.execute(sql)
        except pymysql.Error as e:
            print(e)
        return cur

    def execute_other(self, sql):
        try:
            conn = self.get_one_conn()
            cur = conn.cursor()
            cur.execute(sql)
        except pymysql.Error as e:
            print(e)
        return conn

    def fetch_one(self, table, column='*', condition=''):
        condition = ' WHERE ' + condition if condition else None
        if condition:
            sql = "SELECT %s FROM %s %s" % (column, table, condition)
        else:
            sql = "SELECT %s FROM %s" % (column, table)
        cur = self.execute_query(sql)
        return cur.fetchone()

    def fetch_all(self, table, column='*', condition=''):
        condition = ' WHERE ' + condition if condition else None
        if condition:
            sql = "SELECT %s FROM %s %s" % (column, table, condition)
        else:
            sql = "SELECT %s FROM %s" % (column, table)
        cur = self.execute_query(sql)
        return cur.fetchall()

    def insert(self, table, tdict, return_last_id=True):
        column = ''
        value = ''
        for key in tdict:
            column += (',' if column else '') + key
            value += ("','" if value else "'") + tdict[key]
        value = value + "'"
        sql = "INSERT INTO %s(%s) VALUES(%s)" % (table, column, value)
        print(sql)
        result = self.execute_other(sql)
        result.commit()
#       return self.sql_cursor.lastrowid if return_last_id else result

    def update(self, table, tdict, condition=''):
        if not condition:
            print('condition is must')
            exit()
        else:
            condition = ' WHERE ' + condition
        value = ''
        for key in tdict:
            value += (',' if value else '') + "%s='%s'" % (key, tdict[key])
        sql = "UPDATE %s SET %s %s" % (table, value, condition)
        result = self.execute_other(sql)
        result.commit()
#        return self.sql_cursor.rowcount

    def delete(self, table, condition=''):
        condition = ' WHERE ' + condition if condition else None
        sql = "DELETE FROM %s %s" % (table, condition)
        print(sql)
        result = self.execute_other(sql)
        result.commit()
#        return self.sql_cursor.rowcount

    def replace(self, table, tdict, return_last_id=True):
        column = ''
        value = ''
        for key in tdict:
            column += (',' if column else '') + key
            value += ("','" if value else "'") + tdict[key]
        value = value + "'"
        sql = "REPLACE INTO %s(%s) VALUES(%s)" % (table, column, value)
        result = self.execute_other(sql)
        result.commit()
#        return self.sql_cursor.lastrowid if return_last_id else result

    def rollback(self):
        self.conn.rollback()

    def __del__(self):
        try:
            self.pool.close()
        except Exception as e:
            print('关闭出错原因{}'.format(e))

    def close(self):
        self.__del__()
