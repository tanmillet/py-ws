import pymysql


class Db:
    conn = None
    sql_cursor = None

    def __init__(self, db_config):
        self.host = db_config['host']
        self.port = db_config['port']
        self.user = db_config['user']
        self.password = db_config['password']
        self.charset = db_config['charset']
        self.db = db_config['db']
        self.connect()
        self.sql_cursor = self.conn.cursor()

    def connect(self):
        try:
            self.conn = pymysql.connect(
                host=self.host, port=self.port, user=self.user,
                password=self.password, db=self.db, charset=self.charset
            )
        except pymysql.Error as e:
            print(e)

    def query(self, sql):
        try:
            result = self.sql_cursor.execute(sql)
        except pymysql.Error as e:
            print(e)
            result = False

        return result

    def fetchOne(self, table, column='*', condition=''):
        condition = ' WHERE ' + condition if condition else None
        if condition:
            sql = "SELECT %s FROM %s %s" % (column, table, condition)
        else:
            sql = "SELECT %s FROM %s" % (column, table)
        self.query(sql)
        return self.sql_cursor.fetchone()

    def fetchAll(self, table, column='*', condition=''):
        condition = ' WHERE ' + condition if condition else None
        if condition:
            sql = "SELECT %s FROM %s %s" % (column, table, condition)
        else:
            sql = "SELECT %s FROM %s" % (column, table)
        self.query(sql)
        return self.sql_cursor.fetchall()

    def insert(self, table, tdict, return_last_id=True):
        column = ''
        value = ''
        for key in tdict:
            column += (',' if column else '') + key
            value += ("','" if value else "'") + tdict[key]
        value = value + "'"
        sql = "INSERT INTO %s(%s) VALUES(%s)" % (table, column, value)
        # print(sql)
        result = self.query(sql)
        self.conn.commit()
        return self.sql_cursor.lastrowid if return_last_id == True else result

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
        print(sql)
        result = self.query(sql)
        self.conn.commit()
        return self.sql_cursor.rowcount

    def delete(self, table, condition=''):
        condition = ' WHERE ' + condition if condition else None
        sql = "DELETE FROM %s %s" % (table, condition)
        self.query(sql)
        self.conn.commit()
        return self.sql_cursor.rowcount

    def rollback(self):
        self.conn.rollback()

    def __del__(self):
        try:
            self.sql_cursor.close()
            self.conn.close()
        except:
            pass

    def close(self):
        self.__del__()
