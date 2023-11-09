import json
import pymysql.cursors


class DatabaseManager:
    def __init__(self, host, user, passwd, db):
        self.conn = pymysql.connect(
            host=host,
            user=user,
            password=passwd,
            database=db,
            cursorclass=pymysql.cursors.DictCursor,
        )
        self.cur = self.conn.cursor()

    def insert_outbox_event(self, topic, payload):
        sql = "INSERT INTO outbox_event (event_type, payload, topic, processed) VALUES (%s, %s, %s, %s)"
        self.cur.execute(
            sql, ("calculator", json.dumps(payload), topic, 0)
        )  # 0 means not processed
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()
