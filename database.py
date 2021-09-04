import sqlite3
import os
import time


class Database:
    '''数据库接口类

    name:   数据库名称, 将访问 data/name.sqlite3 数据库
    '''

    def __init__(self, name):
        if not os.path.exists('data'):
            os.mkdir('data')
        if not os.path.exists('data/video_pic'):
            os.mkdir('data/video_pic')
        if not os.path.exists('data/user_face'):
            os.mkdir('data/user_face')
        self.conn = sqlite3.connect('data/' + name + '.sqlite3')
        self.cursor = self.conn.cursor()
        self.create_table()
        self.count = 0

    def __del__(self):
        '''提交并关闭数据库'''
        self.cursor.close()
        self.conn.commit()
        self.conn.close()

    def update_db(self):
        '''统计更新数据并适时提交记录'''
        self.count += 1
        if (self.count == 100):
            self.conn.commit()
            self.count = 0

    def create_table(self):
        '''在数据库中尝试建表'''
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS VIDEO(
        aid         INTEGER PRIMARY KEY,
        bvid        TEXT,
        cid         INTEGER,
        pic         TEXT,
        title       TEXT,
        desc        TEXT,
        keywords    TEXT,
        copyright   INTEGER,
        duration    INTEGER,
        videos      INTEGER,
        pubdate     INTEGER,
        view        INTEGER,
        danmaku     INTEGER,
        like        INTEGER,
        coin        INTEGER,
        favorite    INTEGER,
        share       INTEGER,
        reply       INTEGER,
        owner       INTEGER,
        localpic    INTEGER,
        spider      INTEGER
        );''')

        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS USER(
        mid         INTEGER PRIMARY KEY,
        name        TEXT,
        sex         TEXT,
        face        TEXT,
        sign        TEXT,
        level       INTEGER,
        attention   INTEGER,
        fans    INTEGER,
        localpic     INTEGER,
        spider      INTEGER
        );''')

        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS COMMENT(
        oid         INTEGER PRIMARY KEY,
        data        TEXT,
        spider      INTEGER
        );''')

        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS TEMP(
        id          INTEGER PRIMARY KEY,
        pn          INTEGER
        );''')

    def insert_video(self, video):
        '''插入视频数据

        video:  视频数据
        '''
        data = (
            video['aid'],
            video['bvid'],
            video['cid'],
            video['pic'],
            video['title'],
            video['desc'],
            video['keywords'],
            video['copyright'],
            video['duration'],
            video['videos'],
            video['pubdate'],
            video['stat']['view'],
            video['stat']['danmaku'],
            video['stat']['like'],
            video['stat']['coin'],
            video['stat']['favorite'],
            video['stat']['share'],
            video['stat']['reply'],
            video['owner']['mid'],
            0,
            int(time.time())
        )
        self.cursor.execute(
            'INSERT OR REPLACE INTO VIDEO VALUES (' + ('?,' * 20) + '?);', data)
        self.update_db()

    def insert_user(self, user):
        '''插入用户数据

        user:   用户数据
        '''
        data = (
            user['mid'],
            user['name'],
            user['sex'],
            user['face'],
            user['sign'],
            user['level_info']['current_level'],
            user['attention'],
            user['fans'],
            0,
            int(time.time())
        )
        self.cursor.execute(
            'INSERT OR REPLACE INTO USER VALUES (' + ('?,' * 9) + '?);', data)
        self.update_db()

    def insert_comment(self, comment):
        '''插入评论数据

        comment: 评论数据
        '''
        data = (
            comment['oid'],
            comment['data'],
            int(time.time())
        )
        self.cursor.execute(
            'INSERT OR REPLACE INTO COMMENT VALUES (?, ?, ?);', data)
        self.update_db()

    def update_temp_pn(self, pn):
        '''更新爬取进度

        pn:     已爬取页
        '''
        self.cursor.execute(
            'INSERT OR REPLACE INTO TEMP VALUES (?, ?);', (1, pn))
        self.update_db()

    def get_temp_pn(self):
        '''获取爬取进度'''
        data = self.cursor.execute('SELECT pn from TEMP').fetchone()
        if data:
            return data[0]
        else:
            return None

    def get_video_pic_list(self):
        '''获取未在本地缓存封面图片的视频列表'''
        return self.cursor.execute('SELECT aid, pic FROM VIDEO WHERE localpic = 0').fetchall()

    def get_user_face_list(self):
        '''获取未在本地缓存头像图片的用户列表'''
        return self.cursor.execute('SELECT mid, face FROM USER WHERE localpic = 0').fetchall()

    def get_comment_list(self):
        '''获取未爬取评论的视频列表'''
        return self.cursor.execute('SELECT aid from VIDEO WHERE aid NOT IN (SELECT oid FROM COMMENT)').fetchall()

    def update_video_pic(self, aid):
        '''更新视频封面爬取标识

        aid:    视频 av 号
        '''
        self.cursor.execute(
            'UPDATE VIDEO SET localpic = 1 WHERE aid = ?', (aid,))
        self.update_db()

    def update_user_pic(self, mid):
        '''更新用户头像爬取标识

        mid:    用户 uid
        '''
        self.cursor.execute(
            'UPDATE USER SET localpic = 1 WHERE mid = ?', (mid,))
        self.update_db()

    def count_videos(self):
        '''获取数据库中视频总数'''
        try:
            return self.cursor.execute('select COUNT() from video').fetchone()[0]
        except:
            return 0
