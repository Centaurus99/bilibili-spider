import sqlite3
import configparser
from tqdm import tqdm
from PIL import Image
Image.MAX_IMAGE_PIXELS = 200000000

CONFIG = configparser.ConfigParser()
CONFIG.read('config.ini', encoding='utf-8')
DB_NAME = CONFIG['common'].get('database_name', fallback='data')

conn = sqlite3.connect('data/' + DB_NAME + '.sqlite3')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

assert(cursor.execute(
    'SELECT COUNT() FROM VIDEO WHERE bvid IS NULL;').fetchone()[0] == 0)
assert(cursor.execute(
    'SELECT COUNT() FROM VIDEO WHERE cid IS NULL;').fetchone()[0] == 0)
assert(cursor.execute(
    'SELECT COUNT() FROM VIDEO WHERE pic IS NULL;').fetchone()[0] == 0)
assert(cursor.execute(
    'SELECT COUNT() FROM VIDEO WHERE title IS NULL;').fetchone()[0] == 0)
assert(cursor.execute(
    'SELECT COUNT() FROM VIDEO WHERE desc IS NULL;').fetchone()[0] == 0)
assert(cursor.execute(
    'SELECT COUNT() FROM VIDEO WHERE keywords IS NULL;').fetchone()[0] == 0)
assert(cursor.execute(
    'SELECT COUNT() FROM VIDEO WHERE copyright IS NULL;').fetchone()[0] == 0)
assert(cursor.execute(
    'SELECT COUNT() FROM VIDEO WHERE duration IS NULL;').fetchone()[0] == 0)
assert(cursor.execute(
    'SELECT COUNT() FROM VIDEO WHERE videos IS NULL;').fetchone()[0] == 0)
assert(cursor.execute(
    'SELECT COUNT() FROM VIDEO WHERE pubdate IS NULL;').fetchone()[0] == 0)
assert(cursor.execute(
    'SELECT COUNT() FROM VIDEO WHERE view IS NULL;').fetchone()[0] == 0)
assert(cursor.execute(
    'SELECT COUNT() FROM VIDEO WHERE danmaku IS NULL;').fetchone()[0] == 0)
assert(cursor.execute(
    'SELECT COUNT() FROM VIDEO WHERE like IS NULL;').fetchone()[0] == 0)
assert(cursor.execute(
    'SELECT COUNT() FROM VIDEO WHERE coin IS NULL;').fetchone()[0] == 0)
assert(cursor.execute(
    'SELECT COUNT() FROM VIDEO WHERE favorite IS NULL;').fetchone()[0] == 0)
assert(cursor.execute(
    'SELECT COUNT() FROM VIDEO WHERE share IS NULL;').fetchone()[0] == 0)
assert(cursor.execute(
    'SELECT COUNT() FROM VIDEO WHERE reply IS NULL;').fetchone()[0] == 0)
assert(cursor.execute(
    'SELECT COUNT() FROM VIDEO WHERE owner IS NULL;').fetchone()[0] == 0)
assert(cursor.execute(
    'SELECT COUNT() FROM VIDEO WHERE localpic IS NULL;').fetchone()[0] == 0)
assert(cursor.execute(
    'SELECT COUNT() FROM VIDEO WHERE spider IS NULL;').fetchone()[0] == 0)

assert(cursor.execute(
    'SELECT COUNT() FROM USER WHERE name IS NULL;').fetchone()[0] == 0)
assert(cursor.execute(
    'SELECT COUNT() FROM USER WHERE sex IS NULL;').fetchone()[0] == 0)
assert(cursor.execute(
    'SELECT COUNT() FROM USER WHERE face IS NULL;').fetchone()[0] == 0)
assert(cursor.execute(
    'SELECT COUNT() FROM USER WHERE sign IS NULL;').fetchone()[0] == 0)
assert(cursor.execute(
    'SELECT COUNT() FROM USER WHERE level IS NULL;').fetchone()[0] == 0)
assert(cursor.execute(
    'SELECT COUNT() FROM USER WHERE attention IS NULL;').fetchone()[0] == 0)
assert(cursor.execute(
    'SELECT COUNT() FROM USER WHERE fans IS NULL;').fetchone()[0] == 0)
assert(cursor.execute(
    'SELECT COUNT() FROM USER WHERE localpic IS NULL;').fetchone()[0] == 0)
assert(cursor.execute(
    'SELECT COUNT() FROM USER WHERE spider IS NULL;').fetchone()[0] == 0)

assert(cursor.execute(
    'SELECT COUNT() FROM COMMENT WHERE data IS NULL;').fetchone()[0] == 0)
assert(cursor.execute(
    'SELECT COUNT() FROM COMMENT WHERE spider IS NULL;').fetchone()[0] == 0)

print('NULL check pass.')


assert(cursor.execute(
    'SELECT COUNT() from VIDEO WHERE VIDEO.owner NOT IN (SELECT mid FROM USER)').fetchone()[0] == 0)

print('USER existence check pass.')


assert(cursor.execute(
    'SELECT COUNT() from VIDEO WHERE VIDEO.aid NOT IN (SELECT oid FROM COMMENT)').fetchone()[0] == 0)

print('COMMENT existence check pass.')


cursor = conn.execute('SELECT * FROM VIDEO WHERE localpic = 1')
for row in tqdm(cursor):
    Image.open(
        'data/video_pic/{}.{}'.format(row['aid'], row['pic'].split('.')[-1])).verify()

print('VIDEO pic check pass.')


cursor = conn.execute('SELECT * FROM USER WHERE localpic = 1')
for row in tqdm(cursor):
    Image.open(
        'data/user_face/{}.{}'.format(row['mid'], row['pic'].split('.')[-1])).verify()

print('USER pic check pass.')


print('All check OK.')
