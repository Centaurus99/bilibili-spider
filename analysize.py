import sqlite3
import os
import time
import json
import configparser
import numpy as np
from matplotlib import pyplot as plt
from matplotlib import font_manager, colors
from tqdm import trange, tqdm
from collections import Counter
from wordcloud import WordCloud

# 载入数据库名
CONFIG = configparser.ConfigParser()
CONFIG.read('config.ini', encoding='utf-8')
DB_NAME = CONFIG['common'].get('database_name', fallback='data')

# 连接数据库
conn = sqlite3.connect('data/' + DB_NAME + '.sqlite3')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

if not os.path.exists('analysize'):
    os.mkdir('analysize')


def str_to_timestamp(time_str):
    '''将时间字符串转换成时间戳'''
    return int(time.mktime(time.strptime(time_str, "%Y-%m-%d %H:%M:%S")))


def pic01():
    '''统计视频信息按时段的分布'''

    print('pic0 and pic1')

    # 数据库查询
    view_sum = [0] * 24
    count = [0] * 24
    for day in trange(1, 32, position=1):
        for hour in trange(0, 24, position=0):
            st = str_to_timestamp('2021-08-{} {}:00:00'.format(day, hour))
            ed = str_to_timestamp('2021-08-{} {}:59:59'.format(day, hour))
            count[hour] += cursor.execute(
                'SELECT COUNT() from VIDEO WHERE pubdate BETWEEN ? and ?', (st, ed)).fetchone()[0]
            view_sum[hour] += cursor.execute(
                'SELECT SUM(view) from VIDEO WHERE pubdate BETWEEN ? and ?', (st, ed)).fetchone()[0]

    # 数据准备
    N = 24
    view_sum = np.array(view_sum)
    count = np.array(count)
    theta = np.linspace(0.0 + np.pi / N, 2 * np.pi +
                        np.pi / N, N, endpoint=False)
    width = 2 * np.pi / N
    colors = np.array(['#7bbfea', '#33a3dc'] * 12)

    # 图表绘制
    plt.figure(figsize=(16, 9))
    plt.style.use('ggplot')
    ax = plt.subplot(111, projection='polar')
    ax.set_axisbelow(True)
    ax.set_theta_direction(-1)
    ax.set_theta_zero_location('N')
    ax.set_rlabel_position(90)
    ax.bar(theta, count / 1000, width=width, bottom=0.0, color=colors)
    ax.set_xticks(np.linspace(0, 2*np.pi, 24, endpoint=False))
    ax.set_xticklabels(range(0, 24), fontsize=20)
    ax.set_yticks([i for i in range(0, 6)])
    ax.set_yticklabels([0] + ['{}K'.format(i)
                       for i in range(1, 6)], fontsize=15)
    font = font_manager.FontProperties(fname='font/msyh.ttc', size=28)
    ax.set_title('2021年08月 B站数码区视频投稿数量在一天中的分布', fontproperties=font)
    plt.suptitle("2021.09.05 统计", x=0.75, y=0.9,
                 fontproperties=font, fontsize=18)
    plt.savefig('analysize/pic0.png')

    plt.figure(figsize=(16, 9))
    plt.style.use('ggplot')
    ax = plt.subplot(111, projection='polar')
    ax.set_axisbelow(True)
    ax.set_theta_direction(-1)
    ax.set_theta_zero_location('N')
    ax.set_rlabel_position(90)
    ax.bar(theta, view_sum / count / 1000,
           width=width, bottom=0.0, color=colors)
    ax.set_xticks(np.linspace(0, 2*np.pi, 24, endpoint=False))
    ax.set_xticklabels(range(0, 24), fontsize=20)
    ax.set_yticks([i for i in range(0, 11, 2)])
    ax.set_yticklabels([0] + ['{}K'.format(i)
                       for i in range(2, 11, 2)], fontsize=15)
    font = font_manager.FontProperties(fname='font/msyh.ttc', size=28)
    plt.title('2021年08月 B站数码区一天中各时段发布视频的平均播放量', fontproperties=font)
    plt.suptitle("2021.09.05 统计", x=0.75, y=0.9,
                 fontproperties=font, fontsize=18)
    plt.savefig('analysize/pic1.png')


def pic2():
    '''分析标签'''

    print('\npic2')

    # 数据库查询与数据统计
    st = str_to_timestamp('2021-08-{} {}:00:00'.format(1, 0))
    ed = str_to_timestamp('2021-08-{} {}:59:59'.format(31, 23))
    total_num = cursor.execute(
        'SELECT COUNT() from VIDEO WHERE pubdate BETWEEN ? and ?', (st, ed)).fetchone()[0]
    data = cursor.execute(
        'SELECT * from VIDEO WHERE pubdate BETWEEN ? and ?', (st, ed))

    count = Counter()
    with tqdm(total=total_num) as pbar:
        for row in data:
            keywords = row['keywords']
            count += Counter(keywords.split(','))
            pbar.update(1)
    with open('count.json', 'w') as f:
        json.dump(count, f)

    # 删除无效标签
    del_list = ['数码', '科技', '哔哩哔哩', 'B站', '弹幕',
                'Bilibili', '打卡挑战', '科技猎手', '必剪创作']
    for name in del_list:
        del count[name]

    N = 30

    # 生成词云
    color_list = ['#2585a6', '#228fbd', '#2468a2',
                  '#2570a1', '#009ad6', '#145b7d']
    colormap = colors.ListedColormap(color_list)
    wc = WordCloud(width=1600, height=900, font_path='font/msyh.ttc', max_words=N, mode='RGBA',
                   background_color='white', colormap=colormap).generate_from_frequencies(dict(count.most_common(N)))
    wc.to_file('analysize/pic2-1.png')

    # 生成图表
    y, x = map(list, zip(*count.most_common(N)))

    plt.figure(figsize=(16, 9))
    plt.style.use('ggplot')
    font = font_manager.FontProperties(fname='font/msyh.ttc')
    y_pos = range(len(y), 0, -1)
    plt.barh(y_pos, x, color='#33a3dc')
    plt.yticks(y_pos, y, fontproperties=font, fontsize=14, rotation=10)
    plt.xticks(fontsize=18)
    plt.xlabel('词频 / 次', fontproperties=font, fontsize=20)
    plt.ylabel('标签', fontproperties=font, fontsize=20)
    plt.title('2021年08月 B站数码区视频标签词频统计前 30 名\n',
              fontproperties=font, fontsize=28)
    plt.suptitle("2021.09.05 统计", x=0.75, y=0.88,
                 fontproperties=font, fontsize=18)
    plt.tight_layout()
    plt.savefig('analysize/pic2-0.png')


def pic3():
    '''分析播放量与视频时长关系'''

    print('\npic3')

    # 查询数据库
    x = []
    y = []
    interval = 600
    N = 6
    for i in trange(N):
        y.append(cursor.execute('SELECT AVG(view) from VIDEO WHERE duration BETWEEN ? and ?',
                 (interval * i, interval * (i + 1) - 1)).fetchone()[0])
        x.append('{}~{} 分钟'.format(10 * i, 10 * (i + 1)))
    x.append('{} 分钟以上'.format(10 * N))
    y.append(cursor.execute(
        'SELECT AVG(view) from VIDEO WHERE duration > ?', (interval * N,)).fetchone()[0])

    # 生成图表
    plt.figure(figsize=(16, 9))
    plt.style.use('ggplot')
    font = font_manager.FontProperties(fname='font/msyh.ttc')
    plt.bar(range(len(x)), y, color='#33a3dc')
    plt.xticks(range(len(x)), x, fontproperties=font, fontsize=18)
    plt.yticks(fontsize=18)
    plt.xlabel('视频时长', fontproperties=font, fontsize=20)
    plt.ylabel('平均播放量 / 次', fontproperties=font, fontsize=20)
    plt.title('2021年08月 B站数码区视频平均播放量随视频总时长的分布\n',
              fontproperties=font, fontsize=28)
    plt.suptitle("2021.09.05 统计", x=0.75, y=0.92,
                 fontproperties=font, fontsize=18)
    plt.savefig('analysize/pic3.png')


pic01()
pic2()
pic3()
