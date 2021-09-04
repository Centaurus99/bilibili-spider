import time
import configparser
import logging
import requests
import json
import threading
import re
import io
from queue import Queue, Empty
from bs4 import BeautifulSoup
from PIL import Image

import tools
import database

logging.basicConfig(level=logging.INFO)

# 载入配置
CONFIG = configparser.ConfigParser()
try:
    logging.info('载入配置文件.')
    CONFIG.read('config.ini', encoding='utf-8')
    API_URL = CONFIG['common'].get('api_url')
    VIDEO_URL = CONFIG['common'].get('video_url')
    DB_NAME = CONFIG['common'].get('database_name', fallback='data')
    THREADS = CONFIG['common'].getint('threads', fallback=1)
    WL_MAX = CONFIG['common'].getint('waiting_list', fallback=100)
    USE_PROXY = CONFIG['proxy'].getboolean('use_proxy', fallback=False)
    PROXY_URL = ''
    if USE_PROXY:
        PROXY_URL = CONFIG['proxy'].get('proxy_url')
    ALLOW_FALLBACK = CONFIG['proxy'].getboolean(
        'allow_fallback', fallback=False)
    ALLOW_DELETE = CONFIG['proxy'].getboolean('allow_delete', fallback=False)
    RID = CONFIG['spider'].get('rid')
    ST_PN = CONFIG['spider'].getint('start_pn', fallback=1)
    VIDEO_NUM = CONFIG['spider'].getint('video_num', fallback=-1)
    if VIDEO_NUM < 0:
        VIDEO_NUM = float('inf')

    logging.info('[common][api_url]: {}'.format(API_URL))
    logging.info('[common][video_url]: {}'.format(VIDEO_URL))
    logging.info('[common][database_name]: {}'.format(DB_NAME))
    logging.info('[common][threads]: {}'.format(THREADS))
    logging.info('[common][waiting_list]: {}'.format(WL_MAX))
    logging.info('[proxy][use_proxy]: {}'.format(USE_PROXY))
    logging.info('[proxy][proxy_url]: {}'.format(PROXY_URL))
    logging.info('[proxy][allow_fallback]: {}'.format(ALLOW_FALLBACK))
    logging.info('[proxy][allow_delete]: {}'.format(ALLOW_DELETE))
    logging.info('[spider][rid]: {}'.format(RID))
    logging.info('[spider][start_pn]: {}'.format(ST_PN))
    logging.info('[spider][video_num]: {}'.format(VIDEO_NUM))
except:
    logging.critical('配置文件载入失败!')
    exit(0)
START_FLAG = True
WAIT_TIME = 5

# 初始化任务队列
task_queue = Queue(maxsize=1000)
retry_queue = Queue()
video_queue = Queue()
user_queue = Queue()
video_pic_queue = Queue()
user_pic_queue = Queue()
video_pic_res_queue = Queue()
user_pic_res_queue = Queue()
comment_queue = Queue()
comment_res_queue = Queue()


def GET(url, params={}, use_proxy=USE_PROXY, retry_time=3):
    ''' 带有重试与代理的 GET 方法

    url:        请求 URL
    params:     请求参数
    use_proxy:  是否使用代理
    retry_time: 重试次数上限
    '''
    proxies = {}
    res = None

    # 获取代理
    if use_proxy:
        proxy = tools.get_proxy(PROXY_URL)
        while (not ALLOW_FALLBACK) and (not proxy) and START_FLAG:
            logging.warning('获取代理失败. 重试.')
            time.sleep(1)
            proxy = tools.get_proxy(PROXY_URL)
        if not proxy:
            logging.warning('获取代理失败. 不使用代理.')
        else:
            proxies = {
                'http': 'http://{}'.format(proxy), 'https': 'https://{}'.format(proxy)}

    # 尝试请求
    while (retry_time > 0) and START_FLAG:
        try:
            headers = {
                'User-Agent': tools.get_UA(),
                'X-Forwarded-For': tools.get_IP()
            }
            res = requests.get(url, headers=headers,
                               params=params, proxies=proxies, timeout=3)
            logging.debug(res)
            res.raise_for_status()
            break
        except:
            retry_time -= 1
            if (retry_time == 0):
                if ALLOW_DELETE:
                    logging.warning('删除代理 {} !'.format(proxy))
                    tools.delete_proxy(PROXY_URL, proxy)
                raise
            logging.debug('GET失败. 重试.(剩余:{} 次)'.format(retry_time))
            time.sleep(1)

    if res == None:
        raise requests.exceptions.RequestException()

    # 验证请求完整性
    if ('Content-Length' in res.headers) and (len(res.content) != int(res.headers['Content-Length'])):
        logging.warning('{} 获取信息不完整. 重试.'.format(url))
        raise requests.exceptions.RequestException('获取信息不完整')
    return res


# 由于作业要求不能全部使用 api, 视频信息部分爬取 html 页面进行解析
class VideoSpiderWithoutAPI(threading.Thread):
    '''爬取视频页面线程

    thread_id:  线程 ID
    video_url:  视频域名
    '''

    def __init__(self, thread_id, video_url):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.video_url = video_url
        self.finish = False

    def run(self):
        logging.info('启动 VideoSpider: {}'.format(self.thread_id))
        self.work()
        logging.info('退出 VideoSpider: {}'.format(self.thread_id))

    def work(self):
        while START_FLAG:
            target = None

            # 获取任务
            while START_FLAG:
                try:
                    if retry_queue.empty():
                        target = task_queue.get(block=True, timeout=WAIT_TIME)
                    else:
                        target = retry_queue.get(
                            block=True, timeout=WAIT_TIME)
                    self.finish = False
                    break
                except:
                    self.finish = True
                    logging.debug('视频队列空，等待 {} 秒...'.format(WAIT_TIME))
            if not target:
                break

            # 请求视频页面，获取视频和作者信息
            try:
                video_data = {}
                video_data['aid'] = target[0]
                video_data['bvid'] = target[1]
                video_data['cid'] = target[2]
                video_data['url'] = '{}{}'.format(self.video_url, target[1])

                res = GET(video_data['url'], {})

                json_string = re.search(
                    '<script>window.__INITIAL_STATE__=({.*});', res.text).group(1)
                json_data = json.loads(json_string)
                soup = BeautifulSoup(res.text, 'html.parser')

                if not json_data['videoData']['stat']:
                    logging.warning('视频 {} 已被删除, 跳过!'.format(target[0]))
                    continue

                video_data['title'] = soup.find(
                    'meta', attrs={'name': 'title'})['content']
                video_data['keywords'] = soup.find(
                    'meta', attrs={'name': 'keywords'})['content']
                video_data['pn'] = target[3]
                video_data.update(json_data['videoData'])
                user_data = json_data['upData']

                video_queue.put(video_data)
                user_queue.put(user_data)

                logging.info('获取视频 {} 成功.'.format(target[0]))

            except requests.exceptions.RequestException:
                logging.debug('获取视频 {} 失败. 网络错误. 重试.'.format(target[0]))
                retry_queue.put(target)
            except:
                logging.error('获取视频 {} 失败. 未知错误. 退出.'.format(target[0]))
                retry_queue.put(target)
                raise


class VideoListSpider(threading.Thread):
    '''爬取某分区视频列表线程

    api_url:    API 域名
    rid:        分区 rid
    start_pn:   起始页码 (每页 50 个)
    limit:      尝试获取视频数量上限
    '''

    def __init__(self, api_url, rid, start_pn, limit):
        threading.Thread.__init__(self)
        self.url = api_url + 'web-interface/newlist'
        self.rid = rid
        self.limit = limit
        self.pn = start_pn
        self.finish = False

    def run(self):
        logging.info('启动 VideoListSpider')
        self.work()
        self.finish = True
        logging.info('退出 VideoListSpider')

    def work(self):
        logging.warning('恢复进度. 第 {} 页'.format(self.pn))
        params = {
            'rid': self.rid,
            'pn': self.pn,
            'ps': 50
        }
        while START_FLAG:
            if self.limit <= 0:
                logging.warning('任务队列视频数已达上限, 停止获取列表.')
                break
            try:
                params['pn'] = self.pn
                data = GET(self.url, params, False).json()
                logging.info('获取视频列表第 {} 页成功.'.format(self.pn))

                for video in data['data']['archives']:
                    # 尝试放入任务队列
                    if (self.limit <= 0):
                        break
                    while START_FLAG:
                        try:
                            task_queue.put((video['aid'], video['bvid'], video['cid'], self.pn),
                                           block=True, timeout=WAIT_TIME)
                            self.limit -= 1
                            break
                        except:
                            logging.debug('视频队列满，等待 {} 秒...'.format(WAIT_TIME))

                if (not data['data']['archives']):
                    logging.warning('已将当前分区下所有视频载入列表, 停止获取列表.')
                    break

                time.sleep(1)
                self.pn += 1

            except:
                logging.debug('获取视频列表第 {} 页失败. 重试.'.format(self.pn))


class PicSpider(threading.Thread):
    '''爬取图片线程

    thread_id:  线程 ID
    '''

    def __init__(self, thread_id):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.finish = False

    def run(self):
        logging.info('启动 PicSpider: {}'.format(self.thread_id))
        self.work()
        logging.info('退出 PicSpider: {}'.format(self.thread_id))

    def work(self):
        while START_FLAG:
            data = ('None')
            pic_type = 'None'
            try:
                # 优先爬取视频封面
                if not video_pic_queue.empty():
                    pic_type = '视频'
                    data = video_pic_queue.get(block=True, timeout=WAIT_TIME)
                    self.finish = False
                    res = GET(data[1])

                    try:
                        Image.open(io.BytesIO(res.content)).verify()
                    except:
                        meg = '{} {} 图片不完整. 重试.'.format(pic_type, data[0])
                        logging.warning(meg)
                        raise requests.exceptions.RequestException(meg)

                    with open('data/video_pic/{}.{}'.format(data[0], data[1].split('.')[-1]), 'wb') as f:
                        f.write(res.content)
                    video_pic_res_queue.put(data[0])

                # 然后爬取用户头像
                else:
                    pic_type = '用户'
                    data = user_pic_queue.get(block=True, timeout=WAIT_TIME)
                    self.finish = False
                    res = GET(data[1])

                    try:
                        Image.open(io.BytesIO(res.content)).verify()
                    except:
                        meg = '{} {} 图片不完整. 重试.'.format(pic_type, data[0])
                        logging.warning(meg)
                        raise requests.exceptions.RequestException(meg)

                    with open('data/user_face/{}.{}'.format(data[0], data[1].split('.')[-1]), 'wb') as f:
                        f.write(res.content)
                    user_pic_res_queue.put(data[0])

                logging.info('获取图片 {} {} 成功.'.format(pic_type, data[0]))

            except Empty:
                self.finish = True
                logging.debug('图片队列空，等待 {} 秒...'.format(WAIT_TIME))
            except requests.exceptions.RequestException:
                logging.debug(
                    '获取图片 {} {} 失败. 网络错误. 重试.'.format(pic_type, data[0]))
                if pic_type == '视频':
                    video_pic_queue.put(data)
                elif pic_type == '用户':
                    user_pic_queue.put(data)
            except:
                logging.error(
                    '获取图片 {} {} 失败. 未知错误. 退出!'.format(pic_type, data[0]))
                raise


class CommentSpider(threading.Thread):
    '''爬取评论线程

    thread_id: 线程 ID
    api_url
    '''

    def __init__(self, thread_id, api_url):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.url = api_url + 'v2/reply/main'
        self.finish = False

    def run(self):
        logging.info('启动 CommentSpider: {}'.format(self.thread_id))
        self.work()
        logging.info('退出 CommentSpider: {}'.format(self.thread_id))

    def work(self):
        while START_FLAG:
            oid = None

            # 获取任务
            while START_FLAG:
                try:
                    oid = comment_queue.get(block=True, timeout=WAIT_TIME)
                    self.finish = False
                    break
                except:
                    self.finish = True
                    logging.debug('评论队列空，等待 {} 秒...'.format(WAIT_TIME))
            if not oid:
                break

            # 获取评论
            try:
                params = {
                    'oid': oid[0],
                    'next': 0,
                    'type': 1,
                    'mode': 3
                }
                res = GET(self.url, params=params)
                json_data = res.json()
                replies = []
                try:
                    if json_data['data']['replies']:
                        for data in json_data['data']['replies']:
                            replies.append(data['content']['message'])
                except:
                    logging.warning('{} 评论格式出错. 跳过.'.format(oid[0]))

                comment = {
                    'oid': oid[0],
                    'data': json.dumps(replies, ensure_ascii=False)
                }
                comment_res_queue.put(comment)
                logging.info('获取评论 {} 成功.'.format(oid[0]))
            except requests.exceptions.RequestException:
                logging.debug('获取评论失败. 网络错误. 重试.')
                comment_queue.put(oid)
            except:
                logging.error('获取评论 {} 失败. 未知错误. 退出!'.format(oid[0]))
                raise


class SpiderDB(threading.Thread):
    '''数据库交互线程

    db_name:    数据库名称
    '''

    def __init__(self, db_name):
        threading.Thread.__init__(self)
        self.db_name = db_name
        self.finish = False

    def run(self):
        logging.info('启动 SpiderDB')
        self.db = database.Database(self.db_name)
        self.work()
        del self.db
        logging.info('退出 SpiderDB')
        logging.info('数据已保存.')

    def work(self):
        # 根据数据库获取图片与评论任务
        for data in self.db.get_video_pic_list():
            video_pic_queue.put(data)
        for data in self.db.get_user_face_list():
            user_pic_queue.put(data)
        for data in self.db.get_comment_list():
            comment_queue.put(data)

        while START_FLAG or (not self.finish):
            # 存入视频结果, 加入图片与评论任务队列
            while not video_queue.empty():
                self.finish = False
                video = video_queue.get(block=False)
                video_pic_queue.put((video['aid'], video['pic']))
                comment_queue.put((video['aid'],))
                self.db.insert_video(video)
                self.db.update_temp_pn(video['pn'])

            # 存入用户结果, 加入图片队列
            while not user_queue.empty():
                self.finish = False
                user = user_queue.get(block=False)
                user_pic_queue.put((user['mid'], user['face']))
                self.db.insert_user(user)

            # 存入视频封面结果
            while not video_pic_res_queue.empty():
                self.finish = False
                aid = video_pic_res_queue.get(block=False)
                self.db.update_video_pic(aid)

            # 存入用户头像结果
            while not user_pic_res_queue.empty():
                self.finish = False
                mid = user_pic_res_queue.get(block=False)
                self.db.update_user_pic(mid)

            # 存入评论结果
            while not comment_res_queue.empty():
                self.finish = False
                comment = comment_res_queue.get(block=False)
                self.db.insert_comment(comment)

            time.sleep(WAIT_TIME)
            self.finish = video_queue.empty() and user_queue.empty() and video_pic_res_queue.empty(
            ) and user_pic_res_queue.empty() and comment_res_queue.empty()


def main():
    # 从数据库载入进度
    db = database.Database(DB_NAME)
    global VIDEO_NUM
    VIDEO_NUM -= db.count_videos()
    if st_pn := db.get_temp_pn():
        global ST_PN
        ST_PN = st_pn
    del db

    # 单线程初始化 视频列表爬虫线程 和 数据库线程
    list_spider = VideoListSpider(API_URL, RID, ST_PN, VIDEO_NUM)
    list_spider.start()
    spider_db = SpiderDB(DB_NAME)
    spider_db.start()

    # 初始化 视频爬虫 和 评论爬虫
    spider_list = []
    comment_spider_list = []
    for i in range(0, THREADS):
        spider_list.append(VideoSpiderWithoutAPI(i, VIDEO_URL))
        spider_list[i].start()
        comment_spider_list.append(CommentSpider(i, API_URL))
        comment_spider_list[i].start()

    # 1.5倍数量初始化图片爬虫
    pic_spider_list = []
    for i in range(0, THREADS + (THREADS // 2)):
        pic_spider_list.append(PicSpider(i))
        pic_spider_list[i].start()

    def is_end():
        '''判断爬虫是否全部结束'''
        if list_spider.limit > 0 or \
                task_queue.qsize() > 0 or \
                video_pic_queue.qsize() > 0 or \
                user_pic_queue.qsize() > 0 or \
                comment_queue.qsize() > 0:
            return False
        if not list_spider.finish:
            return False
        if not spider_db.finish:
            return False
        for spider in spider_list:
            if not spider.finish:
                return False
        for spider in pic_spider_list:
            if not spider.finish:
                return False
        for spider in comment_spider_list:
            if not spider.finish:
                return False
        return True

    # 状态显示与结束判断
    try:
        while True:
            msg = '-' * 20 + '\n' +\
                '未入队视频数:   {}\n' + \
                '排队视频数:     {}\n' + \
                '排队视频封面数: {}\n' + \
                '排队用户头像数: {}\n' + \
                '排队评论数:     {}\n' + '-' * 20 + '\n'
            msg = msg.format(list_spider.limit, task_queue.qsize(),
                             video_pic_queue.qsize(), user_pic_queue.qsize(),
                             comment_queue.qsize())
            print(msg)
            if is_end():
                logging.warning('爬虫任务结束, 准备退出.')
                raise Exception()

            time.sleep(10)
    except:
        pass

    logging.warning('开始退出...')

    # 通知线程退出
    global START_FLAG
    START_FLAG = False

    # 阻塞等待线程退出
    try:
        list_spider.join()
        for spider in spider_list:
            spider.join()
        for spider in pic_spider_list:
            spider.join()
        for spider in comment_spider_list:
            spider.join()
        spider_db.join()
    except:
        pass

    logging.warning('退出完成.')


if __name__ == '__main__':
    main()
