# bilibili-spider

使用免费代理池的 bilibili 分区视频爬虫

程序设计训练课程作业，编写时使用 `Python 3.8.7`

## 致谢

- 免费代理池使用 [ProxyPool 爬虫代理IP池](https://github.com/jhao104/proxy_pool)

- UserAgent 列表来自 [List-of-user-agents](https://github.com/tamimibrahim17/List-of-user-agents)

## 使用说明

### 爬虫说明

- 编辑 `config.ini` 中相关配置，并指定爬取分区和数量
- 运行 `spider.py` 开始爬取，若先前已进行一些爬取将恢复进度
- 爬取结果将存储在数据库 `data/{database_name}.sqlite3` 中，视频封面 `{aid}.jpg/.png` 存储在 `data/video_pic/` 中，用户头像 `{uid}.jpg/.png` 存储在 `data/user_face` 中
- `Ctrl + C` 终止程序后需等待程序自行保存数据并退出，防止数据丢失
- 可以在 `spider.py` 中调整 `logging` 等级，默认为 `INFO` 等级，爬取成功也会输出信息
- 每隔十秒会输出进度信息

### 数据库说明

- 爬虫使用 `SQLite3` 作为数据库

- 对于视频，将存储共 21 个字段信息：AV号、BV号、投稿cid、封面图片地址、视频标题、视频简介、视频关键词、自制/转载、视频总时长、分P数、发布时间、播放量、弹幕数、点赞数、投币数、收藏数、分享数、评论数、作者uid、封面是否已下载到本地、爬取时间
- 对于用户，将存储共 10 个字段信息：用户ID、用户名、性别、头像地址、个人签名、用户等级、关注量、粉丝数、头像是否已下载到本地、爬取时间
- 对于评论，将存储共 3 个字段信息：评论oid、评论内容（json 列表）、爬取时间
- 数据库还会存储当前爬取到的视频列表页码用于恢复进度

### 代理说明

#### 架设代理池

代理池使用使用 [ProxyPool 爬虫代理IP池](https://github.com/jhao104/proxy_pool) ，简要教程如下：

##### 部署 Redis 数据库

使用 docker 部署的步骤：

- 拉取镜像： `docker pull redis:latest`

- 运行容器并开放对应端口： `docker run -itd --name redis -p 6379:6379 redis`

- 进入容器： `docker exec -it redis /bin/bash`

- 使用 `redis-cli` 连接数据库： `redis-cli`

- 设置密码（Redis6开始似乎没有密码无法连接）： `config set requirepass ********`

`Warning:` 该密码在容器重启后需要重新设置，永久设置需在配置文件中修改

##### 部署 ProxyPool 本体

- 使用 [ProxyPool 爬虫代理IP池](https://github.com/jhao104/proxy_pool) 的教程
- 使用免费代理池可适当调整增加文件中的 `POOL_SIZE_MIN`

#### 使用代理池

- 在 `config.ini` 中将 `use_proxy` 设为 `true` ，并将 `proxy_url` 设为代理池的地址，即可使用代理池
- 当 `allow_fallback` 为 `true` 时，若从代理池获取代理失败，则不使用代理（不建议启用，因为线程中相邻请求间不设间隔）
- 当 `allow_delete` 为 `true` 时，会通知代理池删除请求失败时使用的代理服务器（使用免费代理池不建议启用，因为代理质量较差，大部分为间隔可用）

### 线程数量说明

- 若使用免费代理池，可适量增加线程数 `threads`（如调至 100），并相应扩大缓冲区大小 `waiting_list`

### `Checker` 说明

- 运行 `checker.py` 可以检查数据完整性，若不完整会抛出对应异常

### 数据分析说明

- `analysize.py` 使用数据库数据生成了一些简单的数据分析图表，具体参见 [数据分析](analysize.md)

## 设计简介

### 各文件简介

- `spider.py`：爬虫主程序，载入配置、实现爬虫各功能
- `tools.py`：网络工具，获取代理服务器、随机 `UserAgent` 和国内 `IP`
- `database.py`：数据库工具，提供爬虫与数据库交互接口
- `checker.py`：数据检验工具
- `analysize.py`：数据分析样例程序
- `config.ini`：配置文件
- `ip_list.json`：国内 `IP` 段列表
- `user_agent.json`：`UserAgent` 列表

### 程序流程

- 启动程序，载入配置文件
- 载入数据库，恢复进度
- 启动 视频列表爬虫线程 和 数据库线程，均为单线程，从而保证获取视频列表不遗漏视频，数据库读写不上锁
- 启动 视频爬虫 和 评论爬虫 和 1.5 倍数量的图片爬虫（因为视频封面和用户头像均需要使用这类线程）
- 视频列表爬虫通过 `API` 获取视频列表并放入队列，视频爬虫获得视频爬取任务后访问对应视频页面，解析 `html` 提取视频信息和作者信息，传递给数据库线程进行写入，并放入图片获取任务队列和评论获取任务队列，供图片爬虫和评论爬虫使用
- 具体细节见程序源码

## 运行测试

- 使用该爬虫爬取 科技-数码 分区（`rid = 95`）下 2021年08月 的所有视频，共计 `81380` 条视频，`42872` 名UP主，线程数 `threads` 设为 `100`，列表缓冲大小 `waiting_list` 设为 `1000` ，使用本地架设的免费代理池，约十小时爬完。

