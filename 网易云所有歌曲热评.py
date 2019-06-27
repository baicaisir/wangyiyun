import requests, random, pymysql, threading, time, json, re
from redis import Redis
from datetime import datetime
import pymongo,time
def UserAgent():
    USER_AGENTS = [
        # opera  速度慢信号差，保留备用使用。
        # 'Mozilla/5.0 (Windows NT 5.1; U; en; rv:1.8.1) Gecko/20061208 Firefox/2.0.0 Opera 9.50',
        # 'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; en) Opera 9.50',
        # firefox
        'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0',
        'Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10',
        # chrome  很强！首推
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36",
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
        'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.133 Safari/534.16',
        # 360   第一个凑合   第二个速度微慢
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER',
        # taobao
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)',
        # 猎豹
        'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 SE 2.X MetaSr 1.0',
        'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SV1; QQDownload 732; .NET4.0C; .NET4.0E; SE 2.X MetaSr 1.0)',
        # UC浏览器
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 UBrowser/4.0.3214.0 Safari/537.36",
    ]
    UA = random.choice(USER_AGENTS)
    return UA
client = pymongo.MongoClient('localhost')
# 连接mongodb数据库
mgdb = client['wangyiyun']  # db = client.new_test  效果相同
db = pymysql.connect('localhost', 'root', 'zzxc', 'proxy')
cursor = db.cursor()
cursor.execute(
    "select * from proxy")
proxy_list = cursor.fetchall()
start = time.time()
ua = UserAgent()
i = 1
rddb = Redis('localhost',port=6379,db=2,decode_responses=True)

# -----------获取解析后的页面
def request_url(url):
    header = {'User-Agent': ua,}
    data={'params':'jtahr740zyyWz92qecQj0HPC8Fm5W+9oFcXgA31FQCEvmWCrOdA0AhD/0eQPgJfLsHuJTyQLVqmx1lfVkm71Wcew2ebt9rpYGDAz412JoMmzC7QSm3kGdpqS22Y0Nbx210R5gXiRO+Abj7qubBQHaIaX3Koa1Th0fYaI1En0ktiQyjmub7XjuyRDhLUexlnj','encSecKey':'cbec178ccd40a561ad61bf397192c24cff2627a28b1b1b45f8bfd108231e7323bcdb4ecf4d19142004f4c6f7ea627fce2e109ead58197d9d01388686c0f78a68265beafcfc7a6952a1cfee3fa7b66b9bb81e97518c1670c36617317c46e7c489530bf1e40d17e386a63068db53658473a2aeca75a5ecfbf1a9e00ec9d7e9587a'}
    proxy = random.choice(proxy_list)
    proxy = {proxy[1]: proxy[2]}
    reponse = requests.post(url=url,headers=header,data=data,proxies=proxy).content.decode('utf-8')  #,data=data
    print(reponse)
    return reponse

class Thread_crawl(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        # self.type_q = type_q

    def run(self):
        db = pymysql.connect('localhost', 'root', 'zzxc', 'wangyiyun')
        cursor = db.cursor()
        # cursor.execute(
        #     # song_id, nickname, content, c_time, likedCount
        #     "create table reping(song_id varchar(20),nickname varchar(50),content text,c_time time ,likedCount int )")
        j = 0
        while True:
            if rddb.llen('song_id')==0:
                break
            else:
                id = rddb.lpop('song_id')
                url = f'http://music.163.com/weapi/v1/resource/comments/R_SO_4_{id}?csrf_token='  #?csrf_token=
                # print(url)
                reponse = request_url(url)

                json_dict = json.loads(reponse)
                try:
                # 获取json
                    hot_commit_list = json_dict['hotComments']
                    if hot_commit_list == []:
                        continue
                    song_id = id
                    total = json_dict['total']  # 评论总数
                    mgdb['comment'].insert_one({'song_id': song_id, 'num': total})
                    for item in hot_commit_list:
                        # 获取json中的热门评论
                        nickname = item['user']['nickname']
                        content = item['content']
                        s_time = item['time']
                        s_time = int(s_time/1000)
                        a = datetime.utcfromtimestamp(s_time)
                        c_time = a.strftime('%Y/%m/%d %H:%M:%S')
                        likedCount = int(item['likedCount'])
                        # song_id = song_id
                        cursor.execute(
                            "insert into reping(song_id ,nickname ,content,c_time,likedCount) values (%s,%s,%s,%s,%s)",(song_id,nickname,content,c_time,likedCount))
                        # mgdb['reping_1'].insert_one(
                        #     {'song_id': song_id, 'nickname': nickname, 'content': content, 'time': time,
                        #      'likedCount': likedCount})
                    db.commit()
                except Exception as e:
                    j +=1
                    # time.sleep(120)
                    rddb.rpush('song_id',id)
                    print(e,j)
                finally:
                    if j >20:
                        break

# type_q = Queue()
if __name__ == '__main__':
    list_thread = ['线程1', '线程2', '线程3', '线程4', '线程5', '线程6', '线程7', '线程8', '线程9', '线程10', '线程11']
    # list_thread = ['线程1']
    thread_list = []
    # 启动多线程
    for jc_name in list_thread:
        craw1 = Thread_crawl()
        craw1.start()
        thread_list.append(craw1)
        # time.sleep(5)
    for i in thread_list:
        i.join()
    end_time = time.time()
    cost = end_time - start
    print('停止时间',time.asctime(time.localtime()))
    print('花费时间',cost)
