import requests,random,threading,time,pymongo,datetime
from redis import Redis

class Thread_crawl(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.header=self.header()
        client = pymongo.MongoClient('localhost')  # 连接mongodb数据库
        self.mgdb = client.wangyiyun

    def run(self):
        rddb = Redis('localhost', port=6379, db=1, decode_responses=True)   #连接redis数据库
        while True:
            if rddb.llen('song_id'):  #判断song_id是否为空
                id = rddb.lpop('song_id')
                self.songdetail(id)
                self.lyric(id)
                self.hot_comments(id)
            else:
                break

    def header(self):
        USER_AGENTS = [
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
        header = {
            'User-Agent': UA,
            'Cookie':'_iuqxldmzr_=32; _ntes_nnid=914a9fc77bfa814e6c7b69f1723ae8a2,1559445302244; _ntes_nuid=914a9fc77bfa814e6c7b69f1723ae8a2; WM_NI=NB%2FYRTHUOoGnChIvZo7%2Fn4fEbmaVgCiNKSK2K%2BQAynjktExAHJFDe%2FVd%2FpdvoWDxu6hYoEnbpbv6%2BqTkSuY%2Bv0CMxtHMr99q0uFKVnUfveNgcqSbvfVQ2G6WfuEA41gfQVQ%3D; WM_NIKE=9ca17ae2e6ffcda170e2e6eed2e233a98684dac53481928bb2c15e868b9baaee72f487f7b5ce63f59f8fb5f92af0fea7c3b92a8e9ee1bad1608c8b8190c66681b88797d965f5f1a2d5e54fa2b3afb7e649ab9285b4ca3a89f59fa2fc66aab5bca6fb4b8c968aa5e82596bce191e77ead898c99f83eac87a1a6ed6ab4bebad7aa7b98b59d96dc21b6f08daefb808c93bedabc7bf4ab8aacd56697baba86cd6da1af9d83f56e968ffea8b55da6ab8ba2b73ba5ba9fb7d437e2a3; WM_TID=eaq%2F25XiSmhEARBQAQNsmhMpCei7AETj; JSESSIONID-WYYY=ruAPfbszYVajXx9qYAgQkizniRGZo9u%5Csi0PfnT9d1iqnyVAw%5CM%5CRRiA%5C6XP2sGzGRWuG%5Cn%2BprchEedVHzq7VdtF%5CsebXb2VoxhthiYuAHOOOhjm2mACA2W%5CAYSEQMSWRvv4saxh8eP1%5Cd5ojDYHTJQbTWUjPsVjvvj0xuHc7Xoko%2BGr%3A1559482546673'
        }
        return header

    def songdetail(self,id):  #通过接口获取详情页信息
        detail_url = 'http://music.163.com/api/song/detail/?id={id}&ids=%5B{id}%5D'.format(id=id)
        response = requests.get(url=detail_url,headers=self.header).json()
        # print(response)
        self.mgdb.song_detail.insert_one({'song_id':response['songs'][0]['id'],'song_name':response['songs'][0]['name'],'singer':response['songs'][0]['artists'][0]['name'],'singer_id':response['songs'][0]['artists'][0]['id'],'album_id':response['songs'][0]['album']['id'],'album':response['songs'][0]['album']['name']})
        print('歌曲详情存储完成')

    def lyric(self,id):  #获取歌词
        lyric_url = 'http://music.163.com/api/song/lyric?os=pc&id={id}&lv=-1&kv=-1&tv=-1'.format(id=id)
        response = requests.get(url=lyric_url, headers=self.header).json()
        try:  #存在歌词不存在的情况
            self.mgdb.song_lyric.insert_one({'song_id':response['lyricUser']['id'],'lyric':response['lrc']['lyric']})
            print('歌词存储完成')
        except Exception as e:
            print(e)
    def hot_comments(self,id):
        hot_url = 'http://music.163.com/api/v1/resource/comments/R_SO_4_{id}?csrf_token='.format(id=id)
        response = requests.get(url=hot_url, headers=self.header).json()
        try:  #有的歌曲无热评
            hot_commit_list = response['hotComments']
            total = response['total']  # 评论总数
            self.mgdb.song_detail.update_one({'song_id': id},{'$set':{'total':total}})
            for item in hot_commit_list:
                # 获取json中的热门评论
                nickname = item['user']['nickname']
                content = item['content']
                s_time = item['time']
                s_time = int(s_time / 1000)
                a = datetime.utcfromtimestamp(s_time)
                c_time = a.strftime('%Y/%m/%d %H:%M:%S')
                likedCount = int(item['likedCount'])
                self.mgdb['hot_comments'].insert_one(
                    {'song_id': id, 'nickname': nickname, 'content': content, 'time': time,
                     'likedCount': likedCount})
            print('评论存储完成')
        except Exception as e:
            print(e)



if __name__ == '__main__':
    start = time.time()
    thread_list = []
    # 启动多线程
    for i in range(10):
        craw1 = Thread_crawl()
        craw1.start()
        thread_list.append(craw1)

    for i in thread_list:
        i.join()
    end_time = time.time()
    cost = end_time - start
    print('花费时间', cost)
    print('程序停止时间', time.asctime(time.localtime()))

