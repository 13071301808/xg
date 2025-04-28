# 多进程
import multiprocessing

"""gunicorn+gevent 的配置文件"""
'''执行：gunicorn -c gunicorn_config.py flask_simple:my_app
'''

# 预加载资源
preload_app = False
# 绑定 ip + 端口
bind = "0.0.0.0:5001"
# 进程数 = cup数量 * 2 + 1
workers = 5

# 线程数 = cup数量 * 2
# threads = 1

# 等待队列最大长度,超过这个长度的链接将被拒绝连接
backlog = 2048

# 工作模式--协程
worker_class = "gevent"

# 最大客户客户端并发数量,对使用线程和协程的worker的工作有影响
# 服务器配置设置的值  1200：中小型项目  上万并发： 中大型
# 服务器硬件：宽带+数据库+内存
# 服务器的架构：集群 主从
worker_connections = 1200
