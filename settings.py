# @Time    : 2021/4/18 23:06
# @Author  : LXJ
# @FileName: settings.py
# @Software: PyCharm

MAX_SCORE = 50

# 日志配置信息
import logging

# 默认的配置
LOG_LEVEL = logging.INFO # 默认等级
LOG_FMT = '%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s: %(message)s' # 默认格式
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S' # 默认时间格式
LOG_FILENAME = 'log.log' # 默认日志文件名称

# 日志配置信息 end

# 测试IP的超时时间
TEST_TIMEOUT = 10

# MongoDB数据库的URL
MONGO_URL = 'mongodb://127.0.0.1:27017'