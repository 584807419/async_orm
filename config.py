# 自动判断运行平台是linux还是windows、运行服务器是测试环境还是线上环境，并应用不同配置
import platform

if platform.system() == 'Windows':
    DEBUG = True
else:
    DEBUG = False

# windows下
if DEBUG:
    DATABASE_HOST = 'pythonic.pub'
    DATABASE_PORT = 212121
    DATABASE_USER = '2121'
    DATABASE_PWD = '2121'
    DATABASE_DB = '212112'
    DATABASE_CHARSET = 'utf8mb4'
    DATABASE_AUTOCOMMIT = True
    DATABASE_MAX = 10
    DATABASE_MIN = 1
# linux下
else:
    DATABASE_HOST = '1.1.1.1'
    DATABASE_PORT = 3306
    DATABASE_USER = 'fdasfas'
    DATABASE_PWD = 'fdsafsaf&'
    DATABASE_DB = 'fadfas'
    DATABASE_CHARSET = 'utf8mb4'
    DATABASE_AUTOCOMMIT = True
    DATABASE_MAX = 50
    DATABASE_MIN = 1
