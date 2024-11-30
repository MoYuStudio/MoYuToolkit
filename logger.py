# coding=utf-8

import datetime
import logging

class Logger:
    def __init__(self,name="",path="log"):
        # 配置日志设置
        logging.basicConfig(level=logging.INFO,
                            format="%(asctime)s - %(levelname)s - %(message)s",
                            filename=path+"\\"+str(name)+"_"+datetime.datetime.now().strftime("%Y%m%d%H%M%S")+".log",  # 日志文件名
                            filemode="a")  # 'a'表示追加模式
        
    # 使用print函数，但同时保存到日志
    def print(self,message):
        print(message)
        logging.info(message)
        
    def print_warning(self,message):
        print(message)
        logging.warning(message)
        
    def info(self,message):
        logging.info(message)
        
    def warning(self,message):
        logging.warning(message)