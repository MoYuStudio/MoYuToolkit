# coding=utf-8

import json
import os

class JsonArchiver:
    def __init__(self, filename="data.json"):
        self.filename = filename
        self.data = {}
        self.load_data()

    def load_data(self):
        """加载JSON数据文件，如果文件不存在则创建一个空字典"""
        if os.path.exists(self.filename):
            with open(self.filename, 'r', encoding='utf-8') as f:
                self.data = json.load(f)
        else:
            self.data = {}

    def save_data(self):
        """保存数据到文件，并重置数据更改标志"""
        with open(self.filename, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, ensure_ascii=False, indent=4)

# 使用示例
if __name__ == "__main__":
    pass