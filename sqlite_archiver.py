# coding=utf-8

import json
import sqlite3

class SqliteArchiver:
    def __init__(self, path="data.db"):
        self.conn = sqlite3.connect(path)
        self.cursor = self.conn.cursor()
    
    def get_primary_key(self, table):
        """获取表主键"""
        self.cursor.execute(f"PRAGMA table_info({table})")
        info = self.cursor.fetchall()
        for col in info:
            if col[5] == 1:
                return col[1]
        raise ValueError(f"表{table}未找到主键")
    
    
    def convert_type(self, value):
        """尝试将JSON字符串转换为Python数据类型"""
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value
    
    def load_table(self, table=""):
        """读取表 需含主键"""
        primary_key = self.get_primary_key(table)

        try:
            self.cursor.execute(f"SELECT * FROM {table}")
            rows = self.cursor.fetchall()
            
            data = {}
            for row in rows:
                key_index = next((i for i, col in enumerate(self.cursor.description) if col[0] == primary_key), None)
                if key_index is None:
                    raise ValueError(f"列 {primary_key} 不存在于表 {table} 中")
                key = row[key_index]
                values = {col[0]: row[idx] for idx, col in enumerate(self.cursor.description) if col[0] != primary_key}
                data[key] = values

            return data

        except Exception as erro:
            print(f"从{table}加载数据时出错：{erro}")
            return None
        
    def load_column(self, table="", column=""):
        """读取列"""
        try:
            self.cursor.execute(f"SELECT {column} FROM {table}")
            rows = self.cursor.fetchall()
            data = [self.convert_type(row[0]) for row in rows]
            
            return data
        
        except Exception as erro:
            print(f"从{table}的{column}列加载数据时出错：{erro}")
            return None
        
    def load_column_with_key(self, table="", column=""):
        """读取主键列与信息列"""
        try:
            # 获取表的主键
            primary_key = self.get_primary_key(table)
            
            # 检查表名和列名是否提供
            if not table or not column:
                raise ValueError("表名和列名不能为空")

            # 构建查询语句
            query = f"SELECT {column}, {primary_key} FROM {table}"
            
            # 执行查询
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            
            # 如果查询结果为空，返回空列表
            if not rows:
                return []
            
            # 格式化查询结果为字典列表
            data = []
            for row in rows:
                entry = {
                    primary_key: self.convert_type(row[1]),
                    column: self.convert_type(row[0])
                }
                data.append(entry)
            
            return data
        
        except Exception as erro:
            print(f"从{table}读取主键列与{column}列时出错：{erro}")
            return None

    def load_row(self, table="", key=""):
        """读取行 需含主键"""
        primary_key = self.get_primary_key(table)
        
        try:
            self.cursor.execute(f"SELECT * FROM {table} WHERE {primary_key} = ?", (key,))
            row = self.cursor.fetchone()  # 获取单条记录
            if row is None:
                raise ValueError(f"未找到主键为 {key} 的记录")
            
            data = row
        
            return data
        
        except Exception as erro:
            print(f"从{table}的{primary_key}列加载{key}行的数据时出错：{erro}")
            return None
    
    def load_grid(self, table="", column="", key=""):
        """读取格"""
        try:
            # 获取表的主键
            primary_key = self.get_primary_key(table)
            
            # 检查提供的主键值是否与表的主键匹配
            if key is None or column is None:
                raise ValueError("主键值和列名不能为空")
            
            # 执行查询
            self.cursor.execute(f"SELECT {column} FROM {table} WHERE {primary_key} = ?", (key,))
            result = self.cursor.fetchone()
            
            # 如果查询结果不为空，返回该格的数据，否则返回None
            if result:
                return self.convert_type(result[0])
            else:
                return None
        
        except Exception as erro:
            print(f"从{table}获取{column}列在主键{primary_key}={key}的数据时出错：{erro}")
            return None
    
    def select_column(self):
        """搜索列 非必要"""
        pass
    
    def select_row(self, table="", column="", value=""):
        """搜索行"""
        if not table or not column or not value:
            raise ValueError("表名、列名和值不能为空")

        try:
            self.cursor.execute(f"SELECT * FROM {table} WHERE {column} LIKE ?", ('%' + value + '%',))
            rows = self.cursor.fetchall()
            if not rows:
                    return {}
                
                # 获取列名和主键
            column_names = [desc[0] for desc in self.cursor.description]
            primary_key = self.get_primary_key(table)

            # 格式化查询结果为字典
            data = {}
            for row in rows:
                key = row[column_names.index(primary_key)]
                values = {col: self.convert_type(row[idx]) for idx, col in enumerate(column_names) if col != primary_key}
                data[key] = values

            return data

        except Exception as erro:
            print(f"从{table}搜索{column}列包含{value}的数据时出错：{erro}")
            return {}
        
    def select_grid(self, table="", column="", key=""):
        """搜索格"""
        try:
            # 获取表的主键
            primary_key = self.get_primary_key(table)
            
            # 检查提供的主键值是否与表的主键匹配
            if key is None or column is None:
                raise ValueError("主键值和列名不能为空")
            
            # 执行查询
            self.cursor.execute(f"SELECT {column} FROM {table} WHERE {primary_key} = ?", (key,))
            result = self.cursor.fetchone()
            
            # 如果查询结果不为空，返回该格的数据，否则返回None
            if result:
                return self.convert_type(result[0])
            else:
                return None
        
        except Exception as erro:
            print(f"从{table}获取{column}列在主键{primary_key}={key}的数据时出错：{erro}")
            return None
    
    def add_grid(self):
        """增加格 增加行"""
        pass
    
    def add_row(self, table="", columns_and_values={}):
        """增加行"""
        try:
            # 检查是否提供了表名和列值对
            if not table or not columns_and_values:
                raise ValueError("表名和列值对不能为空")

            # 构建插入语句
            columns = ', '.join(columns_and_values.keys())
            placeholders = ', '.join(['?' for _ in columns_and_values])
            sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

            # 执行插入操作
            self.cursor.execute(sql, list(columns_and_values.values()))
            self.conn.commit()
            
        except Exception as erro:
            # 发生错误时回滚事务
            self.conn.rollback()
            print(f"插入数据时出错：{erro}")
    
    def set_grid(self, table="", column="", key="", new_value=""):
        """修改格"""
        try:
            primary_key = self.get_primary_key(table)
            self.cursor.execute(f"UPDATE {table} SET {column} = ? WHERE {primary_key} = ?", (json.dumps(new_value, ensure_ascii=False), key))
            self.conn.commit()
        
        except Exception as erro:
            # 发生错误时回滚事务
            self.conn.rollback()
            print(f"修改数据时出错：{erro}")
    
    def remove_row(self, table="", column="", key=""):
        """删除行"""
        try:
            self.cursor.execute(f'DELETE FROM {table} WHERE {column} = ?', (key,))
            self.conn.commit()
            
        except Exception as erro:
            # 发生错误时回滚事务
            self.conn.rollback()
            print(f"修改数据时出错：{erro}")
    
    def close(self):
        """关闭连接"""
        self.cursor.close()
        self.conn.close()
        
# 使用示例
if __name__ == "__main__":
    archiver = SqliteArchiver()
    print(archiver.select_grid(table="client", column="cost", key="刘海宇"))
    archiver.close()