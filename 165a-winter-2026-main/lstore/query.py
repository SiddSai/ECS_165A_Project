from lstore.table import Table, Record
from lstore.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        try:
            ok = self.table.delete(primary_key)
            return True if ok else False
        except Exception as e:
            print(f"Delete failed: {e}")
            return False
        pass
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        schema_encoding = '0' * self.table.num_columns
        try:
            # check if the number of input equals to the number of columns
            if len(columns) != self.table.num_columns:
                return False
            # check if the input is empty
            for v in columns:
                if v is None:
                   return False
            ok = self.table.insert(*columns)
            if ok:
                return True
            else:
                return False
        except Exception as e:
            print(f"Insert failed: {e}")
            return False

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        try:
            # 1) 查找 RID 列表
            # 需确认index 对象的定位方法名及参数
            rids = self.table.index.locate(search_key_index, search_key)

            results = []
            for rid in rids:
                # 2) 读取完整行数据
                # 需确认table 对象的读取方法名
                full_cols = self.table.read(rid)

                if full_cols is None:
                    continue
                num_cols = self.table.num_columns
                projected = [None] * num_cols
                for i in range(num_cols):
                    if projected_columns_index[i] == 1:
                        projected[i] = full_cols[i]
                # 需确认接口 Record 类的构造函数参数顺序
                rec = Record(rid, search_key, projected)
                results.append(rec)
            return results
        except Exception:
            return False


    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        pass

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        try:
            # 1) 检查输入长度
            # 需确认table 记录总列数的变量名
            if len(columns) != self.table.num_columns:
                return False

            # 2) 执行更新
            # 需确认接口: table.update 接收的是 primary_key 还是 rid？
            # 如果你们的 table.update 直接支持主键，则如下：
            ok = self.table.update(primary_key, columns)

            # 3) 返回结果
            return True if ok else False

        except Exception:
            return False

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        try:
            total = 0
            found_any = False

            # 需确认table 记录总列数的变量名
            projection = [0] * self.table.num_columns
            projection[aggregate_column_index] = 1

            for key in range(start_range, end_range + 1):
                # 需确认table 记录主键列索引的变量名 (例如 self.table.key)
                recs = self.select(key, self.table.key, projection)

                if recs:
                    found_any = True

                    # 需确认Record 对象获取数据列的方式
                    value = recs[0].columns[aggregate_column_index]

                    if value is not None:
                        total += value

            if found_any:
                return total
            else:
                return False

        except Exception as e:
            print(f"Sum operation failed: {e}")
            return False
    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        pass

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
