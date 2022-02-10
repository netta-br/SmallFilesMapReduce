import lithops
import pandas as pd
import sqlite3
import io

# from Partitioner import Partitioner
# TODO: Integrate Partitioner: change map processes to receive partition of key\block pairs instead of single key

LOG_LEVEL = 'INFO'  # 'DEBUG'
ENCODING = 'UTF-8'
PREFIX_SEPARATOR = "/"
DATABASE = 'mydata.db'
TEMP_RESULTS_TABLE = "temp_results"
STORAGE = 'localhost'  # 'azure_storage'


class SFMapReduceEngine:
    @staticmethod
    def get_keys_from_container(input_data, prefix=None):
        """Returns a list of the keys in the input data container/prefix"""
        st = lithops.Storage(backend=STORAGE)
        return st.list_keys(input_data, prefix=prefix)

    @staticmethod
    def map_keys_functions(container, keys, map_function):
        """Runs a thread map key function for each key in input data, returns array of threads"""
        execs = []
        st = lithops.Storage(backend=STORAGE)
        for key in keys:
            df = pd.read_csv(io.StringIO(st.get_object(container, key).decode(ENCODING)))
            fexec = lithops.FunctionExecutor(log_level=LOG_LEVEL)
            fexec.call_async(map_function, (key, df))
            execs.append(fexec)
        return execs
    
    @staticmethod
    def query_key_values(table, sql_conn):
        """returns result of query which groups by key in table and concats all values"""

        query = f"SELECT key, GROUP_CONCAT(value) FROM {table} GROUP BY key"
        cur = sql_conn.cursor()
        cur.execute(query)
        results = cur.fetchall()
        return results
    
    @staticmethod
    def reduce_keys_values_functions(keys_values, reduce_function):
        """Runs a thread reduce keys values function for each key in input data, returns array of threads"""
        execs = []
        for i in range(len(keys_values)):
            (key, values) = keys_values[i]
            fexec = lithops.FunctionExecutor(log_level=LOG_LEVEL)
            fexec.call_async(reduce_function, (key, values))
            execs.append(fexec)
        return execs
    
    def execute(self, input_data, map_function, reduce_function):
        """applies map function on keys in input data and then reduces using the reduce function"""
        
        # maps each key in a separate thread to a temp file
        container, prefix = input_data.split(PREFIX_SEPARATOR)
        keys = self.get_keys_from_container(container, prefix=prefix)
        map_execs = self.map_keys_functions(container, keys, map_function)
        
        # get mappers results and convert to dataframes
        dfs = []
        for fexec in map_execs:
            map_results = "key,value\n" + ("\n").join([(",").join(key_value) for key_value in fexec.get_result()])
            dfs.append(pd.read_csv(io.StringIO(map_results)))
            
        # load results to table and query all keys and values
        sql_conn = sqlite3.connect(DATABASE)
        final_df = pd.concat(dfs)
        final_df.to_sql(TEMP_RESULTS_TABLE, sql_conn, if_exists="append", index=False)
        temp_results = self.query_key_values(TEMP_RESULTS_TABLE, sql_conn)
        
        # reduce using the reduce function to the final result
        final_result = []
        reduce_execs = self.reduce_keys_values_functions(temp_results, reduce_function)
        
        for fexec in reduce_execs:
            final_result.append(fexec.get_result())
        return final_result

