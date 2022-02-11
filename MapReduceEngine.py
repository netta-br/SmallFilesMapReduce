from lithops import Storage
from lithops.storage.utils import CloudObject
from lithops.multiprocessing import Pool

# database
import sqlite3


# -------------------------------------
# Implementation of MapReduce engine.
# -------------------------------------

STORAGE = 'localhost'
PREFIX_SEPARATOR = "/"
TIME_OUT_ERROR = 60  # maximal timeout in seconds
DATABASE = 'mydata.db'
TEMP_RESULTS_TABLE = "temp_results"


class MapReduceEngine:

    def __init__(self):
        MapReduceEngine.create_database()

    @staticmethod
    def create_database():
        # Create SQlite database “temp_results.db”:
        sql_conn = sqlite3.connect(DATABASE)
        c = sql_conn.cursor()
        # create a table “temp_results” with the following schema:
        c.execute(f"CREATE TABLE IF NOT EXISTS {TEMP_RESULTS_TABLE} (key text, value text)")
        sql_conn.commit()
        sql_conn.close()

    @staticmethod
    def sort_and_shuffle(map_result):
        # Connent to the database:
        sql_conn = sqlite3.connect(DATABASE)
        c = sql_conn.cursor()

        # Load content from all mappers into the temp_results table in SQLite:
        for res in map_result:
            for pair in res:
                c.execute(f"INSERT INTO {TEMP_RESULTS_TABLE} VALUES (:key, :value)",
                          {'key': pair[0], 'value': pair[1]})
        sql_conn.commit()

        # Query database to create list of (key, value) which is sorted by key:
        c.execute(f"""
            SELECT
                key,
                GROUP_CONCAT(value, ', ')
            FROM
                {TEMP_RESULTS_TABLE}
            GROUP BY
                key
            ORDER BY
                key
            """)
        records = c.fetchall()
        sql_conn.close()

        return records

    def execute(self, input_data, map_function, reduce_function, params):

        # --------
        #   Map:
        # --------
        storage = Storage()  # load configuration from file
        container, prefix = input_data.split(PREFIX_SEPARATOR)  # extract bucket name and prefix
        objects_attr = storage.list_objects(container, prefix=prefix)

        cobj_list = [CloudObject(STORAGE, container, x['Key']) for x in objects_attr]  # aggregate == False

        args_list = [(x, params['column']) for x in cobj_list]
        with Pool() as pool:
            async_result = pool.starmap_async(map_function, args_list)
            try:
                map_result = async_result.get(timeout=TIME_OUT_ERROR)
            except TimeoutError:
                print('MapReduce Failed')
                return

        # ---------------------
        #   Sort and Shuffle:
        # ---------------------
        records = MapReduceEngine.sort_and_shuffle(map_result)

        # -----------
        #   Reduce:
        # -----------
        with Pool() as pool:
            async_result = pool.starmap_async(reduce_function, records)
        try:
            reduce_result = async_result.get(timeout=TIME_OUT_ERROR)
        except TimeoutError:
            print('MapReduce Failed')
            return

        return reduce_result

