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

    def __init__(self, chunk_size):
        MapReduceEngine.create_database()
        self.chunk_size = chunk_size

    @staticmethod
    def create_database():
        # Create SQlite database “temp_results.db”:
        sql_conn = sqlite3.connect(DATABASE)
        c = sql_conn.cursor()
        # create a table “temp_results” with the following schema:
        c.execute(f"CREATE TABLE IF NOT EXISTS {TEMP_RESULTS_TABLE} (key text, value text)")
        sql_conn.commit()
        sql_conn.close()

    def partition_data(self, container, objects_attr):
        partitions = []
        small_objects = []

        for object_attr in objects_attr:
            if object_attr['Size'] < self.chunk_size:
                small_objects.append(object_attr)
            else:
                partitions.append(CloudObject(STORAGE, container, object_attr['Key']))

        partitions.extend(self.small_objects_partition(container, small_objects))
        return partitions

    def small_objects_partition(self, container, small_objects):
        # TODO: Each mapper will receive at most two times the chunk size,
        #  if the chunk size needs to be more strict then consider an efficient algorithm to partition the file array
        partitions = []
        cur = []
        batch_size = 0
        for object_attr in small_objects:
            cloud_object = CloudObject(STORAGE, container, object_attr['Key'])
            if batch_size < self.chunk_size:
                cur.append(cloud_object)
                batch_size += object_attr['Size']
            else:
                partitions.append(cur)
                cur = [cloud_object]
                batch_size = object_attr['Size']
        if cur:
            partitions.append(cur)
        return partitions

    # Wrapper function for:
    #    (1) Processing mapfunction with multiple keys.
    #    (2) Aggregate result.
    @staticmethod
    def map_decorator(func):
        def wrapper(*args):
            keys = args[0]
            value = args[1]
            agg_result = []
            for key in keys:
                agg_result.extend(func(key, value))
            return agg_result
        return wrapper

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

        if params['aggregate']:
            cobj_list = self.partition_data(container, objects_attr)
            map_function = MapReduceEngine.map_decorator(map_function)  # Wrap function to process multiple keys
        else:
            cobj_list = [CloudObject(STORAGE, container, x['Key']) for x in objects_attr]

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

