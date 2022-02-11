from lithops.storage.utils import CloudObject
from lithops import Storage


class NewCloudObject:

    def __init__(self, backend, bucket, key, size):
        self.size = size
        self.cobj = CloudObject(backend, bucket, key)


class Partitioner:
    def __init__(self, backend, chunk_size):
        self.backend = backend
        self.chunk_size = chunk_size

    def partition_objects(self, bucket, prefix):
        """Receives a bucket and prefix, returns partition of key\block pairs to mappers"""
        cloud_objects = self.get_cloud_objects(bucket, prefix)
        partitions = []
        small_objects = []

        for cloud_object in cloud_objects:
            if cloud_object.size < self.chunk_size:
                small_objects.append(cloud_object)
            else:
                partitions.append(cloud_object)

        partitions.append(self.small_objects_partition(small_objects))
        return partitions

    def get_cloud_objects(self, bucket, prefix=None):
        """returns an array of cloud objects from the given bucket"""
        st = Storage(self.backend)
        cloud_objects = []
        for obj_attr in st.list_objects(bucket, prefix):
            key = obj_attr['Key']
            size = obj_attr['Size']
            cloud_objects.append(NewCloudObject(self.backend, bucket, key, size))
        return cloud_objects

    def small_objects_partition(self, small_objects):
        # TODO: Each mapper will receive at most two times the chunk size,
        #  if the chunk size needs to be more strict then consider an efficient algorithm to partition the file array
        partition = []
        cur = []
        batch_size = 0
        for cloud_object in small_objects:
            if batch_size < self.chunk_size:
                cur.append(cloud_object)
                batch_size += cloud_object.size
            else:
                partition.append(cur)
                cur = [cloud_object]
                batch_size = cloud_object.size




