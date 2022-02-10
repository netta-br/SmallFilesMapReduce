from lithops.storage.utils import CloudObject
from lithops import Storage


class NewCloudObject(CloudObject):
    def __init__(self, backend, bucket, key):
        super().__init__(backend, bucket, key)
        st = Storage(backend)
        cloud_object = st.get_cloudobject(bucket, key)
        self.size = cloud_object['content-length']  # TODO: copy here the line I wrote myself to get object size
        self.stream = st.get_object(self.bucket, self.key, stream=True)

    def reset_stream(self):
        """Returns io stream of object (file like object)"""
        st = Storage(self.backend)
        self.stream = st.get_object(self.bucket, self.key, stream=True)
        return self.stream


class Partitioner:
    def __init__(self, backend, chunk_size, lines=None):
        self.backend = backend
        self.chunk_size = chunk_size
        self.lines = lines

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

    def large_objects_partition(self, large_object):
        """Receives a large object and partitions it to chunks by lines of bytes"""
        partition = []
        stream = large_object.reset_stream()
        if self.lines:
            block = stream.readlines(self.lines)
            while block != "\0":  # TODO: find out what marks EOF
                partition.append([large_object.key, block])
                block = stream.readlines(self.lines)
            return partition
        else:
            block = stream.read(self.chunk_size)
            while block != "\0":  # TODO: here too
                partition.append([large_object.key, block])
                block = stream.read(self.chunk_size)
            return partition

    def get_cloud_objects(self, bucket, prefix=None):
        """returns an array of cloud objects from the given bucket"""
        st = Storage(self.backend)
        cloud_objects = []
        for key in st.list_keys(bucket, prefix):
            cloud_objects.append(NewCloudObject(self.backend, bucket, key))
        return cloud_objects

    def partition_objects(self, bucket, prefix):
        """Receives a bucket and prefix, returns partition of key\block pairs to mappers"""
        cloud_objects = self.get_cloud_objects(bucket, prefix)
        partitions = []
        small_objects = []

        for cloud_object in cloud_objects:
            if cloud_object.size < self.chunk_size:
                small_objects.append(cloud_object)
            else:
                partitions.append(self.large_objects_partition(cloud_object))

        partitions.append(self.small_objects_partition(small_objects))
        return partitions
