# SmallFilesMapReduce
## Abstract
In this paper, the problem of small files and possible solutions is discussed in the framework of HDFS and object storage. The existing solutions and their limitations are discussed. An algorithm to detect the need of compacting data files and perform the aggregation is proposed, and a prototype code of a SmallFilesMapReduceEngine is reviewed.

## Motivation and Background
MapReduce is a software framework that allows processing vast amounts of data in parallel on large clusters in a fast, reliable and fault-tolerant manner. It operates on key value pairs and produces pairs of different types. The mapper function converts key/value pairs into intermediate records. The number of mappers is based on the size of the input, usually each mapper is assigned a fixed size chunck\block of data. A reducer reduces a set of intermediate values which share a key to a smaller set of values. An intermediate shuffle step is necessary between the mappers and the reducers, in order to fetch the partition of the mappers output that is relevant to each reducer. Increasing the number of reduces increases the framework overhead, but increases load balancing and lowers the cost of failures [[5](#bibliography)].</br>
The input data, intermediate records and output of the MapReduce is usually persisted in Hadoop Distributed File System (HDFS). It is highly fault-tolerant and is designed to handle large data sets, typically gigabyte to terabyte in size. HDFS has master\slave architecture, where each cluster consists of a single NameNod (file system manager\master) and multiple DataNodes, one per node, that store blocks of data and serve read\write requests from the client, and create\delete\replication instructions from the NameNode. The NameNode keeps all the files metadata and monitors the DataNodes health (Heartbeat). Files in HDFS are write-once, they are stored as a sequence of blocks of a fixed size and are replicated for fault tolerance.[[6](#bibliography)]</br>
Hadoop implementation of MapReduce on HDFS relies on bringing the computing resource to the data, as opposed to moving the data from storage to computation cluster via the network. The disadvantage in this architecture is that the storage and compute resources can only be scaled together and not apart. When network bandwidth is not a bottleneck, MapReduce can be implemented on an object storage system. Object storage is a scalable storage solution for unstructured data that is based HTTP\REST API. Object storage allows to scale the computational resources and to avoid increasing storage cost.[[7](#bibliography)]

## Small Files Problem
When it comes to files that are significantly smaller than the HDFS block size, which is 64MB by default, the system is not efficient. As there is only one NameNode in Hadoop, the files metadata (which weighs more if there are more replications) is centralized and has to be kept in memory. This poses a bottleneck for metadata requests, and a larger number of files and replications results in lower performance of metadata related requests. Another problem is the amount of block reports from DataNodes that make each Heartbeat from DataNode to NameNode a significant event in terms of cluster overhead. The small files is also an issue for MapReduce programs where a lot of processes are created with additional overhead to the program.[[3](#bibliography)]
## Our Approach
We purpose two possible solutions, each a pluggable framework based on current Hadoop MapReduce.
1. To create a storage API that nips the small files problem in the bud and caches small files from client in memory to create aggregated larger files in storage, such as SequenceFiles. This solution is not applicable when the data that needs processing is already in storage, which is not something that can always be controlled by the user.
2. To add a partitioner step before the mappers that sends multiple keys and files for each mapper process. This solution is effective with object storage where the small files problem is reduced to the overhead created by the number of mappers (as opposed to HDFS storage where it is also a problem of efficiency in storing the data).</br></br>
Each method solves the small files problem in MapReduce in terms of processes overhead. The first method is more constrained since it requires access to the data collection process, however it also addresses the HDFS storage small files problems. The first method can be more effective in systems where the bottleneck is network, and the second method is more cost-effective otherwise.

## Prototype Design

## Next Steps

## Conclusion

## Bibliography
1. [SFMapReduce: An Optimized MapReduce Framework for Small Files](https://www.cs.fsu.edu/~yuw/pubs/2015-NAS-Yu.pdf)
2. [Impact of Small Files on Hadoop Performance: Literature Survey and Open Points](https://mjeer.journals.ekb.eg/article_62728_c818f3f951476c6005647f9ba7364efd.pdf)
3. [An optimized approach for storing and accessing small files on cloud storage](https://www.cs.bham.ac.uk/~rza/pub/cloudStorage.pdf)
4. [Cloudera Blog - The Small Files Problem](https://blog.cloudera.com/the-small-files-problem/)
5. [Hadoop Documentation - MapReduce Tutorial](https://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html)
6. [Hadoopp Documentation - HDFS Design](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html#Introduction)
7. [IBM - The Future of Object Storage: From a Data Dump to a Data Lake](https://www.ibm.com/cloud/blog/the-future-of-object-storage-from-a-data-dump-to-a-data-lake)