## HOSS(HDFS-based Object Storage System)
--------------
**HOSS** is a highly available, scalable, manageable,distributed object storage system based on [HDFS](http://hadoop.apache.org/, "hadoop").

Object storage is an approach to storage where data is combined with rich metadata in order to preserve information about both the context and the content of the data.

The metadata present in Object Storage gives users the context and content information they need to properly manage and access unstructured data.  They can easily search for data without knowing specific filenames, dates or traditional file designations.  They can also use the metadata to apply policies for routing, retention and deletion as well as automate storage management. 

-------------

##  Build HOSS
``` shell
cd $HOSS_HOME 
ant clean & ant
```

##  How to Use

``` shell
start-hoss.sh 
stop-hoss.sh 
```
##  Contributing
For bugs and feature requests, please create an issue. Pull requests and stars are always welcome.

see details  "http://cddesire.github.io/hoss/"
