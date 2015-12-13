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

##  Example
- Object Put Example

``` java
public static void write(String objName, String content) throws IOException {
		HosObject hosObject = new HosObject(objName);
		FSDataOutputStream out = hosObject.getWriter();
		if (null != out){
			out.writeBytes(content);
			hosObject.close();
		}
	}
```

- Object Put Example

``` java
public static void write(String objName, String content) throws IOException {
		HosObject hosObject = new HosObject(objName);
		FSDataOutputStream out = hosObject.getWriter();
		if (null != out){
			out.writeBytes(content);
			hosObject.close();
		}
	}
```

- Object Get Example

``` java
public static void read(String objName) throws IOException {
		HosObject hosObject = new HosObject(objName);
		FSDataInputStream in = hosObject.getReader();
		byte[] ioBuffer = new byte[1024];  
        int readLen = in.read(ioBuffer); 
        System.out.print(objName + " contents :");
        while(readLen!=-1)  
        {  
            System.out.print(new String(ioBuffer, 0, readLen));  
            readLen = in.read(ioBuffer);  
        }  
		hosObject.close();
	}
```

- Object Delete Example

``` java
public static void delete(String objName) throws IOException {
		HosObject hosObject = new HosObject(objName);
		hosObject.deleteObject();
	}
```

- List Objects Example

``` java
public static void list() {
		ClientProtocol client  = HosClient.client();
		Text namelist = client.listObjects();
		String[] names = namelist.toString().split("\t");
		System.out.println("object name # position");
		for(String name: names) {
			System.out.println(name);
		}
	}
```

- Top k Hot Object Example

``` java
public static void topHot(int top) {
		Text namelist = client.topHotObject(top);
		String[] names = namelist.toString().split("\t");
		System.out.println("object name # hotness");
		for(String name: names) {
			System.out.println(name);
		}
	}
```


##  Contributing
For bugs and feature requests, please create an issue. Pull requests and stars are always welcome.

see details  "http://cddesire.github.io/hoss/"
