#### 运行方法

1. 本地文件系统运行

   修改运行配置，在 Program arguments  中填入如下参数：

   `src/inputs/kmeans/data.txt src/inputs/kmeans/centers.txt src/outputs/kmeans/`

   三个参数依次表示：

   - 待聚类数据
   - 给定的中心数据
   - 输出路径

2. HDFS 运行

   - 启动 HDFS 

     `$HADOOP_HOME/sbin/start-dfs.sh`

   - 上传文件
   
     `hdfs dfs -put data.txt /ecnu/kmeans/data.txt`
   
     `hdfs dfs -put centers.txt /ecnu/kmeans/centers.txt`
   
   - 修改运行配置
   
     `hdfs://localhost:9000/ecnu/kmeans/data.txt hdfs://localhost:9000/ecnu/kmeans/centers.txt hdfs://localhost:9000/ecnu/kmeans/output`



