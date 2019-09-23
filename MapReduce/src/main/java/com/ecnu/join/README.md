#### 简要说明

模块采用了两种方式实现 Join 操作，一种在 Map 端进行 Join 操作，一种是在 Reduce 端进行 Join 操作，原始数据以及 Join 之后的结果如下：


1. Persons 表

    | Id_P | LastName | FirstName | Address        | City     |
    | :--- | :------- | :-------- | :------------- | :------- |
    | 1    | Adams    | John      | Oxford Street  | London   |
    | 2    | Bush     | George    | Fifth Avenue   | New York |
    | 3    | Carter   | Thomas    | Changan Street | Beijing  |

2. Orders 表

    | Id_O | OrderNo | Id_P |
    | :--- | :------ | :--- |
    | 1    | 77895   | 3    |
    | 2    | 44678   | 3    |
    | 3    | 22456   | 1    |
    | 4    | 24562   | 1    |
    | 5    | 34764   | 65   |

3. 结果集

    | LastName | FirstName | OrderNo |
    | :------- | :-------- | :------ |
    | Adams    | John      | 22456   |
    | Adams    | John      | 24562   |
    | Carter   | Thomas    | 77895   |
    | Carter   | Thomas    | 44678   |

#### 运行方法

##### Map 端 Join 部分

1. 本地文件系统运行

   修改运行配置，在 Program arguments  中填入如下参数：

   `src/inputs/join/persons.txt src/inputs/join/orders.txt src/outputs/mapJoin`

2. HDFS 文件系统运行

   - 启动 HDFS

     `$HADOOP_HOME/sbin/start-dfs.sh`

   - 上传文件

     `hdfs dfs -put orders.txt /ecnu/join/orders.txt`

     `hdfs dfs -put persons.txt /ecnu/join/persons.txt`

   - 修改运行配置

     `hdfs://localhost:9000/ecnu/join/persons.txt hdfs://localhost:9000/ecnu/join/orders.txt hdfs://localhost:9000/ecnu/join/output`

##### Reduce 端 Join 部分

1. 本地文件系统运行

   修改运行配置

   `src/inputs/join/ src/outputs/reduceJoin persons.txt orders.txt`

2. HDFS 文件系统运行

   - 启动 HDFS

   - 上传文件

   - 修改运行配置

     `hdfs://localhost:9000/ecnu/join hdfs://localhost:9000/ecnu/join/output persons.txt orders.txt`