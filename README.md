# map-reduce-project
some map-reduce script for calculating log informations.

## Usage
1、编写代码；
修改对应的Stat_pv_count.java文件里的map方法:
``` java
public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			if (value.toString().indexOf("main_feed") > -1) {
				word.set("pv");
				context.write(word, one);
			}
		}
```

2、对该代码进行打包：
```
mvn clean package 
```
or
```
mvn package
```

3、同步到公司Hadoop集群的通道机上，启动map-reduce job，命令：
```
hdfs jar yourPackageJarFile LogPath OutputDir
```
## Example
1、将本项目代码clone到服务器上，如目录为：/data0/map-reduce-example

2、查看是否安装了mvn，以及对应的java版本：
``` 
which mvn
java -version
```
由于本人所用服务器上环境已部署好，因此不出意外，上面无需考虑。

3、对本项目代码进行打包：
```
mvn clean package
```
此时生成target目录，该目录下有个mapreduce-0.0.1-jar-with-dependencies.jar。

4、将此jar包rsync到HDFS通道机自己的目录下，执行：
```
hadoop jar mapreduce-0.0.1-jar-with-dependencies.jar path/to/your/log/2016/02/03/01/00  output
```
其中，path/to/your/log/2016/02/03/01/00为HDFS上日志目录，
output目录输出计算结果，在当前用户下的HDFS用户根目录下能够找到。
如果成功，显示如下：
```
total 4
-rw-r--r-- 1 chaoqiang chaoqiang 5 Feb  4 12:07 part-r-00000
-rw-r--r-- 1 chaoqiang chaoqiang 0 Feb  4 12:07 _SUCCESS
```

5、提取计算结果：
```
hdfs dfs -cat /user/yourName/output/part-r-00000
```
