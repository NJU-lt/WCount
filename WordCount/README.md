[TOC]

## 作业5 mapreduce 之 WordCount + Sort

### 一、设计思路

#### 1. 总思路流程图及总设计思路

##### 1.1总流程图：![未命名文件](/Users/mac/Downloads/未命名文件.jpg)

##### 1.2设计思路：

​	本题主要解决两个问题，第一个是多文件的字符统计，第二个是根据value排序并按照格式要求输出。

- 问题1:由于本题需要在多文件以及单文件分别进行排序输出，所以在字符统计时的map函数中，我们可以在Hadoop官网的的WordCount2.0基础上进行改写，即在map阶段，将文件名加入<key，value>中的key，key写为word------filename，以此识别不同文件中的单词，这样在使用reduce函数进行词频统计时就可以统计在某个文件中单词出现的次数，考虑到本题还有一个需求，就是统计所有文件的前100个高频词，我们可以将其看作一个特殊的单独文件，即“all”文件，即对于每一个符合统计要求的单词，我们将其写为两个<key,value>，一个key时 word------filename，另一个是word------all，这样进行词频统计时就可以同时完成作业要求中的两个任务。
- 问题2: 在解决问题1的基础上，即我们已经有一个临时文件存储所有<word+filename, num>的二进制统计文件，在此基础上，我们要完成第二个任务：排序并且输出前100个高频词汇到不同文件夹中。
  - 排序方法：利用已有的class：InverseMapper.class交换<word+filename, num>和shuffle阶段的自动根据key进行排序的特性，这样传给reduce节点的就是格式为<num,word+filename>且根据num排好序的键值对。
  - 将reduce节点处理完的数据输出不同文件方法：利用value值，即word+filename中的filename以及MultipleOutputs，将传入reduce节点的键值对，根据filename写到不同文件中去。
  - 取每个文件的前100个方法：利用hashmap的key不可重复特性，统计已经写入文件的键值对属于哪个文件，如果filename已经在hashmap中作为key，那么 value+1 ； 如果该filename的value已经100了，就不再写入。如果filename不在hashmap中，那么把这个filename写进去，并且rank设置为0，如果一共有filecount个键并且每个value都是100，break。

#### 2. Job1+Job2流程图：<img src="/Users/mac/Downloads/MapReduce-2.jpg" alt="MapReduce-2" style="zoom:200%;" />

#### 3. Job1流程图及两个Class设计思路：

##### 3.1 流程图

![MapReduce](/Users/mac/Downloads/MapReduce.jpg)

##### 3.2SoloTokenizerMapper 

- setup：读取配置文件，将停词文件中的单词分别读出到patternsToSkip、patternsToStop，供map函数处理value值。

- parseStopFile：将停词文件stop-word-list.txt中的单词写入patternsToStop

- parseSkipFile：将符号文件punctuation.txt中的字符写入patternsToSkip

- map主要：

  - 忽略大小写：```String line = value.toString().toLowerCase()```

  - 满足patternsToSkip的pattern都过滤掉:

    ```java
     for (String pattern : patternsToSkip) 
     		line = line.replaceAll(pattern, " ");
    ```

  - 在停词范围的都过滤掉：
    
    ```java
    if(patternsToStop.contains(nextword)){
    	continue;}          
    ```
    
  - 获取当前文件名：
    
    ```java
    FileSplit fileSplit = (FileSplit)context.getInputSplit();
    String textName = fileSplit.getPath().getName();
    ```
    
  -  转为<key,value>
  
    ```java
    String allword = nextword + "------all";
     if(nextword.length() >= 3) {
        word.set(word + "------" + textName);
        context.write(word, one);
        context.write(new Text(allword),one);      
    ```

##### 3.3IntSumReducer.Class

- 和原WordCountv2.0没有变化

##### 3.4main函数设置：

```java
        solowcjob.setMapperClass(SoloTokenizerMapper.class);
        solowcjob.setCombinerClass(IntSumReducer.class);
        solowcjob.setReducerClass(IntSumReducer.class);
        solowcjob.setOutputKeyClass(Text.class);
        solowcjob.setOutputValueClass(IntWritable.class);
        solowcjob.setOutputFormatClass(SequenceFileOutputFormat.class);
```

#### 4. Job2的Class设计思路：

- InverseMapper.class
  - 已有class

- SoloSortReducer.Class

  - 设置多文件输出：

    ```java
            private MultipleOutputs<Text, NullWritable> mos=null;
            public void setup(Context context) throws IOException
            {
                mos=new MultipleOutputs<Text,NullWritable>(context);
            }
            public void cleanup(Context context)
            {
                try {
                    mos.close();
                } catch (IOException | InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
    Configuration conf = context.getConfiguration();
    int filenum = Integer.parseInt(conf.get("fileCount")); //读取配置文件中的需要输出的文件的个数。
    mos.write(filename,new Text(sum +": "+word+","+key),NullWritable.get());
    ```
    
  - 设置write条件：```Map<String, Integer> filemap = new HashMap<String,Integer>();```
  
    //                如果该文件名已经在filemap中作为key，那么 原来的value+1 ；
  
    //                如果该文件名的value已经100了，就不再写入，跳过；
  
    //                如果该文件名不在filemap的key中，那么把这个filename写进去，并且value设置为0；
  
    //                如果一共有filecount（从配置文件中读入）个键并且每个键的value都是100，那么break出去。
  
- main函数设置：

  ```java
          FileInputFormat.addInputPath(solosortjob, tempDir2);
          solosortjob.setInputFormatClass(SequenceFileInputFormat.class);
          solosortjob.setMapperClass(InverseMapper.class);
          solosortjob.setReducerClass(SoloSortReducer.class);
          FileOutputFormat.setOutputPath(solosortjob, new Path(otherArgs.get(1)));
          for (String filename : allfile) {
              MultipleOutputs.addNamedOutput(solosortjob, filename, TextOutputFormat.class,Text.class, NullWritable.class);
          }
  ```

### 二、运行截图

#### 1. 伪分布式运行截图

##### 1.1 8088端口WEB截图

<img src="/Users/mac/Desktop/h5/2021-10-30 00-02-30屏幕截图.png" alt="2021-10-30 00-02-30屏幕截图" style="zoom:50%;" />

##### 1.2 9870端口运行截图

<img src="/Users/mac/Desktop/h5/2021-10-30 00-04-31屏幕截图.png" alt="2021-10-30 00-04-31屏幕截图" style="zoom:50%;" /><img src="/Users/mac/Desktop/h5/2021-10-30 00-03-44屏幕截图.png" alt="2021-10-30 00-03-44屏幕截图" style="zoom:50%;" />



<img src="/Users/mac/Desktop/h5/2021-10-30 00-03-28屏幕截图.png" alt="2021-10-30 00-03-28屏幕截图" style="zoom:50%;" />

##### 1.3 终端运行截图

<img src="/Users/mac/Desktop/h5/2021-10-30 00-02-05屏幕截图.png" alt="2021-10-30 00-02-05屏幕截图" style="zoom:50%;" />

#### 2. docker集群运行截图

##### 2.1 8088端口WEB截图![2021-10-30 01-22-12屏幕截图](/Users/mac/Desktop/h5/2021-10-30 00-03-44屏幕截图.png)

##### 2.2 9870端口节点运行状态

![2021-10-30 01-21-41屏幕截图](/Users/mac/Desktop/h5/2021-10-30 01-21-41屏幕截图.png)



![2021-10-30 01-21-20屏幕截图](/Users/mac/Desktop/h5/2021-10-30 01-21-20屏幕截图.png)

![2021-10-30 01-20-12屏幕截图](/Users/mac/Desktop/h5/2021-10-30 01-20-12屏幕截图.png)
##### 2.3 终端运行成功截图

<img src="/Users/mac/Desktop/h5/2021-10-30 01-19-28屏幕截图.png" alt="2021-10-30 01-19-28屏幕截图" style="zoom:50%;" />

<img src="/Users/mac/Desktop/h5/2021-10-30 01-19-13屏幕截图.png" alt="2021-10-30 01-19-13屏幕截图" style="zoom:50%;" />

##### 2.4 传入文件及运行截图

​			<img src="/Users/mac/Desktop/h5/2021-10-30 01-13-46屏幕截图.png" alt="2021-10-30 01-13-46屏幕截图" style="zoom:33%;" />

<img src="/Users/mac/Desktop/h5/2021-10-30 01-17-23屏幕截图.png" alt="2021-10-30 01-17-23屏幕截图" style="zoom:50%;" />

#### 3. 本地文件结构

<img src="/Users/mac/Desktop/h5/2021-10-29 19-52-23屏幕截图.png" alt="2021-10-29 19-52-23屏幕截图" style="zoom:50%;" />



### 三、所遇问题

1. 本地运行存在以下问题

   <img src="/Users/mac/Library/Application Support/typora-user-images/截屏2021-10-30 下午2.25.51.png" alt="截屏2021-10-30 下午2.25.51" style="zoom: 33%;" />

    原因：类库版本过低

    解决方法：修改pom.xml文件

2. 原想使用File方法读取目录下所有文件名于allfile文件中，并且计算文件个数写入配置文件中供reduce函数使用，以及可以实现自动MultipleOutputs操作，但是File方法只能在本地运行，无法读取HDFS文件系统中文件，在伪分布式中运行错误，于是舍弃该方法，变为通过该方法读取所有文件名，写入allfile（list）中。

   for (String filename : allfile) {
               MultipleOutputs.addNamedOutput(solosortjob, filename, TextOutputFormat.class,Text.class, NullWritable.class);}

### 四、总结分析不足之处

1. 性能：
   - 题目仅要求获得前100高频词汇，但是由于我使用InverseMapper.class类，得到所有单词的排序，有很多排序并不需要用到。其次对于后续写入文件时的操作是，跳过已满100的单词，即许多单词是不需要的。
2. 扩展性：
   - 由于无法在main函数中一次性获得所有文件名，所以不得不将所有文件名写入list中给MultipleOutputs.addNamedOutput中使用，因此传入别的文件，本代码需要修改才能运行。
   - 只能传入两个停词文件，且按照一定顺序传入，因为stop文件和punc文件处理方式不同。
   - 只能输出前一百的高频词汇，无法由用户自行设定。
   - 没有 -skip参数，也无法正常运行。

