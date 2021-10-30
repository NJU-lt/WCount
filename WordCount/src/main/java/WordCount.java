import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.util.regex.Pattern;
import java.io.File;

public class WordCount {
    // 日志组名MapCounters，日志名INPUT_WORDS
    static enum MapCounters {
        INPUT_WORDS
    }

    static enum ReduceCounters {
        OUTPUT_WORDS
    }

     static enum CountersEnum { INPUT_WORDS,OUTPUT_WORDS }
//     日志组名CountersEnum，日志名INPUT_WORDS和OUTPUT_WORDS


    /**
     * Reducer没什么特别的升级特性
     *
     * @author Administrator
     */
    public static class IntSumReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
            Counter counter = context.getCounter(
                    ReduceCounters.class.getName(),
                    ReduceCounters.OUTPUT_WORDS.toString());
            counter.increment(1);
        }
    }

    private static class IntWritableDecreasingComparator extends IntWritable.Comparator {

        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("ARGS:" + args[0]+args[1]+args[2]+args[3]);
        Path tempDir = new Path("wordcount-temp-output");
        Configuration conf = new Configuration();
//        File f = new File(path);//获取路径
//        if (!f.exists()) {
//            System.out.println(path + " not exists");//不存在就输出
//            return;
//        }
//        File fa[] = f.listFiles();//用数组接收
//        ArrayList<String> allfile = new ArrayList<String>();
//        for (int i = 0; i < fa.length; i++) {//循环遍历
//            String filename = fa[i].getName();
//            filename = filename.replaceAll(".txt", "");
//            filename = filename.replaceAll("-", "");
//            allfile.add(filename);
//        }
//        allfile.add("all");
//        System.out.println(allfile);

        int fileCount = 41;
        conf.set("fileCount",String.valueOf(fileCount));
        System.out.println(conf.get("fileCount"));
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();


        Job solowcjob = Job.getInstance(conf,"solo wordcount");
        solowcjob.setJarByClass(WordCount.class);
        solowcjob.setMapperClass(SoloTokenizerMapper.class);
        solowcjob.setCombinerClass(IntSumReducer.class);
        solowcjob.setReducerClass(IntSumReducer.class);
        solowcjob.setOutputKeyClass(Text.class);
        solowcjob.setOutputValueClass(IntWritable.class);
        solowcjob.setOutputFormatClass(SequenceFileOutputFormat.class);
        List<String> otherArgs = new ArrayList<String>(); // 除了 -skip 以外的其它参数以外的其它参数
        for (int i = 0; i < remainingArgs.length; ++i) {

            if ("-skip".equals(remainingArgs[i])) {
                solowcjob.addCacheFile(new Path(remainingArgs[++i]).toUri()); 
                solowcjob.addCacheFile(new Path(remainingArgs[++i]).toUri());
                solowcjob.getConfiguration().setBoolean("wordcount.skip.patterns",
                        true); // 这里设置的wordcount.skip.patterns属性，在mapper中使用
            } else {
                otherArgs.add(remainingArgs[i]); // 将除了 -skip
                // 以外的其它参数加入otherArgs中
            }
        }
        FileInputFormat.addInputPath(solowcjob, new Path(otherArgs.get(0)));// otherArgs的第一个参数是输入路径
        Path tempDir2 = new Path("Solo-wordcount-temp-output");
        FileOutputFormat.setOutputPath(solowcjob,tempDir2);
        solowcjob.waitForCompletion(true);



        Job solosortjob = new Job(conf, "sort");
        solosortjob.setJarByClass(WordCount.class);
        FileInputFormat.addInputPath(solosortjob, tempDir2);
        solosortjob.setInputFormatClass(SequenceFileInputFormat.class);
        solosortjob.setMapperClass(InverseMapper.class);
        solosortjob.setReducerClass(SoloSortReducer.class);
        FileOutputFormat.setOutputPath(solosortjob, new Path(otherArgs.get(1)));
        solosortjob.setOutputKeyClass(IntWritable.class);
        solosortjob.setOutputValueClass(Text.class);
        solosortjob.setSortComparatorClass(IntWritableDecreasingComparator.class);
        ArrayList<String> allfile = new ArrayList<String>(Arrays.asList(
           "shakespearecoriolanus24",     "shakespearehamlet25", "shakespearejulius26","shakespearecomedy7",  "shakespearecymbeline17", "shakespearefirst51", "shakespeareking45", "shakespearelife54",
                 "shakespeareloves8", "shakespearemacbeth46", "shakespearemeasure13", "shakespearemerchant5", "shakespearemerry15",
                "shakespearemidsummer16", "shakespearemuch3", "shakespeareothello47", "shakespearealls11", "shakespeareantony23", "shakespeareas12","shakespearepericles21",
                "shakespearerape61", "shakespeareromeo48", "shakespearelife55", "shakespearelife56", "shakespearelovers62","shakespearesecond52", "shakespearesonnets59",
                "shakespearesonnets", "shakespearetaming2", "shakespearetempest4", "shakespearethird53", "shakespearetwo18", "shakespearevenus60","shakespearetimon49", "shakespearetitus50", "shakespearetragedy57", "shakespearetragedy58", "shakespearetroilus22", "shakespearetwelfth20",
                "shakespearewinters19","all"));
        for (String filename : allfile) {
            MultipleOutputs.addNamedOutput(solosortjob, filename, TextOutputFormat.class,Text.class, NullWritable.class);
        }

        //排序改写成降序
        

        System.exit(solosortjob.waitForCompletion(true) ? 0 : 1);
        System.exit(0);


    }


    public static class SoloTokenizerMapper extends
            Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text(); // map输出的key
        private boolean caseSensitive; // 是否大小写敏感，从配置文件中读出赋值
        private Set<String> patternsToSkip = new HashSet<String>(); // 用来保存需过滤的关键词，从配置文件中读出赋值
        private Set<String> patternsToStop = new HashSet<String>(); // 用来保存需tingci的关键词，从配置文件中读出赋值
        private Configuration conf;
        private BufferedReader fis; // 保存文件输入流

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();

            caseSensitive = conf.getBoolean("wordcount.case.sensitive", true); // 配置文件中的wordcount.case.sensitive功能是否打开
            // wordcount.skip.patterns属性的值取决于命令行参数是否有-skip，具体逻辑在main方法中
            if (conf.getBoolean("wordcount.skip.patterns", false)) {

                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();


                Path patternsPath1 = new Path(patternsURIs[0].getPath());
                String patternsFileName1 = patternsPath1.getName().toString();
                parseSkipFile(patternsFileName1); // 将文件加入停词范围，具体逻辑参见parseStopFile(String
                    // fileName)
                Path patternsPath2 = new Path(patternsURIs[1].getPath());
                String patternsFileName2 = patternsPath2.getName().toString();
                parseStopFile(patternsFileName2); // 将文件加入过滤范围，具体逻辑参见parseSkipFile(String
                // fileName)



            }
        }
        private void parseStopFile(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
//                System.out.println("fis"+ fis);
                String pattern = null;
                while ((pattern = fis.readLine()) != null) { // SkipFile的每一行都是一个需要过滤的pattern，例如\!
                    patternsToStop.add(pattern);
//                    System.out.println("pA"+ pattern);
                }
            } catch (IOException ioe) {
                System.err
                        .println("Caught exception while parsing the cached file '"
                                + StringUtils.stringifyException(ioe));
            }
        }


        private void parseSkipFile(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
//                System.out.println("fis"+ fis);
                String pattern = null;
                while ((pattern = fis.readLine()) != null) { // SkipFile的每一行都是一个需要过滤的pattern，例如\!
                    patternsToSkip.add(pattern);
//                    System.out.println("pA"+ pattern);
                }
            } catch (IOException ioe) {
                System.err
                        .println("Caught exception while parsing the cached file '"
                                + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // 这里的caseSensitive在setup()方法中赋值
            String line = value.toString().toLowerCase(); // 如果设置了大小写敏感，就保留原样，否则全转换成小写
//            System.out.println("patternsToSkip"+patternsToSkip);



            for (String pattern : patternsToSkip) { // 将数据中所有满足patternsToSkip的pattern都过滤掉
                line = line.replaceAll(pattern, " ");
            }

            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String textName = fileSplit.getPath().getName();

            StringTokenizer itr = new StringTokenizer(line); // 将line以\t\n\r\f为分隔符进行分隔
            while (itr.hasMoreTokens()) {
                String nextword = itr.nextToken();
                if(patternsToStop.contains(nextword)){
                    continue;
                }
                if(Pattern.compile("^[-\\+]?[\\d]*$").matcher(nextword).matches()) {
                    continue;
                }
                word.set(nextword);
                String allword = nextword + "------all";
                if(nextword.length() >= 3) {

                    word.set(word + "------" + textName);
                    context.write(word, one);
                    context.write(new Text(allword),one);
                    // getCounter(String groupName, String counterName)计数器
                    // 枚举类型的名称即为组的名称，枚举类型的字段就是计数器名称
                    Counter counter = context.getCounter(
                            MapCounters.class.getName(),
                            MapCounters.INPUT_WORDS.toString());
                    counter.increment(1);
                }
            }
        }
    }

    public static class SoloSortReducer extends
            Reducer<IntWritable,Text, Text, NullWritable> {
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

        int all100 = 0;
        Map<String, Integer> filemap = new HashMap<String,Integer>();
        public void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException {

            for(Text val: values) {
                String[] line = val.toString().split("------");
                String word = line[0];
                String filename = line[1];
                filename = filename.replaceAll(".txt", "");
                filename = filename.replaceAll("-", "");
//                如果filename已经在hashmap中作为key，那么 value+1 ；
//                如果该filename的value已经100了，就不再写进去了。
//                如果filename不在hashmap中，那么把这个filename写进去，并且rank设置为0
//                如果一共有filecount个键并且每个value都是100，那么break出去。
                Iterator iter = filemap.entrySet().iterator();
                while(iter.hasNext()){
                    Map.Entry entry = (Map.Entry) iter.next();
                    Object num = entry.getValue();
                    Integer NUM = Integer.parseInt(String.valueOf(num));
                    if(NUM == 100){
                        all100 += 1;
                    }
                }
                Configuration conf = context.getConfiguration();
                int filenum = Integer.parseInt(conf.get("fileCount"));
                if(all100 == filenum){
                    break;
                }
                if(filemap.containsKey(filename)){
                    int sum = filemap.get(filename);
                    if(sum == 100){
                        continue;
                    }
                    else{
                        sum += 1;
                        filemap.put(filename,sum);
                        mos.write(filename,new Text(sum+": "+word+","+key),NullWritable.get());
                    }
                }else {
                    filemap.put(filename,1);
                    mos.write(filename,new Text(1+": "+word+","+key),NullWritable.get());
                }

            }
        }
    }

}