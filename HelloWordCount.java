/// A Test Program
/// Try to append 2D Array Data into Sequence File.
/// 2020-1-18 by wangfengdev@163.com
///
/// need add external libs:
/// /usr/local/hadoop/share/hadoop/common/hadoop-common-2.7.6.jar
/// /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.6.jar
/// /usr/local/hadoop/share/hadoop/common/lib/commons-logging-1.1.3.jar
/// /usr/local/hadoop/share/hadoop/common/lib/guava-11.0.2.jar
/// /usr/local/hadoop/share/hadoop/common/lib/commons-collections-3.2.2.jar
/// /usr/local/hadoop/share/hadoop/common/lib/log4j-1.2.17.jar
/// /usr/local/hadoop/share/hadoop/common/lib/commons-io.2.4.jar
///
/// run:
/// 1. >start-dfs.sh
/// 2. >hadoop fs -put ~/testfile.txt /testfile.txt
/// 3. use idea build the jar.
/// 4. >hadoop xxx.jar /some/input/file /hdfs/output/dir
/// 5. >hadoop fs -cat /hdfs/output/dir/part-00000
///


package com.company;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.* ;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class TestArray2SeqFile {

    public static class MyMapClass extends MapReduceBase
            implements Mapper<LongWritable,Text,Text,IntWritable>{
        private final static IntWritable one = new IntWritable(1) ;
        private final Text word = new Text() ;
        public void map(LongWritable key,Text value,
                        OutputCollector<Text,IntWritable> output,
                        Reporter reporter) throws IOException {
            String line = value.toString() ;
            StringTokenizer tok = new StringTokenizer(line) ;
            while( tok.hasMoreTokens() )
            {
                word.set(tok.nextToken()) ;
                output.collect( word , one);
            }
        }
    }

    public static class MyRedClass extends MapReduceBase
            implements Reducer<Text,IntWritable,Text,IntWritable>{
        public void reduce(Text key , Iterator<IntWritable> values ,
                           OutputCollector<Text,IntWritable> output ,
                           Reporter reporter ) throws IOException {
            int sum = 0 ;
            while( values.hasNext())
            {
                sum += values.next().get() ;
            }
            output.collect(key , new IntWritable(sum));
        }
    }


    public static void main(String[] args) throws IOException {
	// write your code here
        JobConf conf = new JobConf(TestArray2SeqFile.class) ;
        conf.setJobName("A hello world demo");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class) ;

        conf.setMapperClass(MyMapClass.class);
        conf.setReducerClass(MyRedClass.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf , new Path(args[0]));
        FileOutputFormat.setOutputPath(conf , new Path(args[1]));

        JobClient.runJob(conf) ;

    }
}
