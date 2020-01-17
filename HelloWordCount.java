/// A Test Program for word count.
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
/// 4. >hadoop HelloWordCount.jar /some/input/file /hdfs/output/dir
/// 5. >hadoop fs -cat /hdfs/output/dir/part-00000
///


package com.company;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.* ;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/// this wordcount2.0 use Hadoop2.x API.
/// check the tutorial at hadooop.apache.org/docs/r2.7.6/
public class Main {

    public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>{
        private final static IntWritable one = new IntWritable(1) ;
        private final Text word = new Text() ;
        public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            String line = value.toString() ;
            StringTokenizer tok = new StringTokenizer(line) ;
            while( tok.hasMoreTokens() )
            {
                word.set(tok.nextToken()) ;
                context.write( word , one);
            }
        }
    }

    public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        public void reduce(Text key , Iterator<IntWritable> values ,
                           Context context) throws IOException, InterruptedException {
            int sum = 0 ;
            while( values.hasNext())
            {
                sum += values.next().get() ;
            }
            context.write(key , new IntWritable(sum));
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // write your code here
        Job job = Job.getInstance(new Configuration() , "Hello Word Count") ;
        job.setJarByClass(Main.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job , new Path(args[0]));
        FileOutputFormat.setOutputPath(job , new Path(args[1]));

        job.waitForCompletion(true) ;
    }
}
