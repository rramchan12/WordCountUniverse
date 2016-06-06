package com.ravi.starter;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    //hadoop supported data types                                                                                                                                                          
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value,
    		Context context)
    		throws IOException, InterruptedException {
    	// TODO Auto-generated method stub
    	
    System.out.println("Key is : "+key.get());
    String line = value.toString(); 
    StringTokenizer tokenizer = new StringTokenizer(line); 
    while (tokenizer.hasMoreTokens()){
    	
    	word.set(tokenizer.nextToken());
    	//sending to Output Collector which then passes the same to reducer
    	context.write(word, one);
    	
    }
    
    }

}
