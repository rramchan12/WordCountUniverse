package com.ravi.starter;

import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartitioner extends Partitioner<Text, IntWritable>  {
	
	// this partitioner will break our result set up into three outputs:
    // one containing "old words"
    // one containing "timelss words"
    // everything else
	
	public int getPartition(Text key, IntWritable value, int numReduceTasks) 
	
	{
		String[] oldWords = {"thou", "shall", "hath", "upon"};
		String[] newWords ={"i", "love", "you"};
		
		Integer reduceNumber = 0;
		if (Arrays.asList(oldWords).contains(key.toString())){
			reduceNumber =1;
		}
		else if (Arrays.asList(newWords).contains(key.toString())){
			reduceNumber=2;
		}
		
		
		return reduceNumber;
	}

}
