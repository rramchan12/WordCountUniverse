package com.ravi.starter;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountWithPartitioner {

	public static void main(String[] args) throws Exception{

		Job wordCountPartitionJob;
		try {
			wordCountPartitionJob = new Job();
			wordCountPartitionJob.setJobName("wordCountPartitionJob");

			// Set Mapper and Reducer
			wordCountPartitionJob.setMapperClass(WordCountMapper.class);
			wordCountPartitionJob.setReducerClass(WordCountReducer.class);
			// Set Partitioner and no of Reduce tasks
			wordCountPartitionJob
					.setPartitionerClass(WordCountPartitioner.class);
			wordCountPartitionJob.setNumReduceTasks(3);
			
			// Set Input and Output Key Class
			wordCountPartitionJob.setOutputKeyClass(Text.class);
			wordCountPartitionJob.setOutputValueClass(IntWritable.class);

			// Set Input and Output Directory
			FileInputFormat.addInputPath(wordCountPartitionJob, new Path(args[0]));
			FileOutputFormat.setOutputPath(wordCountPartitionJob, new Path(args[1]));
			
			boolean success = wordCountPartitionJob.waitForCompletion(true);
			System.exit(success ? 0 : 1);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
