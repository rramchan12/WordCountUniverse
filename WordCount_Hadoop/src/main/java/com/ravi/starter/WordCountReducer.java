package com.ravi.starter;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text,  IntWritable>{
	
    //Reduce method for just outputting the key from mapper as the value from mapper is just an empty string
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int sum = 0; 
		/*iterates through all the values available with a key and add them together and give the                                                                                          
        final result as the key and sum of its values*/
		System.out.println("Key is :"+key);
		for (IntWritable value : values ){
			System.out.println("Value is :"+value.get());
			sum = sum + value.get();
		}
		context.write(key, new IntWritable(sum));
	}
    
}
