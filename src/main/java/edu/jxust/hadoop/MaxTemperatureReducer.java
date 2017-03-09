package edu.jxust.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer
 * @author GIS
 *
 */
public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text keyin, Iterable<IntWritable> valuein,Context context) throws IOException, InterruptedException {
	
		int maxValue = Integer.MIN_VALUE;
		for (IntWritable value : valuein) {
			maxValue = Math.max(maxValue, value.get());
			System.out.println(String.format("%s:%d",keyin, value.get()));
		}
		context.write(new Text(String.format("%s", keyin)), new IntWritable(maxValue));
		System.out.println(String.format("时间->%s:%d", keyin,maxValue));
		context.write(keyin, new IntWritable(maxValue));
	}

}
