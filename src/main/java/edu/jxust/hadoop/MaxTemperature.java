package edu.jxust.hadoop;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class MaxTemperature {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		// String uri="hdfs://192.168.128.1:9000/"; //hdfs ��ַ
		// String local="G:\\TDDOWNLOAD\\jdk-7u79-linux-x64.rpm"; //����·��
		// String remote="hdfs://192.168.128.1:9000/tim";
		// Configuration conf = new Configuration();
		// conf.set("dfs.blocksize","67108864");
		// copyFile(conf,uri,local,remote);
		Logger.getLogger(MaxTemperature.class);
		maxTemperature();
	}

	private static void maxTemperature() throws Exception {
		Configuration conf = new Configuration();
		conf.set("dfs.blocksize", "67108864");
		Job job = Job.getInstance(conf, "MaxTemp");
		job.setJarByClass(MaxTemperature.class);

		job.setJobName("Max Temperature");
		/*
		 * String inPath = "hdfs://192.168.128.1:9000/tim/ncdc"; String outPath
		 * = "hdfs://192.168.128.1:9000/tim/MaxTemperatureout3";
		 */
		String inPath = "hdfs://192.168.128.1:9000/tim/ncdc";
		String outPath = String.format("hdfs://192.168.128.1:9000/tim/test/MaxTemperatureout_%s",
				System.currentTimeMillis());

		FileInputFormat.addInputPath(job, new Path(inPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));

		job.setMapperClass(MaxTemperatureMapper.class);

		job.setReducerClass(MaxTemperatureReducer.class);

		job.setMapOutputKeyClass(Text.class);

		job.setMapOutputValueClass(IntWritable.class);
		System.out.println(job.waitForCompletion(true));
		String s =StringUtils.leftPad(Integer.toBinaryString(50), 10, '0');
		System.out.println("Lang测试："+s);
	}

	/**
	 * �ϴ��ļ�
	 * 
	 * @param conf
	 * @param local
	 * @param remote
	 * @throws IOException
	 */
	public static void copyFile(Configuration conf, String uri, String local, String remote) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		fs.copyFromLocalFile(new Path(local), new Path(remote));
		System.out.println("copy from: " + local + " to " + remote);
		fs.close();
	}

}
