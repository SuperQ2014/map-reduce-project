package io.uve.hdfs2hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import io.uve.hdfs2hdfs.Hdfs2hdfsTask.ReadWriteMapper;

public class Main {

	public static void main(String[] args) throws Exception {
		
		if (args.length != 3) {
			System.err.println("hadoop jar your.jar [hdfs input path] [local output path] [job name]");
		}
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, args[2]);
		job.setJarByClass(Hdfs2hdfsTask.class);
		job.setMapperClass(ReadWriteMapper.class);
		job.setNumReduceTasks(0);
//		job.setReducerClass(ReadWriteReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
