package io.uve.hdfs2hdfs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class Hdfs2hdfsTask {

	public static class ReadWriteMapper extends Mapper<Object, Text, Text, NullWritable> {
		private final static NullWritable empty = null;
		private Logger logger = Logger.getLogger(ReadWriteMapper.class);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String msg = value.toString();
			String parsedMsg = ReadWriteProcess.parseLine(msg);
			if (parsedMsg != null) {
				logger.info(parsedMsg);
				context.write(new Text(parsedMsg), empty);
			}
		}
	}

	public static class ReadWriteReducer extends Reducer<Text, NullWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
		}
	}
}
