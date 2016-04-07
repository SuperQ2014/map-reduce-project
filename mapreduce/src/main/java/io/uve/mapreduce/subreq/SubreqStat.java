package io.uve.mapreduce.subreq;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class SubreqStat {
	
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, LongWritable> {
		private final static LongWritable one = new LongWritable(1);

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String msg = value.toString();
			SubreqParser parser = new SubreqParser(msg);
			String subreqName = parser.getSubreqname();
			
			context.write(new Text(subreqName), one);
			context.write(new Text("total"), one);
		}
	}

	public static class LongSumReducer extends
			Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();

		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
}
