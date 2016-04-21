package io.uve.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import io.uve.stats.StatsParser;

public class Stat_pv_count {
	
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String msg = value.toString();
			if (msg.indexOf("main_feed") > -1) {
				Text pv_key = new Text();
				pv_key.set("pv");
				context.write(pv_key, one);
				
				StatsParser statsParser = new StatsParser(msg);
				int tmeta2_num = statsParser.getL2Count();
				Text impression_key = new Text();
				impression_key.set("下发");
				context.write(impression_key, new IntWritable(tmeta2_num));
				
				int available_pos_num = 0;
				try {
					available_pos_num = statsParser.getJSONObject().getInt("available_pos");
				} catch (Exception e) {
					
				}
				Text available_pos_key = new Text();
				available_pos_key.set("available_pos_sum");
				context.write(available_pos_key, new IntWritable(available_pos_num));
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
}
