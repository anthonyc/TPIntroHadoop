import java.io.IOException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question3_1 {
	
	public static class TagCountMapperJob1 extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("\t");
			Country country = null;
			boolean hasGeoCoordinates = false;
			double latitude = 0;
			double longitude = 0;
			
			try {
				longitude = Double.parseDouble(line[10]);
				latitude = Double.parseDouble(line[11]);
				
				hasGeoCoordinates = true;
			} catch (NullPointerException nullPointerException) {
				System.out.println("[WARNING] This line doesn't content a valid GPS coordinates");
			} catch (NumberFormatException numberFormatException) {
				System.out.println("[WARNING] This line doesn't content a valid GPS coordinates");
			}
			
			if (hasGeoCoordinates) {
				country = Country.getCountryAt(latitude, longitude);
			}
			
			if (country instanceof Country) {
				for (String tag : line[8].split(",")) {
					if (tag != null && !tag.isEmpty()) {
						context.write(new Text(country.toString()), new Text(URLDecoder.decode(tag, "utf-8")));
					}
				}
			}
		}
	}
	
	public static class TagCountCombinerJob1 extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			System.out.println("========Combine===========");
			System.out.println("key : " + key);
			System.out.println("=========================");
			
			for (Text value : values) {
				context.write(key, value);
			}
			
			System.out.println("======Fin Combine=========");
		}
	}
	
	public static class TagCountReducerJob1 extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			System.out.println("========Reduce===========");
			System.out.println("key : " + key);
			System.out.println("=========================");
			
			for (Text value : values) {
				context.write(key, value);
			}
			
			System.out.println("======Fin Reduce=========");
		}
	}
	
	public static class TagCountMapperJob2 extends Mapper<Text, Text, Text, StringAndIntWritable> {
		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			context.write(key, new StringAndIntWritable(key, new IntWritable(1)));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		String finaleOutput = otherArgs[2];
		String numbersOfPopularTag = otherArgs[3];
		configuration.set("numbersOfPopularTag", numbersOfPopularTag);
		
		Job job1 = Job.getInstance(configuration, "Question3_1");
		job1.setJarByClass(Question3_1.class);
		
		job1.setMapperClass(TagCountMapperJob1.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		
		job1.setCombinerClass(TagCountCombinerJob1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setReducerClass(TagCountReducerJob1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job1, new Path(input));
		job1.setInputFormatClass(TextInputFormat.class);
		
		FileOutputFormat.setOutputPath(job1, new Path(output));
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		if (job1.waitForCompletion(true)) {
			Job job2 = Job.getInstance(configuration, "Question3_1");
			
			job2.setJarByClass(Question3_1.class);
			
			job2.setMapperClass(TagCountMapperJob1.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
			
			job2.setCombinerClass(TagCountCombinerJob1.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			
			job2.setReducerClass(TagCountReducerJob1.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job1, new Path(output));
			job2.setInputFormatClass(SequenceFileInputFormat.class);
			
			FileOutputFormat.setOutputPath(job1, new Path(finaleOutput));
			job2.setOutputFormatClass(TextOutputFormat.class);
			
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
	}
}
