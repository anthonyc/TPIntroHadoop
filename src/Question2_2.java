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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question2_2 {
	
	public static class TagCountMapper extends Mapper<LongWritable, Text, Text, StringAndInt> {
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
						context.write(new Text(country.toString()), new StringAndInt(new Text(URLDecoder.decode(tag, "utf-8")), new IntWritable(1)));
					}
				}
			}
		}
	}
	
	public static class TagCountCombiner extends Reducer<Text, StringAndInt, Text, StringAndInt> {
		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values, Context context) throws IOException, InterruptedException {
			System.out.println("========Combine===========");
			System.out.println("key : " + key);
			System.out.println("=========================");
			HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
			
			for (StringAndInt value : values) {
				if (hashMap.containsKey(value.toString())) {
					hashMap.put(value.toString(), hashMap.get(value.toString()).intValue() + 1);
				} else {
					hashMap.put(value.toString(), 1);
				}
			}
			
			for (Map.Entry<String, Integer> entry : hashMap.entrySet()) {
				context.write(key, new StringAndInt(new Text(entry.getKey()), new IntWritable(entry.getValue())));
			}
			
			System.out.println("======Fin Combine=========");
		}
	}
	
	public static class TagCountReducer extends Reducer<Text, StringAndInt, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values, Context context) throws IOException, InterruptedException {
			System.out.println("========Reduce===========");
			System.out.println("key : " + key);
			System.out.println("=========================");
			PriorityQueue<StringAndInt> priorityQueue = new PriorityQueue<StringAndInt>();
			
			for (StringAndInt value : values) {
				priorityQueue.add(new StringAndInt(new Text(value.toString()), new IntWritable(value.getNumberOfOccurences())));
			}
			
			
			int numbersOfPopularTag = Integer.parseInt(context.getConfiguration().get("numbersOfPopularTag"));
			if (numbersOfPopularTag > priorityQueue.size()) {
				numbersOfPopularTag = priorityQueue.size();
			}
			
			for (int i=0; i < numbersOfPopularTag; i++) {
				context.write(key, new Text(priorityQueue.poll().toString()));
			}
			
			System.out.println("======Fin Reduce=========");
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		String numbersOfPopularTag = otherArgs[2];
		configuration.set("numbersOfPopularTag", numbersOfPopularTag);
		
		Job job = Job.getInstance(configuration, "Question1_1");
		job.setJarByClass(Question2_2.class);
		
		job.setMapperClass(TagCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringAndInt.class);
		
		job.setCombinerClass(TagCountCombiner.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringAndInt.class);
		
		job.setReducerClass(TagCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
