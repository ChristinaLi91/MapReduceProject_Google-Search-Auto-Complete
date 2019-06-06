import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.*;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threashold;

		@Override
		public void setup(Context context) {
			// how to get the threashold parameter from the configuration?
			Configuration conf = context.getConfiguration();
			threashold = conf.getInt("threashold", 20);
		}


		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if((value == null) || (value.toString().trim()).length() == 0) {
				return;
			}
			//this is cool\t20
			String line = value.toString().trim();

			String[] wordsPlusCount = line.split("\t");
			if(wordsPlusCount.length < 2) {
				return;
			}

			String[] words = wordsPlusCount[0].split("\\s+");
			int count = Integer.valueOf(wordsPlusCount[1]);

			if(count < threashold){
				return;
			}

			StringBuilder stringBuilder = new StringBuilder();
			for (int i=0; i<words.length-1; i++){
				stringBuilder.append(words[i]);
			}
			String outputKey = stringBuilder.toString().trim();
			String outputValue = words[words.length -1];

			//context.getCounter("stringBuilder", stringBuilder);

			if(!((outputKey == null) || (outputKey.length() <1))) {
				context.write(new Text(outputKey), new Text(outputValue + "=" + count));
		}
	}
}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int n;
		// get the n parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			n = conf.getInt("n", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			//can you use priorityQueue to rank topN n-gram, then write out to hdfs?
			TreeMap<Integer, List<String>> tm = new TreeMap<Integer, List<String>>(Collections.reverseOrder());
			for(Text val: values){
				String curValue = val.toString().trim();
				String word = curValue.split("=")[0].trim();
				int count = Integer.parseInt(curValue.split("=")[1].trim());
				if(tm.containsKey(count)){
					tm.get(count).add(word);
				}
				else{
					List<String> list = new ArrayList<String>();
					list.add(word);
					tm.put(count,list);
				}
			}
		 }
		}
}
