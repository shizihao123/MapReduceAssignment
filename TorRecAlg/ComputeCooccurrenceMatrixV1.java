/** Compute the cooccurrence matrix for movies. 
 * @author wangxd
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

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

public class ComputeCooccurrenceMatrixV1 {

	public static final double MINRATIO = 0.1;
	public static final int MINTOUCH = 0;
	
	public static class CooccurrenceMap extends Mapper<LongWritable, Text, Text, Text> {

		Text one = new Text("1");
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			// 获取 电影 or 用户 ID列表
			String line = value.toString();
			line = line.substring(line.indexOf("\t")+1);
            String[] items = line.split(",");
            
            // 去重
            items = (String[])(new HashSet<String>(Arrays.asList(items)).toArray(new String[0]));
            
			// 转换为Text格式
            Text[] texts = new Text[items.length];
			for (int i = 0; i < items.length; i++) {
				texts[i] = new Text(items[i]);
			}
			
			// 向Reduce节点发送<[itema,itemb], 1>
			for (int i = 0; i < texts.length; i++) {
				for (int j = 0; j < texts.length; j++) {
					context.write(new Text(items[i] + "," + items[j]), one);
				}
			}
		}
	
	}
	
	public static class CooccurrenceReduce extends Reducer <Text, Text, Text, IntWritable> {
		
		public void reduce (Text key, Iterable<Text>values, Context context) throws IOException, InterruptedException {
			int num = 0;
			for (Text value : values) {
				num++;
			}
			
			// 滤除得票过低的
			if (num >= MINTOUCH)
				context.write(key, new IntWritable(num));
		}
		
	}
	
	public static class RatioMap extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			// 获取 主电影，从电影，得票数
			String line = value.toString();
			int comSeperator = line.indexOf(",");
			int tabSeperator = line.indexOf("\t");
			String itemMaster = line.substring(0, comSeperator);
			String itemSlave = line.substring(comSeperator + 1, tabSeperator);
			String numString = line.substring(tabSeperator + 1);
			
			context.write(new Text(itemMaster), new Text(itemSlave + ":" + numString));
		}
	
	}
	
	
	public static class RatioReduce extends Reducer <Text, Text, Text, Text> {
		
		public void reduce (Text key, Iterable<Text>values, Context context) throws IOException, InterruptedException {
			
			double numOfMasters = 0.0;
			String valueString;
			String masterString = key.toString();
			String slaveString;
			int colonSeperator;
			LinkedList<Text> cache = new LinkedList<Text>();
			
			// 统计电影被看过的次数
			for (Text value : values) {
				valueString = value.toString();
				colonSeperator = valueString.indexOf(":");
				slaveString = valueString.substring(0, colonSeperator);
				if (slaveString.equals(masterString)) {
					numOfMasters += Integer.parseInt(valueString.substring(colonSeperator + 1));
				}
				// 为了第二遍遍历，我得进行缓存
				cache.add(new Text(value));
			}
			
			// 统计看过该电影的用户看过的其他电影比例
			String ratioList = "";
			double ratio;
			for (Text value : cache) {
				valueString = value.toString();
				colonSeperator = valueString.indexOf(":");
				slaveString = valueString.substring(0, colonSeperator);
				ratio = Integer.parseInt(valueString.substring(colonSeperator + 1)) / numOfMasters;
				if (!slaveString.equals(masterString) && ratio >= MINRATIO) {
					ratioList += ";" + slaveString + ":" + ratio;
				}
			}
			
			//System.out.println("numOfMasters = " + numOfMasters);
			
			if (ratioList.length() > 0) {
				context.write(key, new Text(ratioList.substring(1)));
			}
		}
		
	}

	public static void ComputeCooccurrence (String iFileName, String oFileName) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();		 
		Job job = new Job(conf, "MovieRemmendation::ComputeCooccurrence");

		// 设置Reduce节点数量
		job.setNumReduceTasks(6);
		 
		// 设置输出格式
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		 		 
		job.setMapperClass(CooccurrenceMap.class);
		job.setReducerClass(CooccurrenceReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setJarByClass(ComputeCooccurrenceMatrixV1.class);
		 
		FileInputFormat.addInputPath(job, new Path(iFileName));
		FileOutputFormat.setOutputPath(job, new Path(oFileName));
		
		job.waitForCompletion(true);
	}
	
	
	public static void ComputeRatio (String iFileName, String oFileName) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();		 
		Job job = new Job(conf, "MovieRemmendation::ComputeRatio");

		// 设置Reduce节点数量
		job.setNumReduceTasks(6);
		 
		// 设置输出格式
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		 		 
		job.setMapperClass(RatioMap.class);
		job.setReducerClass(RatioReduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setJarByClass(ComputeCooccurrenceMatrixV1.class);
		 
		FileInputFormat.addInputPath(job, new Path(iFileName));
		FileOutputFormat.setOutputPath(job, new Path(oFileName));
		
		job.waitForCompletion(true);
	}
	
	
	/**
	 * @param args args[0] indicates the input file's name; args[1] indicates the output 
	 * file's name. 
	 */
	 public static void main(String[] args) throws Exception {
		 
		 if (args.length != 2) {
			 System.out.println("Usage : ComputeCooccurrenceMatrix inputFileName outputFileName");
			 System.exit(0);
		 }
		 
		 // 统计电影同现频数
		 ComputeCooccurrence(args[0], args[1] + "/Cooccurrence");
		 // 统计电影同现频率
		 ComputeRatio(args[1] + "/Cooccurrence", args[1] + "/Ratio");
	 }

}
