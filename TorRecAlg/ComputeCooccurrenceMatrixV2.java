/** Compute the cooccurrence matrix for movies. 
 * @author wangxd
 */

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ComputeCooccurrenceMatrixV2 {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

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
			
			// 向Reduce节点发送<itema, itemb>
			for (int i = 0; i < texts.length; i++) {
				for (int j = 0; j < texts.length; j++) {
					context.write(texts[i], texts[j]);
				}
			}
		}
	
	}
	
	public static class Reduce extends Reducer <Text, Text, Text, Text> {
	
		public static double minRatio;
		
		public void setup (Context context) {
			minRatio = Double.parseDouble(context.getConfiguration().get("MINRATIO", "0.1"));
		}
		
		public void reduce (Text key, Iterable<Text>values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
			String keyString = key.toString();
			String valueString;
			double numOfMasters = 0.0;
			Integer numOfSlaves = 0;
			
			for (Text value : values) {
				valueString = value.toString();
				if (valueString.equals(keyString)) {
					numOfMasters++;
				} else {
					numOfSlaves = hashMap.get(valueString);
					if (numOfSlaves == null) {
						hashMap.put(valueString, 1);
					} else {
						hashMap.put(valueString, numOfSlaves + 1);
					}
					
				}
			}
			//System.out.println("numOfMasters = " + numOfMasters + "; hasmap.size = " + hashMap.size());
			//System.out.println("The hash map is : ");
			//for (String item : hashMap.keySet()) {
				//System.out.print(item + " ");
			//}

			double ratio = 0.0;
			String resultString = "";
			for (String item : hashMap.keySet()) {
				//System.out.println("item = " + item);
				Integer num = hashMap.get(item);
				if (num == null) {
					System.out.println("Impossible!");
				}
				ratio = hashMap.get(item) / numOfMasters;
				
				if (ratio >= minRatio) {
					resultString += ";" + item + ":" + ratio;
				}
			}
			
			if (resultString.length() > 0) {
				context.write(key, new Text(resultString.substring(1)));
			}
		}
	}

	/**
	 * @param args args[0] indicates the MINRATIO. args[0] indicates the input file's name;
	 * args[1] indicates the output file's name. 
	 */
	 public static void main(String[] args) throws Exception {
		 
		 if (args.length != 3) {
			 System.out.println("Usage : ComputeCooccurrenceMatrix MINRATIO inputFileName outputFileName");
			 System.exit(0);
		 }
		 
		 Configuration conf = new Configuration();		 

		 double minRatio = Double.parseDouble(args[0]);
		 if (minRatio >= 0 && minRatio <= 1) {
			 conf.set("MINRATIO", args[0]);
		 } else {
			 conf.set("MINRATIO", "0.0");
		 }
		 
		 Job job = new Job(conf, "MovieRemmendation::ComputeCooccurrenceMatrix");
		 
		 // 设置Reduce节点数量
		 job.setNumReduceTasks(6);
		 
		 // 设置输出格式
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
		 		 
		 job.setMapperClass(Map.class);
		 job.setReducerClass(Reduce.class);
		 job.setInputFormatClass(TextInputFormat.class);
		 job.setOutputFormatClass(TextOutputFormat.class);
		 
		 job.setJarByClass(ComputeCooccurrenceMatrixV2.class);
		 
		 FileInputFormat.addInputPath(job, new Path(args[1]));
		 FileOutputFormat.setOutputPath(job, new Path(args[2]));

		 job.waitForCompletion(true);
	 }

}
