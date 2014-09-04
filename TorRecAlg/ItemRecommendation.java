import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;

/**
 * 
 */

/**
 * @author wangxd
 */
public class ItemRecommendation {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private HashMap<String, List<String>> coMatrix = new HashMap<String, List<String>>();
		
		public void setup (Context context) throws IOException, InterruptedException {
			super.setup(context);
			// Get the cached archives/files
			Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			String line, item, values;
			int pivot;
			for (Path localFile : localFiles) {
				BufferedReader bufferedReader = new BufferedReader(new FileReader(localFile.toString()));
				try {
					while ((line = bufferedReader.readLine()) != null) {
						pivot = line.indexOf("\t");
						item = line.substring(0, pivot);
						values = line.substring(pivot + 1);
						if (values != null && values.length() > 0) {
							//System.out.println("===> " + item + " + " + values);
							coMatrix.put(item, Arrays.asList(values.split(";")));
						}
					}
				} finally { 
					bufferedReader.close();
					//System.out.println("WTF");
				}
			}
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//			System.out.println("==> the size of the comatrix is " + coMatrix.size());
			// 获取 电影 or 用户 ID列表
			String line = value.toString();
			String user = line.substring(0, line.indexOf("\t"));
			Text userText = new Text(user);
			line = line.substring(line.indexOf("\t")+1);
	        String[] items = line.split(",");
	        
	        // 去重
	        HashSet<String> itemSet = new HashSet<String>(Arrays.asList(items));
	        //items = (String[])(new HashSet<String>(Arrays.asList(items)).toArray(new String[0]));
	        
	        List<String> values;
	        for (String item : itemSet.toArray(new String[0])) {
	        	values = coMatrix.get(item);
	        	if (values != null) {
	        		for (String item_score : values) {
	        			if (!itemSet.contains(item_score.substring(0, item_score.indexOf(":"))))
	        				context.write(userText, new Text(item_score));
	        		}
	        	}
	        }
	        
//	        String abc = null;
//	        if (abc.length() > 0) {
//	        	System.out.println("Whatever");
//	        }
	        	        
		}
	}
	
	
	public static class Reduce extends Reducer <Text, Text, Text, Text> {
		
		public void reduce (Text key, Iterable<Text>values, Context context) throws IOException, InterruptedException {
			HashMap<String, Double> item_scores = new HashMap<String, Double>();
			String valueString;
			String item;
			Double score_inc, score_base;
			int pivot;
			
			for (Text value : values) {
				valueString = value.toString();
				pivot = valueString.indexOf(":");
				item = valueString.substring(0, pivot);
				score_inc = Double.parseDouble(valueString.substring(pivot + 1));
				score_base = item_scores.get(item);
				if (score_base != null) {
					item_scores.put(item, score_base * (1 + score_inc));
				} else {
					item_scores.put(item, 1 + score_inc);
				}
			}
			
			ValueComparator bvc =  new ValueComparator(item_scores);
			TreeMap<String, Double> sorted_map = new TreeMap<String, Double>(bvc);
			sorted_map.putAll(item_scores);

			//context.write(key, new Text(sorted_map.toString()));
			
			String result = "";
			for (String value : sorted_map.keySet()) {
				// Attention : DO NOT use sorted_map.You have given it an abnormal comparator!
				result += ";" + value + ":" + item_scores.get(value);
			}
			if (result.length() > 0) {
				context.write(key, new Text(result.substring(1)));
			}
			
			
		}
		
	}

	
	/**
	 * @param args
	 * @throws URISyntaxException 
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws URISyntaxException, IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		 if (args.length != 3) {
			 System.out.println("Usage : ItemRecommendation coMatrixDir inputFileName outputFileName");
			 System.exit(0);
		 }
		 
		 Configuration conf = new Configuration();
		 
		 // 把共现矩阵存入分布式缓存中
		 FileSystem hdfs = FileSystem.get(conf);
		 
		 FileStatus[] files = hdfs.listStatus(new Path(args[0]), new PathFilter() {
			@Override
			public boolean accept(Path pathname) {
				return pathname.getName().startsWith("part-");
			}
		 });
		 
		 for (FileStatus file : files) {
			 //System.out.println(file.getPath());
			 DistributedCache.addCacheFile(new URI(file.getPath().toString()), conf);
		 }
		 
		 Job job = new Job(conf, "ZiJingBTMR::ItemRecommendation");
		 
		 // 设置Reduce节点数量
		 job.setNumReduceTasks(6);
		 
		 // 设置输出格式
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
		 		 
		 job.setMapperClass(Map.class);
		 job.setReducerClass(Reduce.class);
		 job.setInputFormatClass(TextInputFormat.class);
		 job.setOutputFormatClass(TextOutputFormat.class);
		 
		 job.setJarByClass(ItemRecommendation.class);
		 
		 FileInputFormat.addInputPath(job, new Path(args[1]));
		 FileOutputFormat.setOutputPath(job, new Path(args[2]));
		 
		 job.waitForCompletion(true);

	}

}


class ValueComparator implements Comparator<String> {

    Map <String, Double> base;
    public ValueComparator(Map <String, Double> base) {
        this.base = base;
    }

    // Note: this comparator imposes orderings that are inconsistent with equals.    
    public int compare(String a, String b) {
        if (base.get(a) >= base.get(b)) {
            return -1;
        } else {
            return 1;
        } // returning 0 would merge keys
    }
}
