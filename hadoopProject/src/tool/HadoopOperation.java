package tool;

import java.io.*;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;



public class HadoopOperation {
	
	public static void uploadInputFile(String localFile) throws IOException{
		Configuration conf = new Configuration();
		String hdfsPath = "hdfs://localhost:9000/";
		String hdfsInput = "hdfs://localhost:9000/user/hadoop/input";
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		fs.copyFromLocalFile(new Path(localFile), new Path(hdfsInput));
		fs.close();
		System.out.println("上传到文件夹");
	}
	
	public static void getOutput(String outputFile) throws IOException{
		String remoteFile = "hdfs://localhost:9000/output/part-r-00000";
		Path path = new Path(remoteFile);
		Configuration conf = new Configuration();
		String hdfsPath = "hdfs://localhost:9000/";
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		fs.copyFromLocalFile(path, new Path(outputFile));
		System.out.println("已将输出文件保存到本地");
		fs.close();
	}
	
	public static void deleteOutput() throws IOException{
		Configuration conf = new Configuration();
		String hdfsOutput = "hdfs://localhost:9000/user/hadoop/output";
		String hdfsPath = "hdfs://localhost:9000/";
		Path path = new Path(hdfsOutput);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		fs.deleteOnExit(path);
		fs.close();
		System.out.println("output文件已删除");
	}
	

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			line = line.replace("\\", "");
			String regex = "性别：</span><span class=\"pt_detail\">(.*?)</span>";
			Pattern pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(line);
			while(matcher.find()){
				String term = matcher.group(1);
				word.set(term);
				context.write(word, one);
			}
		}
	}
	
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key,Iterable< IntWritable> values,  Context context) throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable val:values){
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception{
//		deleteOutput();
//		runMapReduce(args);
		 
		getOutput("/Users/Har/Desktop/userinfo.txt");
	}
}
