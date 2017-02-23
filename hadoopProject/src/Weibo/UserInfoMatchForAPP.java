package weibo;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;


public class UserInfoMatchForAPP {
	public static class UserInfoWritable implements Writable {
		private String sex = null;
		private String area = null;
		private String birth = null;
		
		public String getSex() {
			return sex;
		}

		public String getArea() {
			return area;
		}

		public String getBirth() {
			return birth;
		}
		
	    public void setSex(String sex) {
			this.sex = sex;
		}

		public void setArea(String area) {
			this.area = area;
		}

		public void setBirth(String birth) {
			this.birth = birth;
		}

	    public UserInfoWritable() {}

	    public UserInfoWritable(String sex, String area, String birth) {
	        this.sex = sex;
	        this.area = area;
	        this.birth = birth;
	    }
	    
	    @Override
	    public void readFields(DataInput in) throws IOException {
	    	this.sex = WritableUtils.readString(in);
	    	this.area = WritableUtils.readString(in);
	    	this.birth = WritableUtils.readString(in);
	    }

	    @Override 
	    public void write(DataOutput out) throws IOException {
	    	WritableUtils.writeString(out, sex);
	    	WritableUtils.writeString(out, area);
	    	WritableUtils.writeString(out, birth);
	    }

	    @Override
	    public String toString() {
	        return this.sex + "\t" + this.area + "\t" + this.birth;
	    }
	}
	
	public static class UserInfoMapper extends Mapper<Object, Text, Text, UserInfoWritable>{
		private Text uid = new Text();
		private UserInfoWritable userInfo;
		Pattern uidPattern = Pattern.compile("uid=(\\d{10})");
		Pattern sexPattern = Pattern.compile("<br />性别:(.{1,2})<");
		Pattern areaPattern = Pattern.compile("<br />地区:(.{1,15})<");
		Pattern birthPattern = Pattern.compile("<br />生日:(.{1,10})<");
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
			String url = value.toString();
					
			Matcher matcher;
			if (uid.toString().equals("")){
				matcher = uidPattern.matcher(url);
				if (matcher.find()){
					String term = matcher.group(1);
					uid.set(term);
				}
			}
			
			if (userInfo.getSex()==null){
				matcher = sexPattern.matcher(url);
				if (matcher.find()){
					String term = matcher.group(1);
					userInfo.setSex(term);
				}
			}
			
			if (userInfo.getArea()==null){
				matcher = areaPattern.matcher(url);
				if (matcher.find()){
					String term = matcher.group(1);
					userInfo.setArea(term);
				}
			}
			
			if (userInfo.getBirth()==null){
				matcher = birthPattern.matcher(url);
				if (matcher.find()){
					String term = matcher.group(1);
					userInfo.setBirth(term);
				}	
			}
			
		}
		
		
		@Override
		public void run(Context context) throws IOException, InterruptedException {
		    setup(context);
		    String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		    System.out.println(fileName);
		    userInfo = new UserInfoWritable();
		    while (context.nextKeyValue()) {
		    	
		       map(context.getCurrentKey(), context.getCurrentValue(), context);
		       if (userInfo.getBirth()!=null){
		    	   break;
		       }
		    }
		    if (!this.uid.toString().equals("")){
		    	context.write(uid, userInfo);
		    }
		    
		    cleanup(context);
		}
	}
	
	public static class UserInfoReducer extends Reducer<Text, UserInfoWritable, Text, UserInfoWritable> {
		public void reduce(Text key, UserInfoWritable value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}
	

	public static void runMapReduce(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if(otherArgs.length != 2){
			System.err.println("Usage: <in><out>");
			System.exit(2);
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Users Information Collecting");
		job.setJarByClass(UserInfoMatchForAPP.class);
		job.setMapperClass(UserInfoMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(UserInfoWritable.class);
		FileInputFormat.setInputDirRecursive(job, true);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	public static void main(String[] args) throws Exception{
		String[] args1 = {"/weibo_simple", "/weibo_m_13"};
		runMapReduce(args1);
//		runMapReduce(args);
	}
}
