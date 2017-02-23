package weibo;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;


public class UserInfoMatchForWeb {
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
		private Text nickName = new Text();
		private UserInfoWritable userInfo;
		Pattern nickNamePattern = Pattern.compile("昵称：</span><span class=\"pt_detail\">(.*?)</span>");
		Pattern sexPattern = Pattern.compile("性别：</span><span class=\"pt_detail\">(.*?)</span>");
		Pattern areaPattern = Pattern.compile("所在地：</span><span class=\"pt_detail\">(.*?)</span>");
		Pattern birthPattern = Pattern.compile("生日：</span><span class=\"pt_detail\">(\\d{4}).*?</span></li>");
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String url = value.toString();
			if (url.indexOf("span class=\"pt_detail") == -1){
				return;
			}
					
			Matcher matcher;
			if (nickName.toString().equals("")){
				matcher = nickNamePattern.matcher(url);
				if (matcher.find()){
					String term = matcher.group(1);
					nickName.set(term);
					return;
				}
			}
			
			if (userInfo.getSex()==null){
				matcher = sexPattern.matcher(url);
				if (matcher.find()){
					String term = matcher.group(1);
					userInfo.setSex(term);
					return;
				}
			}
			
			if (userInfo.getArea()==null){
				matcher = areaPattern.matcher(url);
				if (matcher.find()){
					String term = matcher.group(1);
					userInfo.setArea(term);
					return;
				}
			}
			
			if (userInfo.getBirth()==null){
				matcher = birthPattern.matcher(url);
				if (matcher.find()){
					int term = Integer.parseInt(matcher.group(1));
					if(term<=1979){
						userInfo.setBirth("-79");
					}else if(term<=1989){
						userInfo.setBirth("80-89");
					}else if(term<=1994){
						userInfo.setBirth("90-94");
					}else{
						userInfo.setBirth("95-");
					}
					
					return;
				}	
			}
			
		}
		
		
		@Override
		public void run(Context context) throws IOException, InterruptedException {
		    setup(context);
		    userInfo = new UserInfoWritable();
		    while (context.nextKeyValue()) {
		       map(context.getCurrentKey(), context.getCurrentValue(), context);
		       if (userInfo.getBirth()!=null){
		    	   break;
		       }
		    }
		    if (!this.nickName.toString().equals("")){
		    	context.write(nickName, userInfo);
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
		String[] args1 = {"/input", "/output5"};
		runMapReduce(args1);
//		runMapReduce(args);
	}
}
