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


public class UserInfoCollect {
	public static class UserInfoWritable implements Writable {
		String uid = null;  // 用户id
		int followNum = -1;  // 关注人数
		int fanNum = -1;  // 粉丝人数
		int blogNum = -1;  // 发布的微博数
		int isCertification = -1;  // 是否微博认证 0表示否 1表示是
		int forwardNum = -1;  // 首页转发数
		int commentNum = -1;  // 首页评论数
		int likeNum = -1;  // 首页点赞数
		int isV = -1;  // 是否为大V 0表示否 1表示是
		public UserInfoWritable() {}
		
	    public UserInfoWritable(String uid, int followNum, int fanNum, int blogNum, int isCertification, int forwardNum,
				int commentNum, int likeNum) {
			super();
			this.uid = uid;
			this.followNum = followNum;
			this.fanNum = fanNum;
			this.blogNum = blogNum;
			this.isCertification = isCertification;
			this.forwardNum = forwardNum;
			this.commentNum = commentNum;
			this.likeNum = likeNum;
		}
	    
	    @Override
	    public void readFields(DataInput in) throws IOException {
	    	this.uid = WritableUtils.readString(in);
	    	this.followNum = WritableUtils.readVInt(in);
	    	this.fanNum = WritableUtils.readVInt(in);
	    	this.blogNum = WritableUtils.readVInt(in);
	    	this.isCertification = WritableUtils.readVInt(in);
	    	this.forwardNum = WritableUtils.readVInt(in);
	    	this.commentNum = WritableUtils.readVInt(in);
	    	this.likeNum = WritableUtils.readVInt(in);
	    }

	    @Override 
	    public void write(DataOutput out) throws IOException {
	    	WritableUtils.writeString(out, this.uid);
	    	WritableUtils.writeVInt(out, this.followNum);
	    	WritableUtils.writeVInt(out, this.fanNum);
	    	WritableUtils.writeVInt(out, this.blogNum);
	    	WritableUtils.writeVInt(out, this.isCertification);
	    	WritableUtils.writeVInt(out, this.forwardNum);
	    	WritableUtils.writeVInt(out, this.commentNum);
	    	WritableUtils.writeVInt(out, this.likeNum);
	    }

	    @Override
	    public String toString() {
	        return this.uid + "\t" + this.followNum + "\t" + this.fanNum + "\t" +  this.blogNum + 
	        		this.isCertification + "\t" + this.forwardNum + "\t" + this.commentNum + "\t" + this.likeNum;
	    }
	}

	public static class InfoWritable implements Writable{	
		int typeNum = -1;  // 匹配信息的类别 0:用户id 1:关注的人数 2:粉丝数 3:发布的微博数 4:是否微博认证 
							// 5:首页转发数 6:首页评论数 7:首页点赞数 8:是否大V
		String matchInfo = null;
		
		
		public InfoWritable(int typeNum, String matchInfo) {
			super();
			this.typeNum = typeNum;
			this.matchInfo = matchInfo;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
	    	this.typeNum = WritableUtils.readVInt(in);
	    	this.matchInfo = WritableUtils.readString(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
	    	WritableUtils.writeVInt(out, typeNum);
	    	WritableUtils.writeString(out, matchInfo);		
		}
	}
	
	public static class UserInfoMapper extends Mapper<Object, Text, Text, UserInfoWritable>{
		private Text uid = new Text();
		Pattern uidPattern = Pattern.compile("uid=(\\d{10})");
		Pattern followNumPattern = Pattern.compile("关注[(\\d{1,8})]");
		Pattern fanNumPattern = Pattern.compile("粉丝[(\\d{1,8})]");
		Pattern blogNumPattern = Pattern.compile("微博[(\\d{1,8})]");
		Pattern userInfoClassPattern = Pattern.compile("<div class=\"u\">(.*?)</div>");
		
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
			
			if (userInfo.getSex().equals("")){
				matcher = sexPattern.matcher(url);
				if (matcher.find()){
					String term = matcher.group(1);
					userInfo.setSex(term);
					return;
				}
			}
			
			if (userInfo.getArea().equals("")){
				matcher = areaPattern.matcher(url);
				if (matcher.find()){
					String term = matcher.group(1);
					userInfo.setArea(term);
					return;
				}
			}
			
			if (userInfo.getBirth().equals("")){
				matcher = birthPattern.matcher(url);
				if (matcher.find()){
					String term = matcher.group(1);
					userInfo.setBirth(term);
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
		       if (!userInfo.getBirth().equals("")){
		    	   break;
		       }
		    }
		    context.write(nickName, userInfo);
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
		job.setJarByClass(UserInfoMatch.class);
		job.setMapperClass(UserInfoMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(UserInfoWritable.class);
		FileInputFormat.setInputDirRecursive(job, true);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	public static void main(String[] args) throws Exception{
		String[] args1 = {"/userInfo", "/output"};
		runMapReduce(args1);
//		runMapReduce(args);
	}
}
