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
		int followNum = -1;  // 关注人数
		int fanNum = -1;  // 粉丝人数
		int blogNum = -1;  // 发布的微博数
		int isV = -1;  // 是否微博认证 0表示否 1表示是
		int forwardNum = 0;  // 首页转发数
		int commentNum = 0;  // 首页评论数
		int likeNum = 0;  // 首页点赞数

		public UserInfoWritable() {}
		
	    public UserInfoWritable(int followNum, int fanNum, int blogNum, int isCertification, int forwardNum,
				int commentNum, int likeNum) {
			this.followNum = followNum;
			this.fanNum = fanNum;
			this.blogNum = blogNum;
			this.isV = isCertification;
			this.forwardNum = forwardNum;
			this.commentNum = commentNum;
			this.likeNum = likeNum;
		}
	    
	    @Override
	    public void readFields(DataInput in) throws IOException {
	    	this.followNum = in.readInt();
	    	this.fanNum = in.readInt();
	    	this.blogNum = in.readInt();
	    	this.isV = in.readInt();
	    	this.forwardNum = in.readInt();
	    	this.commentNum = in.readInt();
	    	this.likeNum = in.readInt();
	    }

	    @Override 
	    public void write(DataOutput out) throws IOException {
	    	out.writeInt(followNum);
	    	out.writeInt(fanNum);
	    	out.writeInt(blogNum);
	    	out.writeInt(isV);
	    	out.writeInt(forwardNum);
	    	out.writeInt(commentNum);
	    	out.writeInt(likeNum);
	    }

	    @Override
	    public String toString() {
	        return this.followNum + "\t" + this.fanNum + "\t" +  this.blogNum + "\t" + 
	        		this.isV + "\t" + this.forwardNum + "\t" + this.commentNum + "\t" + this.likeNum;
	    }
	}

	public static class InfoWritable implements Writable{	
		int typeNum = -1;  // 匹配信息的类别 0:用户id 1:关注的人数 2:粉丝数 3:发布的微博数 4:是否微博认证 
							// 5:首页转发数 6:首页评论数 7:首页点赞数
		int matchInfo = -1;
		
		public InfoWritable(){}
		public InfoWritable(int typeNum, int matchInfo) {
			this.typeNum = typeNum;
			this.matchInfo = matchInfo;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
	    	this.typeNum = in.readInt();
	    	this.matchInfo = in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException {
	    	out.writeInt(typeNum);
	    	out.writeInt(matchInfo);
		}
		
		@Override
	    public String toString() {
	        return this.typeNum + "\t" + this.matchInfo;
	    }
	}
	
	public static class UserInfoMapperII extends Mapper<Object, Text, Text, InfoWritable>{
		private Text uid = new Text();
		Pattern userInfoClassPattern = Pattern.compile("<div class=\"u\">(.*?)</div>");
		Pattern uidPattern = Pattern.compile("uid=(\\d{10})");
		Pattern followNumPattern = Pattern.compile("关注\\[(\\d{1,8})\\]");
		Pattern fanNumPattern = Pattern.compile("粉丝\\[(\\d{1,8})\\]");
		Pattern blogNumPattern = Pattern.compile("微博\\[(\\d{1,8})\\]");
		Pattern vPattern = Pattern.compile("alt=\"(\\w)\"");
		Pattern forwardPattern = Pattern.compile("[^文]转发\\[(\\d{1,8})\\]");
		Pattern commentPattern = Pattern.compile("[^文]评论\\[(\\d{1,8})\\]");
		Pattern likePattern = Pattern.compile("赞\\[(\\d{1,8})\\]</[^sS]");
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String url = value.toString();
			System.out.println(url);
			String urlHead = null;
			Matcher matcher = userInfoClassPattern.matcher(url);
			if (!matcher.find()){
				return;
			}
			else{
				urlHead = matcher.group(1);
				if (urlHead.indexOf("送Ta会员")==-1){
					return;
				}
			}
			System.out.println(urlHead);
			matcher = uidPattern.matcher(url);
			if (matcher.find()){
				uid.set(matcher.group(1));
				System.out.println("uid:" + uid.toString());
			}
			else{
				System.out.println("无法匹配uid");
			}
			matcher = followNumPattern.matcher(url);
			if (matcher.find()){
				System.out.println("follow:" + matcher.group(1));
				context.write(uid, new InfoWritable(1, Integer.parseInt(matcher.group(1))));
			}
			else{
				System.out.println("无法匹配关注人数");
			}
			
			matcher = fanNumPattern.matcher(url);
			if (matcher.find()){
				System.out.println("fan:" + matcher.group(1));
				context.write(uid, new InfoWritable(2, Integer.parseInt(matcher.group(1))));
			}
			else{
				System.out.println("无法匹配粉丝人数");
			}
			
			matcher = blogNumPattern.matcher(url);
			if (matcher.find()){
				System.out.println("blog:" + matcher.group(1));
				context.write(uid, new InfoWritable(3, Integer.parseInt(matcher.group(1))));
			}
			else{
				System.out.println("无法匹配微博个数");
			}
			
			matcher = vPattern.matcher(urlHead);
			InfoWritable iw = new InfoWritable(4, 0);
			while (matcher.find()){
				if (matcher.group(1).equals("v") || matcher.group(1).equals("V")){
					iw.matchInfo = 1;
					break;
				}
			}
			context.write(uid, iw);
			
			matcher = forwardPattern.matcher(url);
			while(matcher.find()){
				context.write(uid, new InfoWritable(5, Integer.parseInt(matcher.group(1))));
			}
			
			matcher = commentPattern.matcher(url);
			while(matcher.find()){
				context.write(uid, new InfoWritable(6, Integer.parseInt(matcher.group(1))));
			}
			
			matcher = likePattern.matcher(url);
			while(matcher.find()){
				context.write(uid, new InfoWritable(7, Integer.parseInt(matcher.group(1))));
			}
		}
	}
	
	public static class UserInfoReducerII extends Reducer<Text, InfoWritable, Text, UserInfoWritable> {
		public void reduce(Text uid, Iterable<InfoWritable> infos, Context context)
				throws IOException, InterruptedException {
			UserInfoWritable uiw = new UserInfoWritable();
			for (InfoWritable info : infos) {
				if (info.typeNum == 1){
					uiw.followNum = info.matchInfo;
				}
				else if(info.typeNum == 2){
					uiw.fanNum = info.matchInfo;
				}
				else if(info.typeNum == 3){
					uiw.blogNum = info.matchInfo;
				}
				else if(info.typeNum == 4){
					uiw.isV = info.matchInfo;
				}
				else if(info.typeNum == 5){
					uiw.forwardNum += info.matchInfo;
				}
				else if(info.typeNum == 6){
					uiw.commentNum += info.matchInfo;
				}
				else if(info.typeNum == 7){
					uiw.likeNum += info.matchInfo;
				}
			}
			context.write(uid, uiw);
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
		job.setJarByClass(UserInfoCollect.class);
		job.setMapperClass(UserInfoMapperII.class);
		job.setReducerClass(UserInfoReducerII.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(InfoWritable.class);
		FileInputFormat.setInputDirRecursive(job, true);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	public static void main(String[] args) throws Exception{
		String[] args1 = {"/weibo_simple", "/output_weibo_s1"};
		runMapReduce(args1);
//		runMapReduce(args);
	}
}
