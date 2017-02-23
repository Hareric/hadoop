package weibo;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;



public class UserInfoMultipleCollect2 {
	List<Path> errorPath = new ArrayList<Path>();
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
	
	public static class UserInfoWritableII implements Writable{
		
		String sex = null;
		String area = null;
		String birth = null;
		
	    public UserInfoWritableII() {}

	    public UserInfoWritableII(String sex, String area, String birth) {
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
	public static class InfoWritable implements Writable{	
		int typeNum = -1;  // 匹配信息的类别 0:用户id 1:关注的人数 2:粉丝数 3:发布的微博数 4:是否微博认证 
							// 5:首页转发数 6:首页评论数 7:首页点赞数
							// 8:性别 9:地区 10:生日
		String matchInfo = null;
		
		public InfoWritable(){}
		public InfoWritable(int typeNum, String matchInfo) {
			this.typeNum = typeNum;
			this.matchInfo = matchInfo;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
	    	this.typeNum = in.readInt();
	    	this.matchInfo = WritableUtils.readString(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
	    	out.writeInt(typeNum);
	    	WritableUtils.writeString(out, matchInfo);
		}
		
		@Override
	    public String toString() {
	        return this.typeNum + "\t" + this.matchInfo;
	    }
	}
	
	
	public static class UserInfoMapper extends Mapper<Object, Text, Text, InfoWritable>{
		
		
		public void mapForMain(Object key, Text value, Context context) throws IOException, InterruptedException{
			Text uid = new Text();
			Pattern userInfoClassPattern = Pattern.compile("<div class=\"u\">(.*?)</div>");
			Pattern uidPattern = Pattern.compile("uid=(\\d{10})");
			Pattern followNumPattern = Pattern.compile("关注\\[(\\d{1,8})\\]");
			Pattern fanNumPattern = Pattern.compile("粉丝\\[(\\d{1,8})\\]");
			Pattern blogNumPattern = Pattern.compile("微博\\[(\\d{1,8})\\]");
			Pattern vPattern = Pattern.compile("alt=\"(\\w)\"");
			Pattern forwardPattern = Pattern.compile("[^文]转发\\[(\\d{1,8})\\]");
			Pattern commentPattern = Pattern.compile("[^文]评论\\[(\\d{1,8})\\]");
			Pattern likePattern = Pattern.compile("赞\\[(\\d{1,8})\\]</[^sS]");
			String url = value.toString();
			String urlHead = null;
			Matcher matcher = userInfoClassPattern.matcher(url);
			if (!matcher.find()){
				return;
			}
			else{
				urlHead = matcher.group(1);
			}
//			matcher = uidPattern.matcher(url);
//			if (matcher.find()){
//				uid.set(matcher.group(1));
//				System.out.println("uid:" + uid.toString());
//			}
//			else{
//				return;
//			}
			String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
			uid.set((fileName.substring(0, fileName.length()-5)));
			matcher = followNumPattern.matcher(url);
			if (matcher.find()){
				System.out.println("follow:" + matcher.group(1));
				context.write(uid, new InfoWritable(1, matcher.group(1)));
			}
			else{
				System.out.println("无法匹配关注人数");
			}
			
			matcher = fanNumPattern.matcher(url);
			if (matcher.find()){
				System.out.println("fan:" + matcher.group(1));
				context.write(uid, new InfoWritable(2, matcher.group(1)));
			}
			else{
				System.out.println("无法匹配粉丝人数");
			}
			
			matcher = blogNumPattern.matcher(url);
			if (matcher.find()){
				System.out.println("blog:" + matcher.group(1));
				context.write(uid, new InfoWritable(3, matcher.group(1)));
			}
			else{
				System.out.println("无法匹配微博个数");
			}
			
			matcher = vPattern.matcher(urlHead);
			InfoWritable iw = new InfoWritable(4, "0");
			while (matcher.find()){
				if (matcher.group(1).equals("v") || matcher.group(1).equals("V")){
					iw.matchInfo = "1";
					break;
				}
			}
			context.write(uid, iw);
			
			matcher = forwardPattern.matcher(url);
			while(matcher.find()){
				context.write(uid, new InfoWritable(5, matcher.group(1)));
			}
			
			matcher = commentPattern.matcher(url);
			while(matcher.find()){
				context.write(uid, new InfoWritable(6, matcher.group(1)));
			}
			
			matcher = likePattern.matcher(url);
			while(matcher.find()){
				context.write(uid, new InfoWritable(7, matcher.group(1)));
			}
		}
		
		public void mapForInformation(Object key, Text value, Context context) throws IOException, InterruptedException{
			Text uid = new Text();
			Pattern uidPattern = Pattern.compile("uid=(\\d{10})");
			Pattern sexPattern = Pattern.compile("<br />性别:(.{1,2}?)<");
			Pattern areaPattern = Pattern.compile("<br />地区:(.{1,15}?)<");
			Pattern birthPattern = Pattern.compile("<br />生日:(.{1,10}?)<");
			String url = value.toString();
					
			Matcher matcher;
			matcher = uidPattern.matcher(url);
			if (matcher.find()){
				uid.set(matcher.group(1));
				System.out.println(matcher.group(1));
			}
			else{
				return;
			}
			
			
			matcher = sexPattern.matcher(url);
			if (matcher.find()){
				System.out.println(matcher.group(1));
				context.write(uid, new InfoWritable(8, matcher.group(1)));
			}
			else{
				System.out.println("无法匹配性别");
			}
			
			matcher = areaPattern.matcher(url);
			if (matcher.find()){
				System.out.println(matcher.group(1));
				context.write(uid, new InfoWritable(9, matcher.group(1)));
			}
			else{
				System.out.println("无法匹配地区");
			}
				
			matcher = birthPattern.matcher(url);
			if (matcher.find()){
				System.out.println(matcher.group(1));
				context.write(uid, new InfoWritable(10, matcher.group(1)));
			}
			else{
				System.out.println("无法匹配生日");
			}
		}
		
		
		public void run(Context context) throws IOException, InterruptedException {
		    setup(context);
		    Path path = ((FileSplit) context.getInputSplit()).getPath();
		    System.out.println(path);
		    while (context.nextKeyValue()) {    	
			       this.mapForMain(context.getCurrentKey(), context.getCurrentValue(), context);
			}   	    
		    cleanup(context);
		}
	}
	
		
	public static class UserInfoReducer extends Reducer<Text, InfoWritable, Text, Writable> {
		private MultipleOutputs output;

		@Override
		public void setup(Context context) {
			output = new MultipleOutputs(context);
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			output.close();
		}
		public void reduce(Text uid, Iterable<InfoWritable> infos, Context context)
				throws IOException, InterruptedException {
			UserInfoWritable uiw = new UserInfoWritable();
			UserInfoWritableII uiwII = new UserInfoWritableII();
			for (InfoWritable info : infos) {
				if (info.typeNum == 1){
					uiw.followNum = Integer.parseInt(info.matchInfo);
				}
				else if(info.typeNum == 2){
					uiw.fanNum = Integer.parseInt(info.matchInfo);
				}
				else if(info.typeNum == 3){
					uiw.blogNum = Integer.parseInt(info.matchInfo);
				}
				else if(info.typeNum == 4){
					uiw.isV = Integer.parseInt(info.matchInfo);
				}
				else if(info.typeNum == 5){
					uiw.forwardNum += Integer.parseInt(info.matchInfo);
				}
				else if(info.typeNum == 6){
					uiw.commentNum += Integer.parseInt(info.matchInfo);
				}
				else if(info.typeNum == 7){
					uiw.likeNum += Integer.parseInt(info.matchInfo);
				}
				else if(info.typeNum == 8){
					uiwII.sex = info.matchInfo;
				}
				else if(info.typeNum == 9){
					uiwII.area = info.matchInfo;
				}
				else if(info.typeNum == 10){
					uiwII.birth = info.matchInfo;
				}
			}
			this.output.write("UserInfoI", uid, uiw);
			this.output.write("UserInfoII", uid, uiwII);
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
		job.setJarByClass(UserInfoMultipleCollect2.class);
		job.setMapperClass(UserInfoMapper.class);
		job.setReducerClass(UserInfoReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(InfoWritable.class);
		FileInputFormat.setInputDirRecursive(job, true);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		MultipleOutputs.addNamedOutput(job, "UserInfoI", TextOutputFormat.class, Text.class, UserInfoWritable.class );
        MultipleOutputs.addNamedOutput(job, "UserInfoII", TextOutputFormat.class, Text.class, UserInfoWritableII.class );
 
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	public static void main(String[] args) throws Exception{
		String[] args1 = {"/gbdt_train_data", "/output_gbdt_train_data"};
		runMapReduce(args1);
//		runMapReduce(args);
	}
}
