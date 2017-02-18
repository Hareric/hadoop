package tool;

import java.util.regex.*;


public class Draft {
	public static void main(String[] args){
		Pattern userInfoClassPattern = Pattern.compile("<div class=\"u\">(.*?)</div>");
		Pattern uidPattern = Pattern.compile("uid=(\\d{10})");
		Pattern followNumPattern = Pattern.compile("关注[(\\d{1,8})]");
		Pattern fanNumPattern = Pattern.compile("粉丝[(\\d{1,8})]");
		Pattern blogNumPattern = Pattern.compile("微博[(\\d{1,8})]");
		Pattern vPattern = Pattern.compile("alt=\"(\\w)\"");
		Pattern forwardPattern = Pattern.compile("转发[(\\d{1,8})]");
		Pattern commentPattern = Pattern.compile("评论[(\\d{1,8})]");
		Pattern likePattern = Pattern.compile("赞[(\\d{1,8})]");
		int a = Integer.parseInt("5514");
		
		String s = "uid=1231231231 uid=1261231231 ";
		
		Matcher matcher = uidPattern.matcher(s);
//		while (matcher.find()){
//			String term = matcher.group(1);
//			System.out.println(term);
//		}
		matcher.find();
		String term = matcher.group(1);
		System.out.println(term);
		matcher.find();
		 term = matcher.group(1);
		System.out.println(a);
		
	}
}
