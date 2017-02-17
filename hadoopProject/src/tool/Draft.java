package tool;

import java.util.regex.*;


public class Draft {
	public static void main(String[] args){
		String s = "uid=1231231231 uid=1261231231 ";
		
		Pattern uidPattern = Pattern.compile("uid=(\\d{10})");
		Matcher matcher = uidPattern.matcher(s);
		while (matcher.find()){
			String term = matcher.group(1);
			System.out.println(term);
		}
		
	}
}
