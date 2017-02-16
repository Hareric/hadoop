package Weibo;

import org.apache.hadoop.io.Text;

public class Draft {
	public static void main(String[] args){
		Text nickName = new Text();
		if (nickName.toString().equals("")){
			System.out.println("null");
		}
	}
}
