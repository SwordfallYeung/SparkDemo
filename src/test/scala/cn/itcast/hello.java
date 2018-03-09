package cn.itcast;

/**
 * @author y15079
 * @create 2018-03-08 14:17
 * @desc
 **/
public class hello {
	public static void main(String[] args) {
		boolean a = isValidInt("1");
		System.out.println(a);
	}

	public static boolean isValidInt(String str){
		boolean convertSuccess=true;

		try {
			Integer.parseInt(str);
		} catch (Exception e) {
			// 如果throw java.text.ParseException或者NullPointerException，就说明格式不对，不是Int类型的
			convertSuccess=false;
		}
		return convertSuccess;
	}
}
