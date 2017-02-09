package edu.jxust.Common;

public class Data {

	/*
	 * String转Integer，超过整型最大值，则返回整型最大值 2147483647；
	 * 小于整型最小值，则取整型最小值 -2147483648 
	 * 
	 * @return 转换后数据
	 * 
	 * @author 张炫铤
	 * 
	 */
	public static Integer ConvertToInt(String str) throws NumberFormatException {
		if (str == null) {
			throw new NumberFormatException("null");
		}

		long pre_foo = Long.parseLong(str);
		if (pre_foo < Integer.MIN_VALUE) {
			return Integer.MIN_VALUE;
		} else if (pre_foo > Integer.MAX_VALUE) {
			return Integer.MAX_VALUE;
		} else {
			return (int) pre_foo;
		}
	}
}
