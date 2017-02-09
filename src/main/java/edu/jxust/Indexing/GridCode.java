/**   
* @Title: HilbertCode.java 
* @Package edu.jxust.Indexing 
* @Description: TODO 
* @author 张炫铤  
* @date 2017年1月8日 下午4:43:06 
* @version V1.0   
*/
package edu.jxust.Indexing;

import org.apache.commons.lang3.StringUtils;

import com.vividsolutions.jts.geom.Coordinate;

import edu.jxust.Common.Hilbert2D;

/** 
* @ClassName: HilbertCode 
* @Description: Hilbert曲线编码
* @author 张炫铤
* @date 2017年1月8日 下午4:43:06 
*  
*/
public class GridCode {

	/** 
	* @Title: getHilbertCode 
	* @Description: 获取Hilbert曲线编码，该编码方式是：“层级号”_“Hilbert曲线四进制编码”
	* @param gridLevel
	* @param gridCoor
	* @return
	* @throws 
	*/
	public static String getHilbertCode(Integer gridLevel, Coordinate gridCoor){
		Hilbert2D h=new Hilbert2D();
		String s = StringUtils.leftPad(gridLevel.toString(), 2, '0');
		return s+"_0"+hilbertEncode(h.xy2d((int)gridCoor.x,(int)gridCoor.y ,gridLevel),gridLevel);
	}
	
	/** 
	* @Title: getHilbertEncode 
	* @Description: 获取Hilbert曲线编码，该编码方式不带层级号，即：“Hilbert曲线四进制编码”
	* @param gridLevel
	* @param gridCoor
	* @return
	* @throws 
	*/
	public static String getHilbertEncode(Integer gridLevel, Coordinate gridCoor){
		Hilbert2D h=new Hilbert2D();		
		return "0"+hilbertEncode(h.xy2d((int)gridCoor.x,(int)gridCoor.y ,gridLevel),gridLevel);
	}
	private static String hilbertEncode(int num, int n) {
		n=n*2;
		String s = StringUtils.leftPad(Integer.toBinaryString(num), n, '0');	
		char[] chars = s.toCharArray();
		
		String code = "";
		
		for (int i = 0; i < n; i = i + 2) {
			String c = String.valueOf(chars[i]) + String.valueOf(chars[i + 1]);
			code += Integer.valueOf(c, 2).toString();
		}
		return code;
		
	}
	
}
