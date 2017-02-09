/**   
* @Title: GridSize.java 
* @Package edu.jxust.Indexing 
* @Description: 网格长宽 
* @author 张炫铤  
* @date 2017年1月3日 下午3:33:08 
* @version V1.0   
*/
package edu.jxust.Indexing;

/** 
* @ClassName: GridSize 
* @Description: 网格长宽
* @author 张炫铤
* @date 2017年1月3日 下午3:33:08 
*  
*/
public class GridSize {
	private double breadth;
	private double length;

	public GridSize(int gridLevel) {
		breadth = calcBreadth(gridLevel);
		length = calcLength(gridLevel);
	}

	public GridSize(double length, double breadth) {
		this.breadth = breadth;
		this.length = length;
	}

	public double getBreadth() {
		return breadth;
	}

	protected void setLength(double length) {
		this.length = length;
	}

	protected void setBreadth(double breadth) {
		this.breadth = breadth;
	}

	public double getLength() {
		return length;
	}

	private double calcBreadth(int gridLevel) {
		return 180 / Math.pow(2, gridLevel);
	}

	private double calcLength(int gridLevel) {
		return 360 / Math.pow(2, gridLevel);
	}
	
//	private int getFactorialofTwo(int n){//n > 0  
//	    return 2 << (n-1);//2的n次方  
//	} 
}
