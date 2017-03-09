/**   
* @Title: QueryKey.java 
* @Package edu.jxust.Common 
* @Description: TODO 
* @author 张炫铤  
* @date 2017年3月9日 下午1:52:24 
* @version V1.0   
*/
package edu.jxust.Common;

/** 
* @ClassName: QueryKey 
* @Description: TODO
* @author 张炫铤
* @date 2017年3月9日 下午1:52:24 
*  
*/
public class QueryRowKey {
	String startRow;
	String stopRow;

	/** 
	* <p>Title: </p> 
	* <p>Description: </p> 
	* @param startRow
	* @param stopRow 
	*/
	public QueryRowKey(String startRow, String stopRow) {
		this.startRow = startRow;
		this.stopRow = stopRow;
	}

	public String getStartRow() {
		return this.startRow;
	}

	public void setStartRow(String startRow) {
		this.startRow = startRow;
	}

	public String getStopRow() {
		return this.stopRow;
	}

	public void setStopRow(String stopRow) {
		this.stopRow = stopRow;
	}
}
