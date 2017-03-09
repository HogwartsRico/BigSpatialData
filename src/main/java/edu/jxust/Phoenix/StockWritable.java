/**   
* @Title: StockWritable1.java 
* @Package edu.jxust.Phoenix 
* @Description: TODO 
* @author 张炫铤  
* @date 2017年3月7日 下午1:07:11 
* @version V1.0   
*/
package edu.jxust.Phoenix;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/** 
* @ClassName: StockWritable1 
* @Description: TODO
* @author 张炫铤
* @date 2017年3月7日 下午1:07:11 
*  
*/
public class StockWritable implements DBWritable, Writable {
	private String stockName;

	public String getStockName() {
		return this.stockName;
	}

	public void setStockName(String stockName) {
		this.stockName = stockName;
	}

	private int year;

	public int getYear() {
		return this.year;
	}

	private double[] recordings;

	public double[] getRecordings() {
		return this.recordings;
	}

	private double maxPrice;

	public void setMaxPrice(double maxPrice) {
		this.maxPrice = maxPrice;
	}

	/*
	 * Title: readFields Description:
	 * 
	 * @param arg0
	 * 
	 * @throws IOException
	 * 
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput arg0) throws IOException {

	}

	/*
	 * Title: write Description:
	 * 
	 * @param arg0
	 * 
	 * @throws IOException
	 * 
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub

	}

	/*
	 * Title: readFields Description:
	 * 
	 * @param arg0
	 * 
	 * @throws SQLException
	 * 
	 * @see org.apache.hadoop.mapreduce.lib.db.DBWritable#readFields(java.sql.
	 * ResultSet)
	 */
	@Override
	public void readFields(ResultSet rs) throws SQLException {
		stockName = rs.getString("STOCK_NAME");
		year = rs.getInt("RECORDING_YEAR");
		final Array recordingsArray = rs.getArray("RECORDINGS_QUARTER");
		recordings = (double[]) recordingsArray.getArray();

	}

	/*
	 * Title: write Description:
	 * 
	 * @param arg0
	 * 
	 * @throws SQLException
	 * 
	 * @see org.apache.hadoop.mapreduce.lib.db.DBWritable#write(java.sql.
	 * PreparedStatement)
	 */
	@Override
	public void write(PreparedStatement pstmt) throws SQLException {
		pstmt.setString(1, stockName);
		pstmt.setDouble(2, maxPrice);

	}

}
