/**   
* @Title: StockWritable.java 
* @Package edu.jxust.Phoenix 
* @Description: TODO 
* @author 张炫铤  
* @date 2017年3月6日 下午5:11:02 
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
* @ClassName: StockWritable 
* @Description: TODO
* @author 张炫铤
* @date 2017年3月6日 下午5:11:02 
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

	private double[] recordings;

	public double[] getRecordings() {
		return this.recordings;
	}

	private double maxPrice;

	public void setMaxPrice(double maxPrice) {
		this.maxPrice = maxPrice;
	}

	@Override
	public void readFields(DataInput input) throws IOException {

	}

	@Override
	public void write(DataOutput output) throws IOException {

	}

	@Override
	public void readFields(ResultSet rs) throws SQLException {
		stockName = rs.getString("STOCK_NAME");
		year = rs.getInt("RECORDING_YEAR");
		final Array recordingsArray = rs.getArray("RECORDINGS_QUARTER");
		recordings = (double[]) recordingsArray.getArray();
	}

	@Override
	public void write(PreparedStatement pstmt) throws SQLException {
		pstmt.setString(1, stockName);
		pstmt.setDouble(2, maxPrice);
	}

}
