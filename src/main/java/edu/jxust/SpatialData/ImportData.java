/**   
* @Title: ImportData.java 
* @Package edu.jxust.SpatialData 
* @Description: 导入空间数据 
* @author 张炫铤  
* @date 2017年3月4日 下午9:35:45 
* @version V1.0   
*/
package edu.jxust.SpatialData;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/** 
* @ClassName: ImportData 
* @Description: 导入空间数据
* @author 张炫铤
* @date 2017年3月4日 下午9:35:45 
*  
*/
public class ImportData {
	private static Connection con = null;

	public ImportData(Connection connection) {
		con = connection;
	}

	public void insertData(Statement stmt, String table, String values) throws SQLException {
		String sql = String.format("upsert into %s values(%s)", table, values);
		stmt.executeUpdate(sql);
	}

	public void commit() throws SQLException {
		con.commit();
	}

	public void close() throws SQLException {
		if (con.isClosed() == false)
			con.close();
	}
}
