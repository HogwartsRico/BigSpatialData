/**   
* @Title: TestPhoenix.java 
* @Package edu.jxust.BigSpatialData 
* @Description: TODO 
* @author 张炫铤  
* @date 2017年2月15日 下午4:14:34 
* @version V1.0   
*/
package edu.jxust.BigSpatialData;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;

import org.apache.log4j.BasicConfigurator;

import edu.jxust.Indexing.Grid;

/** 
* @ClassName: TestPhoenix 
* @Description: TODO
* @author 张炫铤
* @date 2017年2月15日 下午4:14:34 
*  
*/
public class TestPhoenix {

	/** 
	* @Title: main 
	* @Description: TODO
	* @param args
	* @throws 
	*/
	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		Statement stmt = null;
		ResultSet rset = null;

		// Connection con =
		// DriverManager.getConnection("jdbc:phoenix:[192.168.128.1]");
		Connection con = GetConnection();
		long startTime = System.currentTimeMillis();

		stmt = con.createStatement();

		// stmt.executeUpdate("create table test (mykey integer not null primary
		// key, mycolumn varchar)");
		stmt.executeUpdate("upsert into TEST_PHOENIX1 values ('6','Hello','wer')");
		stmt.executeUpdate("upsert into TEST_PHOENIX1 values ('7','World!','tyef')");
		con.commit();

		PreparedStatement statement = con.prepareStatement("select * from TEST_PHOENIX1");
		rset = statement.executeQuery();
		while (rset.next()) {
			System.out.println(String.format("%s %s", rset.getString("mycolumn"), rset.getString("DEPT_NAME")));//
		}
		long endTime = System.currentTimeMillis();
		System.out.println("查询时间：" + (endTime - startTime));
		statement.close();
		con.close();

	}

	public static Connection GetConnection() {
		Connection cc = null;
		String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
		String url = "jdbc:phoenix:192.168.128.1";

		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		if (cc == null) {
			try {
				cc = DriverManager.getConnection(url);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return cc;
	}

}
