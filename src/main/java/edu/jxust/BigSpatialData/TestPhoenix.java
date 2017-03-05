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

import org.apache.log4j.Logger;

import edu.jxust.Common.DBUtil;
import edu.jxust.Indexing.Grid;
import edu.jxust.hadoop.MaxTemperature;

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
//		BasicConfigurator.configure();
		Logger.getLogger(TestPhoenix.class);
		String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
		Class.forName(driver);
		Statement stmt = null;
		ResultSet rset = null;

		// Connection con =
		// DriverManager.getConnection("jdbc:phoenix:[192.168.128.1]");
		Connection con = DBUtil.getConnection();
		long startTime = System.currentTimeMillis();

		 stmt = con.createStatement();
		 PreparedStatement ps =con.prepareStatement("upsert into TEST_PHOENIX (mykey,mycolumn,test) values (?,?,?)");
		 PreparedStatement ps2 =con.prepareStatement("upsert into TEST_PHOENIX1 (mykey,mycolumn,dept_name) values (?,?,?)");
		 for(int i=600;i<800;i++){
             ps.setInt(1, i);
             ps.setString(2, String.format("Hello NO.%s ",i));
             ps.setBytes(3,   Integer.toString(i).getBytes());             
             ps.addBatch();
             
             ps2.setString(1, Integer.toString(i));
             ps2.setString(2, String.format("Hello NO.%s ",i));
             ps2.setString(3,   Integer.toString(i));             
             ps2.addBatch();
             if(i%100==0){
                 ps.executeBatch();
                 ps2.executeBatch();
             }
         }
		 ps.executeBatch();
         ps2.executeBatch();
		 ps.close();
		 ps2.close();
		 
//		 ps.setInt(1, 4);;
//		 ps.setBytes(2, "4".getBytes());
//		 ps.executeUpdate();
		 //stmt.executeUpdate("create table test (mykey integer not null primarykey, mycolumn varchar)");
		
		 
		 //stmt.executeUpdate(String.format("upsert into TEST_PHOENIX (mykey,test) values (3,'%s')",new String("6".getBytes())));
		 //stmt.executeUpdate(String.format("upsert into TEST_PHOENIX (mykey,test) values (4,'%s')","7".getBytes()));
		 con.commit();

		PreparedStatement statement = con.prepareStatement("select * from TEST_PHOENIX");
		rset = statement.executeQuery();
		int count=0;
		while (rset.next()) {
			count++;
			if(count>=10)
				break;
			System.out.println(String.format("%s %s", rset.getString("mycolumn"), new String(rset.getBytes("test"))));//
		}
		long endTime = System.currentTimeMillis();
		System.out.println("查询时间：" + (endTime - startTime));
		statement.close();
		con.close();

	}

	public static Connection GetConnection() {
		Connection cc = null;
		String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
		String url = "jdbc:phoenix:192.168.128.1:longRunning";//长时间连接

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
