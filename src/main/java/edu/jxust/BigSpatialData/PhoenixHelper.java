/**   
* @Title: PhoenixHelper.java 
* @Package edu.jxust.BigSpatialData 
* @Description: TODO 
* @author 张炫铤  
* @date 2017年2月15日 下午10:23:55 
* @version V1.0   
*/
package edu.jxust.BigSpatialData;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** 
* @ClassName: PhoenixHelper 
* @Description: TODO
* @author 张炫铤
* @date 2017年2月15日 下午10:23:55 
*  
*/
public class PhoenixHelper {
	Connection connection;

	public PhoenixHelper(String host) {
		this.connection = GetConnection(host);
	}

	public Connection GetConnection(String host) {
		Connection connection = null;
		String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
		String url = String.format("jdbc:phoenix:%s", host);// 192.168.128.1

		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		if (connection == null) {
			try {
				connection = DriverManager.getConnection(url);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return connection;
	}

	public void close() {
		try {
			this.connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
