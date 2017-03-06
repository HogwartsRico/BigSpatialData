/**   
* @Title: DBUtil.java 
* @Package edu.jxust.Common 
* @Description: TODO 
* @author 张炫铤  
* @date 2017年2月16日 上午9:30:20 
* @version V1.0   
*/
package edu.jxust.Common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.InputStream;

import java.util.Properties;

/** 
* @ClassName: DBUtil 
* @Description: jdbc数据操作类
* @author 张炫铤
* @date 2017年2月16日 上午9:30:20 
*  
*/
public class DBUtil {
	// 定义链接所需要的变量
	private static Connection con = null;
	// private static PreparedStatement ps = null;
	// private static ResultSet rs = null;

	// 定义链接数据库所需要的参数
	private static String url = "";
	private static String username = "";
	private static String driver = "";
	private static String password = "";
	private static Boolean isLongTimeRunning = false;
	// 定义读取配置文件所需要的变量
	private static Properties dbProperties = null;
	// private static InputStream fis = null;

	/** 
	 * 加载驱动 
	 */
	static {
		InputStream fis = null;
		try {
			// 从dbinfo.properties配置文件中读取配置信息
			dbProperties = new Properties();
			// fis =
			// DBUtil.class.getResourceAsStream("/DataBase.properties");//src/main
			fis = DBUtil.class.getClassLoader().getResourceAsStream("DataBase.properties");// src/main
			// DBUtil.class.getClassLoader()
			dbProperties.load(fis);
			url = dbProperties.getProperty("url");
			username = dbProperties.getProperty("username");
			driver = dbProperties.getProperty("driver").trim();
			password = dbProperties.getProperty("password");
			isLongTimeRunning = Boolean.valueOf(dbProperties.getProperty("longRunning"));
			Class.forName(driver);
		} catch (Exception e) {
			System.out.println("驱动加载失败！");
			e.printStackTrace();
		} finally {
			try {
				fis.close();
			} catch (Exception e) {
				e.printStackTrace();
			}

			fis = null; // 垃圾回收自动处理
		}

	}

	/** 
	 * 得到Connection链接 
	 * @return Connection 
	 */
	public static Connection getConnection() {

		try {
			// 建立连接
			if (isLongTimeRunning) {
				url = String.format("%s:longRunning", url);
			} else {
				url = String.format("%s:shortRunning", url);
			}

			con = DriverManager.getConnection(url, username, password);

		} catch (Exception e) {
			System.out.println("数据库链接失败！");
			e.printStackTrace();
		}

		return con;
	}
	
	public static Connection getDefaultConnection() {
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

	/** 
	 * 统一的资源关闭函数 
	 * @param rs 
	 * @param ps 
	 * @param ct 
	 */
	public static void close(ResultSet rs, Statement ps, Connection con) {

		if (rs != null) {
			try {
				rs.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		if (ps != null) {
			try {
				ps.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		if (con != null) {
			try {
				con.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void close(ResultSet rs, PreparedStatement ps, Connection con) {

		if (rs != null) {
			try {
				rs.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		if (ps != null) {
			try {
				ps.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		if (con != null) {
			try {
				con.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
