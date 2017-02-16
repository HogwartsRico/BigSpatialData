/**   
* @Title: HBaseHelper.java 
* @Package edu.jxust.Common 
* @Description: HBase操作帮助类
* @author 张炫铤  
* @date 2017年1月16日 下午9:13:23 
* @version V1.0   
*/
package edu.jxust.Common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

/** 
* @ClassName: HBaseHelper 
* @Description: HBase操作帮助类
* @author 张炫铤
* @date 2017年1月16日 下午9:13:23 
*  
*/
public class HBaseHelper {
	// private Configuration conf;// 配置器
	private HBaseAdmin admin;// HBase管理员
	// private HConnection connection;

	/** 
	* <p>Title:HBase操作帮助类 </p> 
	* <p>Description:HBase操作帮助类，默认主节点域名为master，连接端口为2181 </p> 
	* @throws IOException 
	*/
	public HBaseHelper() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "master");// 使用eclipse时必须添加这个，否则无法定位master需要配置hosts
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		this.admin = new HBaseAdmin(getConnection(conf));
		System.out.println("创建HBase配置成功！");
	}

	/** 
	 * 获取HBase配置器 
	 *  
	 * @param conf 
	 *            Hadoop配置器 
	 * @throws IOException 
	 */
	public HBaseHelper(Configuration conf) throws IOException {
		this.admin = new HBaseAdmin(getConnection(conf));
		System.out.println("创建HBase配置成功！");
	}

	/** 
	* <p>Title: </p> 
	* <p>Description: </p> 
	* @param masterHost hbase.zookeeper.quorum设置主节点，默认为master
	* @param clientPort hbase.zookeeper.property.clientPort端口，默认2181
	* @throws IOException 
	*/
	public HBaseHelper(String masterHost, String clientPort) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", masterHost);// 使用eclipse时必须添加这个，否则无法定位master需要配置hosts
		conf.set("hbase.zookeeper.property.clientPort", clientPort);
		conf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 600000);
		this.admin = new HBaseAdmin(getConnection(conf));
		System.out.println("创建HBase配置成功！");
	}

	/** 
	* @Title: getConfiguration 
	* @Description: 根据主节点域名和连接端口获取连接配置
	* @param masterHost 主节点域名
	* @param clientPort 连接端口
	* @return 连接配置
	* @throws 
	*/
	public static Configuration getConfiguration(String masterHost, String clientPort) {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", masterHost);// 使用eclipse时必须添加这个，否则无法定位master需要配置hosts
		conf.set("hbase.zookeeper.property.clientPort", clientPort);
		return conf;
	}

	public static HConnection getConnection(String masterHost, String clientPort) throws IOException {
		return HConnectionManager.createConnection(getConfiguration(masterHost, clientPort));
	}

	public static HConnection getConnection(Configuration conf) throws IOException {
		conf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 12000000);
		return HConnectionManager.createConnection(conf);
	}

	public HTableInterface getTable(String tableName) throws IOException {
		return this.admin.getConnection().getTable(tableName);
	}

	public Boolean tableExist(String tableName) throws IOException {
		return this.admin.tableExists(tableName);
	}

	/** 
	 * 创建HBase表 
	 *  
	 * @param tableName 
	 *            表名 
	 * @param colFamilies 
	 *            列簇 
	 * @throws IOException 
	 */
	public void createTable(String tableName, String colFamilies[]) throws IOException {
		if (this.admin.tableExists(tableName)) {
			System.out.println("Table: " + tableName + " 已存在 !");
		} else {
			HTableDescriptor dsc = new HTableDescriptor(TableName.valueOf(tableName));
			int len = colFamilies.length;
			for (int i = 0; i < len; i++) {
				HColumnDescriptor family = new HColumnDescriptor(colFamilies[i]);
				dsc.addFamily(family);
			}
			admin.createTable(dsc);
			System.out.println("创建表" + tableName + "成功");
		}

	}

	/** 
	 * 删除表 
	 *  
	 * @param tableName 表名
	 *             
	 * @throws IOException 
	 */
	public void deleteTable(String tableName) throws IOException {
		if (this.admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			System.out.println("禁用表" + tableName + "!");
			admin.deleteTable(tableName);
			System.out.println("删除表成功!");
		} else {
			System.out.println(tableName + "表不存在 !");
		}
	}

	/** 
	 * 插入记录 
	 *  
	 * @param tableName 
	 *            表名 
	 * @param rowkey 
	 *            键 
	 * @param family 
	 *            簇 
	 * @param qualifier 
	 * @param value 
	 *            值 
	 * @throws IOException 
	 */
	public void insertRecord(String tableName, String rowkey, String family, String qualifier, String value)
			throws IOException {
		HTableInterface table = this.admin.getConnection().getTable(tableName);
		insertRecord(table, rowkey, family, qualifier, value);
		// Put put = new Put(rowkey.getBytes());
		// put.add(family.getBytes(), qualifier.getBytes(), value.getBytes());
		// table.put(put);
		// System.out.println(tableName + "插入key:" + rowkey + "行成功!");
	}

	public void insertRecord(HTableInterface table, String rowkey, String family, String qualifier, String value)
			throws IOException {
		Put put = new Put(rowkey.getBytes());
		put.add(family.getBytes(), qualifier.getBytes(), value.getBytes());
		table.put(put);
	}

	/** 
	 * 删除一行记录 
	 *  
	 * @param tableName 
	 *            表名 
	 * @param rowkey 
	 *            主键 
	 * @throws IOException 
	 */
	public void deleteRecord(String tableName, String rowkey) throws IOException {
		HTableInterface table = this.admin.getConnection().getTable(tableName);
		Delete del = new Delete(rowkey.getBytes());
		table.delete(del);
		System.out.println(tableName + "删除行" + rowkey + "成功!");
	}

	public void deleteRecord(HTableInterface table, byte[] rowkey) throws IOException {
		Delete del = new Delete(rowkey);
		table.delete(del);
	}

	public void deleteAll(String tableName) throws IOException {
		HTableInterface table = this.admin.getConnection().getTable(tableName);
		Scan scan = new Scan();
		ResultScanner rs = table.getScanner(scan);
		Result result;
		while ((result = rs.next()) != null) {
			deleteRecord(table, result.getRow());
		}
	}

	/** 
	 * 获取一条记录 
	 *  
	 * @param tableName 
	 *            表名 
	 * @param rowkey 
	 *            主键 
	 * @return 
	 * @throws IOException 
	 */
	public Result getOneRecord(String tableName, String rowkey) throws IOException {
		HTableInterface table = this.admin.getConnection().getTable(tableName);
		Get get = new Get(rowkey.getBytes());
		Result rs = table.get(get);
		return rs;
	}

	/** 
	 * 获取所有数据 
	 * @param tableName 表名 
	 * @return 
	 * @throws IOException 
	 */
	public List<Result> getAllRecord(String tableName) throws IOException {
		HTableInterface table = this.admin.getConnection().getTable(tableName);
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		List<Result> list = new ArrayList<Result>();
		for (Result r : scanner) {
			list.add(r);
		}
		scanner.close();
		return list;
	}

	public void addFamily(String tableName, String familyColumn) throws IOException {
		HColumnDescriptor columnDescriptor = new HColumnDescriptor(familyColumn);
		admin.addColumn(tableName, columnDescriptor);
		System.out.println(String.format("表 %s 成功添加列簇 %s", tableName, familyColumn));
	}

	public void deleteFamily(String tableName, String familyColumn) throws IOException {
		admin.deleteColumn(tableName, familyColumn);
		System.out.println(String.format("表 %s 成功删除列簇 %s", tableName, familyColumn));
	}
	
	public void close()throws IOException {
		this.admin.close();
	}
}
