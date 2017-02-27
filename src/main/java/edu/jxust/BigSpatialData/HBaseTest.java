/**   
* @Title: HBaseTest.java 
* @Package edu.jxust.SpatialData 
* @Description: HBase测试
* @author 张炫铤  
* @date 2017年1月16日 下午9:19:04 
* @version V1.0   
*/
package edu.jxust.BigSpatialData;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import edu.jxust.Common.HBaseHelper;
import edu.jxust.SpatialData.DataTable;

/** 
* @ClassName: HBaseTest 
* @Description: HBase测试
* @author 张炫铤
* @date 2017年1月16日 下午9:19:04 
*  
*/
public class HBaseTest {
	private static Logger logger ;

	/** 
	* @Title: main 
	* @Description: HBase测试
	* @param args
	* @throws 
	*/
	public static void main(String[] args) throws Exception {
		// System.out.println(App.getId());
		//BasicConfigurator.configure();
		PropertyConfigurator.configure("src/main/resources/log4j.properties"); 
		logger= Logger.getLogger(HBaseHelper.class);
      
        
     
        
		HBaseHelper hbase = new HBaseHelper();
		//System.out.println(hbase.tableExist("SpatialData"));
		//HTableInterface table = hbase.getTable("SpatialIndex_16");
		//Result  rr=table.get(new Get("08_000023323".getBytes()));
		 //getReversedData(table,false);
		//queryAll(table);
		long startTime=System.currentTimeMillis();
		DataTable.importNoneIndexData(hbase.getTable("SpatialData"), hbase.getTable("NoneIndexData"));
		logger.info(String.format("导入时间为：%s", System.currentTimeMillis()-startTime));
		hbase.close();
		
		// hbase.deleteAll("SpatialData");

		// Configuration conf = HBaseConfiguration.create();
		// conf.set("hbase.zookeeper.quorum", "master");//
		// 使用eclipse时必须添加这个，否则无法定位master需要配置hosts

		// queryAll(conf, "test1");

		// LOG.notify();
	}

	/**
	 * 查询目标表数据
	 * 
	 * @param configuration
	 *            HBase Configuration
	 * @param tableName
	 *            查询数据的目标表名称
	 * @throws IOException
	 */
	public static void queryAll(HTableInterface table) throws IOException {

		try {
			Scan scan = new Scan();
			ResultScanner rs = table.getScanner(scan);
			// 取10行数据
			Result r = rs.next();
			int i = 0;
			int total=0;
			while (r != null) {
				i++;
				total++;
				logger.info("获得到rowkey:" + new String(r.getRow()));
				for (Cell cell : r.rawCells()) {
					System.out.println("Rowkey : " + Bytes.toString(r.getRow()) + "   Familiy:Quilifier : "
							+ Bytes.toString(CellUtil.cloneQualifier(cell)) + "   Value : "
							+ Bytes.toString(CellUtil.cloneValue(cell)) + "   Time : " + cell.getTimestamp());
				}
				r = rs.next();
				if (i >= 2 && r != null) {

					scan.setStartRow(r.getRow());
					rs.close();
					rs = table.getScanner(scan);
					rs.next();
					i = 0;
				}
			}
			System.out.println(total);
		} catch (IOException e) {
			e.printStackTrace();
		}
		table.close();
	}

	private static void queryTable(String tableName) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "master");// 使用eclipse时必须添加这个，否则无法定位master需要配置hosts
		conf.set("hbase.zookeeper.property.clientPort", "2181");

		HBaseHelper hbaseOpra = new HBaseHelper(conf);
		List<Result> result = hbaseOpra.getAllRecord("tableName");
		for (int i = 0; i < result.size(); i++) {
			Result r = result.get(i);
			System.out.println("获得到rowkey:" + new String(r.getRow()));
			for (Cell cell : r.rawCells()) {
				System.out.println("Rowkey : " + Bytes.toString(r.getRow()) + "   Familiy:Quilifier : "
						+ Bytes.toString(CellUtil.cloneQualifier(cell)) + "   Value : "
						+ Bytes.toString(CellUtil.cloneValue(cell)) + "   Time : " + cell.getTimestamp());

			}
		}
	}

	private static void getReversedData(HTableInterface table,Boolean setReversed) throws IOException {
		try {

			Scan scan = new Scan();
			scan.setReversed(setReversed);// 倒序扫描
			ResultScanner rs = table.getScanner(scan);

			// 取10行数据
			for (Result r : rs.next(10)) {
			logger.info("获得到rowkey:" + new String(r.getRow()));
				for (Cell cell : r.rawCells()) {
					System.out.println("Rowkey : " + Bytes.toString(r.getRow()) + "   Familiy:Quilifier : "
							+ Bytes.toString(CellUtil.cloneQualifier(cell)) + "   Value : "
							+ Bytes.toString(CellUtil.cloneValue(cell)) + "   Time : " + cell.getTimestamp());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		table.close();
	}
}
