/**   
* @Title: GridIndex.java 
* @Package edu.jxust.BigSpatialData 
* @Description: TODO 
* @author 张炫铤  
* @date 2017年2月9日 上午10:45:50 
* @version V1.0   
*/
package edu.jxust.BigSpatialData;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.io.WKBReader;
import edu.jxust.Common.HBaseHelper;
import edu.jxust.Indexing.Grid;
import edu.jxust.Indexing.MutiGridIndex;

/** 
* @ClassName: GridIndex 
* @Description: 创建网格索引
* @author 张炫铤
* @date 2017年2月9日 上午10:45:50 
*  
*/
public class GridIndex {
	static Logger logger;

	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		PropertyConfigurator.configure("src/main/resources/log4j.properties");

		logger = Logger.getLogger(HBaseTest.class);
		long startTime = System.currentTimeMillis();
		createGridIndex(16, "SpatialIndex_16");
		long endTime = System.currentTimeMillis();
		logger.info("计算时间：" + (endTime - startTime));
	}

	public static void createGridIndex(int gridLevel, String indexTableName) throws Exception {
		HBaseHelper hbase = new HBaseHelper("master", "2181");

		HTableInterface dtSpatialData = hbase.getTable("SpatialData");
		HTableInterface dtSpatialIndex = hbase.getTable(indexTableName);
		Scan scan = new Scan();
		byte[] fGeoInfo = "GeometryInfo".getBytes();// 列簇family
		byte[] cGeometry = "Geometry".getBytes();// 列column
		byte[] cMBR = "MBR".getBytes();// 列column

		// 限定扫描列
		scan.addColumn(fGeoInfo, cGeometry);
		scan.addColumn(fGeoInfo, cMBR);
		ResultScanner rs = dtSpatialData.getScanner(scan);
		byte[] fMutiGridIndex = "MutiGridIndex".getBytes();

		WKBReader wkb = new WKBReader();

		Result result = rs.next();
		int count = 0;
		int total = 0;
		List<Put> puts = new ArrayList<>();
		while (result != null) {
			count++;
			total++;
			byte[] cIndexValue = result.getRow();// 数据表的行键
			String rowkeyData = new String(cIndexValue);
			byte[] cIndex = rowkeyData.substring(rowkeyData.indexOf("_") + 1).getBytes();// 采用图层标识+顺序码作为索引列名，确保索引一致性
			//long startTime = System.currentTimeMillis();
			List<Grid> grids = MutiGridIndex.Index(wkb.read(result.getValue(fGeoInfo, cGeometry)),
					wkb.read(result.getValue(fGeoInfo, cMBR)), gridLevel);
			// long endTime=System.currentTimeMillis();
			// long gridCalcTime=endTime-startTime;
			for (Grid grid : grids) {
				Put put = new Put(grid.getGridCode().getBytes());
				put.add(fMutiGridIndex, cIndex, cIndexValue);
				puts.add(put);

			}
			// long gridPutTime=System.currentTimeMillis()-endTime;
			logger.info(String.format("当前实体为：%s", rowkeyData));
			result = rs.next();
			if (count >= 1000 && result != null) {
				dtSpatialIndex.put(puts);
				puts = new ArrayList<>();
				scan.setStartRow(result.getRow());
				rs.close();
				rs = dtSpatialData.getScanner(scan);
				rs.next();
				count = 0;
			}

		}
		dtSpatialIndex.put(puts);
		logger.info(String.format("共创建 %s 条记录", total));
		rs.close();
		dtSpatialData.close();
		dtSpatialIndex.close();
		hbase.close();
	}
}
