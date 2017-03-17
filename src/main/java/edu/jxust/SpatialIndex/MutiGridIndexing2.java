package edu.jxust.SpatialIndex;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.io.WKBReader;

import edu.jxust.BigSpatialData.HBaseTest;
import edu.jxust.Common.HBaseHelper;
import edu.jxust.Indexing.Grid;
import edu.jxust.Indexing.MutiGridIndex;

public class MutiGridIndexing2 {
	static Logger logger;

	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		PropertyConfigurator.configure("src/main/resources/log4j.properties");

		logger = Logger.getLogger(HBaseTest.class);
		long startTime = System.currentTimeMillis();
		createGridIndex(10, "SpatialIndex_10_ZJ", "SPATIAL_DATA_ZJ", 0);
		long endTime = System.currentTimeMillis();
		logger.info("计算时间：" + (endTime - startTime));
	}

	public static void createGridIndex(int gridLevel, String indexTableName, String spatialTableName, int layerID)
			throws Exception {
		HBaseHelper hbase = new HBaseHelper("master", "2181");

		HTableInterface dtSpatialData = hbase.getTable(spatialTableName);
		HTableInterface dtSpatialIndex = hbase.getTable(indexTableName);
		byte[] famliyIndex = null;
		String layerIdStr = Integer.toString(layerID);

		// 以图层标识创建索引表列簇
		// if (hbase.addFamily(indexTableName, layerIdStr))
		// famliyIndex = Bytes.toBytes(layerIdStr);// 以图层标识作为列簇名称
		famliyIndex = Bytes.toBytes(layerIdStr);
		Scan scanData = new Scan();
		byte[] fData = "0".getBytes();// 列簇family
		byte[] cGeometry = "GEOMETRY".getBytes();// 列column
		byte[] cMBR = "MBR".getBytes();// 列column
		// 限定扫描列
		scanData.addColumn(fData, cGeometry);
		scanData.addColumn(fData, cMBR);
		ResultScanner rsData = dtSpatialData.getScanner(scanData);

		WKBReader wkb = new WKBReader();

		Result resultData = rsData.next();
		int count = 0;
		int total = 0;
		List<Put> puts = new ArrayList<>();
		while (resultData != null) {
			count++;
			total++;
			byte[] dataKey = resultData.getRow();// 数据表的行键
			String rowkeyData = new String(dataKey);
			byte[] cIndex = rowkeyData.substring(rowkeyData.indexOf("_") + 1).getBytes();// 采用图层标识+顺序码作为索引列名，确保索引一致性
			long s = System.currentTimeMillis();
			List<Grid> grids = MutiGridIndex.Index(wkb.read(resultData.getValue(fData, cGeometry)),
					wkb.read(resultData.getValue(fData, cMBR)), gridLevel);
			long ll = System.currentTimeMillis() - s;
			for (Grid grid : grids) {
				Put put = new Put(grid.getGridCode().getBytes());
				put.add(famliyIndex, cIndex, dataKey);
				puts.add(put);

			}

			resultData = rsData.next();
			if (count >= 1000 && resultData != null) {
				dtSpatialIndex.put(puts);
				puts = new ArrayList<>();
				scanData.setStartRow(resultData.getRow());
				rsData.close();
				rsData = dtSpatialData.getScanner(scanData);
				rsData.next();
				count = 0;
				logger.info(String.format("已创建 %s 条记录", total));
			}

		}
		dtSpatialIndex.put(puts);
		logger.info(String.format("共创建 %s 条记录", total));
		rsData.close();
		dtSpatialData.close();
		dtSpatialIndex.close();
		hbase.close();
	}
}
