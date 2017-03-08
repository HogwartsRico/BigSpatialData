/**   
* @Title: SpatialQueryTest.java 
* @Package edu.jxust.BigSpatialData 
* @Description: TODO 
* @author 张炫铤  
* @date 2017年3月8日 上午9:30:45 
* @version V1.0   
*/
package edu.jxust.BigSpatialData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.geotools.geometry.jts.JTSFactoryFinder;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKTReader;

import edu.jxust.Common.HBaseHelper;
import edu.jxust.SpatialQuery.QueryGeometry;

/** 
* @ClassName: SpatialQueryTest 
* @Description: TODO
* @author 张炫铤
* @date 2017年3月8日 上午9:30:45 
*  
*/
public class SpatialQueryTest {

	private static Logger logger;

	/**
	 * @throws Exception 
	* @Title: main 
	* @Description: TODO
	* @param args
	* @throws 
	*/
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		logger = Logger.getLogger(SpatialQueryTest.class);
		// testQuery();
		spatialQuery();
	}

	private static void testSpatialQuery() throws IOException, ParseException {
		HBaseHelper hbase = new HBaseHelper();
		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
		String geoWKT = "MULTIPOLYGON (((-87.845456936885569 33.133716798357682, -87.843843662323536 33.133752490272855, -87.844664576370576 33.135144474961294, -87.845728195440302 33.134866078023606, -87.845456936885569 33.133716798357682)))";
		byte[] family = Bytes.toBytes("GeometryInfo");
		byte[] qualifier = Bytes.toBytes("Geometry");
		WKTReader wktReader = new WKTReader(geometryFactory);
		WKBReader wkbReader = new WKBReader();
		Geometry geo = wktReader.read(geoWKT);
		long startTime = System.currentTimeMillis();
		QueryGeometry query = new QueryGeometry();
		List<String> queryCodes = query.getIndexGridCodes(geo, 8, 16);
		List<String> dealCodes = query.getLastLevelGridCodes();
		HTableInterface tableIndex = hbase.getTable("SpatialIndex_16");
		HTableInterface tableSpatial = hbase.getTable("SpatialData");
		List<byte[]> spatialDataKey = new ArrayList<byte[]>();
		for (String code : queryCodes) {
			Result rs = tableIndex.get(new Get(Bytes.toBytes(code)));
			while (rs != null) {
				for (Cell cell : rs.rawCells()) {
					spatialDataKey.add(CellUtil.cloneValue(cell));
				}
			}
		}
		for (String code : dealCodes) {
			Result rs = tableIndex.get(new Get(Bytes.toBytes(code)));
			while (rs != null) {
				for (Cell cell : rs.rawCells()) {
					byte[] dataKey = CellUtil.cloneValue(cell);
					if (spatialDataKey.contains(dataKey) == true)
						continue;// 剔除重复记录，避免多余空间运算

					Result rsData = tableSpatial.get(new Get(dataKey));
					if (rsData != null) {
						Cell geometryCell = rsData.getColumnLatestCell(family, qualifier);
						Geometry dataGeo = wkbReader.read(CellUtil.cloneValue(geometryCell));
						if (dataGeo.intersects(geo)) {
							spatialDataKey.add(dataKey);
						}
					}
				}
			}
		}
		hbase.close();

		logger.info(String.format("查询时间为", System.currentTimeMillis() - startTime));
		for (byte[] datakey : spatialDataKey) {
			logger.info(Bytes.toString(datakey));
		}
	}

	private static void spatialQuery() throws Exception {
		HBaseHelper hbase = new HBaseHelper();
		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
		String geoWKT = "MULTIPOLYGON (((-87.845456936885569 33.133716798357682, -87.843843662323536 33.133752490272855, -87.844664576370576 33.135144474961294, -87.845728195440302 33.134866078023606, -87.845456936885569 33.133716798357682)))";
		byte[] family = Bytes.toBytes("GeometryInfo");
		byte[] qualifier = Bytes.toBytes("Geometry");
		WKTReader wktReader = new WKTReader(geometryFactory);
		WKBReader wkbReader = new WKBReader();
		Geometry geo = wktReader.read(geoWKT);

		QueryGeometry query = new QueryGeometry();

		
		HTableInterface tableIndex = hbase.getTable("SpatialIndex_16");
		HTableInterface tableSpatial = hbase.getTable("SpatialData");

		long startTime = System.currentTimeMillis();
		List<String> queryCodes = query.getIndexGridCodes(geo, 8, 16);
		List<String> dealCodes = query.getLastLevelGridCodes();
		List<byte[]> spatialDataKey = new ArrayList<byte[]>();
		List<byte[]> indexDataKey = new ArrayList<byte[]>();
		for (String code : queryCodes) {
			getQuery(tableIndex, code, spatialDataKey);
		}
		for (String code : dealCodes) {
			getIndexKeyQuery(tableIndex, code, spatialDataKey, indexDataKey);
		}

		for (byte[] dataKey : indexDataKey) {
			getSpatialDataKeyQuery(tableSpatial, family, qualifier, dataKey, geo, spatialDataKey);
		}
		hbase.close();

		logger.info(String.format("查询时间为:%s", System.currentTimeMillis() - startTime));
		for (byte[] datakey : spatialDataKey) {
			logger.info(new String(datakey,"GBK"));						
		}
	}

	private static void getQuery(HTableInterface table, String dataKey, List<byte[]> values) throws IOException {
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(dataKey));
		scan.setStopRow(Bytes.toBytes(dataKey));
		ResultScanner scanner = table.getScanner(scan);
		Result rs = scanner.next();
		while (rs != null) {
			for (Cell cell : rs.rawCells()) {
				values.add(CellUtil.cloneValue(cell));
			}
			rs = scanner.next();
		}
	}

	private static void getIndexKeyQuery(HTableInterface table, String dataKey, List<byte[]> spatialDataKey,
			List<byte[]> values) throws IOException {
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(dataKey));
		scan.setStopRow(Bytes.toBytes(dataKey));
		ResultScanner scanner = table.getScanner(scan);
		Result rs = scanner.next();
		while (rs != null) {
			for (Cell cell : rs.rawCells()) {
				byte[] dataValue = CellUtil.cloneValue(cell);
				if (spatialDataKey.contains(dataValue) || values.contains(dataValue))
					continue;
				logger.info(Bytes.toString(dataValue));
				values.add(dataValue);
			}
			rs = scanner.next();
		}
	}

	private static void getSpatialDataKeyQuery(HTableInterface table, byte[] family, byte[] qualifier, byte[] dataKey,
			Geometry geo, List<byte[]> spatialDataKey) throws IOException, Exception {
		Scan scan = new Scan();
		scan.addColumn(family, qualifier);
		scan.setStartRow(dataKey);
		scan.setStopRow(dataKey);
		ResultScanner scanner = table.getScanner(scan);
		Result rs = scanner.next();
		WKBReader wkbReader = new WKBReader();
		while (rs != null) {
			for (Cell cell : rs.rawCells()) {
				byte[] dataValue = CellUtil.cloneValue(cell);
				if (wkbReader.read(dataValue).intersects(geo)) {
					spatialDataKey.add(rs.getRow());
				}
			}
			rs = scanner.next();
		}
	}

	private static void testQuery() throws IOException {
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes("16_00201000123232333"));
		scan.setStopRow(Bytes.toBytes("16_00201000123232333"));
		HBaseHelper hbase = new HBaseHelper();
		HTableInterface tableIndex = hbase.getTable("SpatialIndex_16");
		ResultScanner scanner = tableIndex.getScanner(scan);
		Result rs = scanner.next();
		while (rs != null) {

			//logger.info("获得到rowkey:" + new String(rs.getRow()));
			for (Cell cell : rs.rawCells()) {
				System.out.println("Rowkey : " + Bytes.toString(rs.getRow()) + "   Familiy:Quilifier : "
						+ Bytes.toString(CellUtil.cloneQualifier(cell)) + "   Value : "
						+ Bytes.toString(CellUtil.cloneValue(cell)) + "   Time : " + cell.getTimestamp());
			}
			rs = scanner.next();
		}

		hbase.close();
	}
}
