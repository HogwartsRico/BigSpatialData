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
import java.util.Map.Entry;

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

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;

import edu.jxust.Common.HBaseHelper;
import edu.jxust.Common.QueryRowKey;
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
	 * @Title: main @Description: TODO @param args @throws
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		logger = Logger.getLogger(SpatialQueryTest.class);
		// testQuery();
		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
		//String geoWKT = "MULTIPOLYGON (((-87.819500308892998 41.612520490441057, -87.737108865694495 41.609679406192868, -87.735418986535876 41.658685901794001, -87.817810429734322 41.66152698604219, -87.819500308892998 41.612520490441057)))";
		// Envelope mbr = new Envelope(-87.845456936885,33.133716798357,
		// -87.843843662323, 33.135144474961);
		String geoWKT = "MULTIPOLYGON (((-87.880109052584487 41.514176482292399, -87.516567737957132 41.519648508000905, -87.521455685297411 41.702818394793901, -87.884996999924752 41.697362000242826, -87.880109052584487 41.514176482292399)))";
		//String geoWKT="MULTIPOLYGON (((-87.829265977111291 41.548351260827424, -87.608937000179822 41.549770000259237, -87.61037902331374 41.676020944699758, -87.83070800024521 41.674605000133653, -87.829265977111291 41.548351260827424)))";
		WKTWriter wktWriter = new WKTWriter();
		WKTReader wktReader = new WKTReader(geometryFactory);
		Geometry geo = wktReader.read(geoWKT);
		String wkt = wktWriter.writeFormatted(geo.getEnvelope());

		spatialQuery(geo.getEnvelope());
	}

	private static void testSpatialQuery() throws IOException, ParseException {

		String geoWKT = "MULTIPOLYGON (((-87.845456936885569 33.133716798357682, -87.843843662323536 33.133752490272855, -87.844664576370576 33.135144474961294, -87.845728195440302 33.134866078023606, -87.845456936885569 33.133716798357682)))";

		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
		WKTReader wktReader = new WKTReader(geometryFactory);
		WKBReader wkbReader = new WKBReader();
		Geometry geo = wktReader.read(geoWKT);
		WKTWriter wktWriter = new WKTWriter();
		String wkt = wktWriter.writeFormatted(geo.getEnvelope());
		long startTime = System.currentTimeMillis();
		QueryGeometry query = new QueryGeometry();
		List<String> queryCodes = query.getIndexGridCodes(geo, 8, 16);
		List<String> dealCodes = query.getLastLevelGridCodes();
		HBaseHelper hbase = new HBaseHelper();
		byte[] family = Bytes.toBytes("GeometryInfo");
		byte[] qualifier = Bytes.toBytes("Geometry");
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
		int i=0;
		for (byte[] datakey : spatialDataKey) {
			logger.info(Bytes.toString(datakey));
			i++;
		}
		
		logger.info(String.format("查询返回记录：",i));
	}

	private static void spatialQuery(String geoWKT) throws Exception {

		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
		// String geoWKT = "MULTIPOLYGON (((-87.845456936885569
		// 33.133716798357682, -87.843843662323536 33.133752490272855,
		// -87.844664576370576 33.135144474961294, -87.845728195440302
		// 33.134866078023606, -87.845456936885569 33.133716798357682)))";
		// String geoWKT = "POLYGON ((-87.8457281954403 33.13371679835768,
		// -87.8457281954403 33.135144474961294, -87.84384366232354
		// 33.135144474961294, -87.84384366232354 33.13371679835768,
		// -87.8457281954403 33.13371679835768))";
		WKTReader wktReader = new WKTReader(geometryFactory);
		// WKBReader wkbReader = new WKBReader();
		Geometry geo = wktReader.read(geoWKT);
		spatialQuery(geo);
	}

	private static void spatialQuery(Geometry geo) throws Exception {
		QueryGeometry query = new QueryGeometry();
		HBaseHelper hbase = new HBaseHelper();
		byte[] family = Bytes.toBytes("GeometryInfo");
		byte[] qualifier = Bytes.toBytes("Geometry");
		HTableInterface tableIndex = hbase.getTable("SpatialIndex_16");
		HTableInterface tableSpatial = hbase.getTable("SpatialData");

		long startTime = System.currentTimeMillis();
		//List<String> queryCodes = query.getIndexGridCodes(geo, 8, 16);
		//List<String> dealCodes = query.getLastLevelGridCodes();
		
		Map<QueryRowKey,Integer> queryCodes = query.getIndexMapGridCodes(geo, 8, 16);
		Map<QueryRowKey,Integer> dealCodes = query.getLastLevelMapGridCodes();
		
		List<String> spatialDataKey = new ArrayList<String>();
		List<String> indexDataKey = new ArrayList<String>();
		for (Entry<QueryRowKey,Integer> entry: queryCodes.entrySet()) {
			getQuery(tableIndex, entry.getKey(), spatialDataKey);

		}
		
		for (Entry<QueryRowKey,Integer> entry: dealCodes.entrySet()) {			
			getIndexKeyQuery(tableIndex, entry.getKey(), spatialDataKey, indexDataKey);
		}
		

		for (String dataKey : indexDataKey) {
			getSpatialDataKeyQuery(tableSpatial, family, qualifier, Bytes.toBytes(dataKey), geo, spatialDataKey);
		}
		hbase.close();

		logger.info(String.format("查询时间为:%s", System.currentTimeMillis() - startTime));
		int i=0;
		for (String datakey : spatialDataKey) {
			logger.info(datakey);
			i++;
		}
		logger.info(String.format("查询返回记录：%s",i));
	}

	private static void getQuery(HTableInterface table, QueryRowKey queryKey, List<String> values) throws IOException {
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(queryKey.getStartRow()));
		scan.setStopRow(Bytes.toBytes(queryKey.getStopRow()));
		ResultScanner scanner = table.getScanner(scan);
		Result rs = scanner.next();
		while (rs != null) {
			for (Cell cell : rs.rawCells()) {
				String value = Bytes.toString(CellUtil.cloneValue(cell));
				if (values.contains(value))
					continue;
				values.add(value);
			}
			rs = scanner.next();
		}
	}

	private static void getIndexKeyQuery(HTableInterface table, QueryRowKey queryKey, List<String> spatialDataKey,
			List<String> values) throws IOException {
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(queryKey.getStartRow()));
		scan.setStopRow(Bytes.toBytes(queryKey.getStopRow()));
		ResultScanner scanner = table.getScanner(scan);
		Result rs = scanner.next();
		while (rs != null) {
			for (Cell cell : rs.rawCells()) {
				String dataValue = Bytes.toString(CellUtil.cloneValue(cell));
				if (spatialDataKey.contains(dataValue) || values.contains(dataValue))
					continue;
				values.add(dataValue);
			}
			rs = scanner.next();
		}
	}

	private static void getSpatialDataKeyQuery(HTableInterface table, byte[] family, byte[] qualifier, byte[] dataKey,
			Geometry geo, List<String> spatialDataKey) throws IOException, Exception {
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
					spatialDataKey.add(Bytes.toString(rs.getRow()));
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

			// logger.info("获得到rowkey:" + new String(rs.getRow()));
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
