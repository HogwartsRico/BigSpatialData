/**   
* @Title: QueryGridIndex.java 
* @Package edu.jxust.SpatialQuery 
* @Description: TODO 
* @author 张炫铤  
* @date 2017年3月18日 上午12:40:34 
* @version V1.0   
*/
package edu.jxust.SpatialQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBReader;

import edu.jxust.BigSpatialData.SpatialQueryTest;
import edu.jxust.Common.HBaseHelper;
import edu.jxust.Common.QueryRowKey;
import edu.jxust.Indexing.Grid;
import edu.jxust.Indexing.GridIndex;
import edu.jxust.SpatialData.ShapefileOpera;

/**
 * @ClassName: QueryGridIndex
 * @Description: 查询网格索引，查询算法与多级网格索引不同，但同样采取网格编码合并策略
 * @author 张炫铤
 * @date 2017年3月18日 上午12:40:34
 * 
 */
public class QueryGridIndex {
	private static Logger logger;

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		logger = Logger.getLogger(QueryGridIndex.class);

		String path = "G:\\张炫铤_工作空间\\测试数据\\data\\半山村_WGS_1984.shp";
		int level = 18;
		String indextable = String.format("GridIndex_%s_GHYT", StringUtils.leftPad(Integer.toString(level), 2, '0'));
		String tableName = "GHYT";
		Geometry geo = ShapefileOpera.getDefaultGeometry(path);
		spatialQuery(geo, tableName, indextable, level);
	}

	private static void spatialQuery(Geometry geo, String tableName, String indexTable, int level) throws Exception {
		QueryMutiGridIndex query = new QueryMutiGridIndex();
		HBaseHelper hbase = new HBaseHelper();
		byte[] family = Bytes.toBytes("0");
		byte[] qualifier = Bytes.toBytes("SHAPE");
		HTableInterface tableIndex = hbase.getTable(indexTable);
		HTableInterface tableSpatial = hbase.getTable(tableName);

		long startTime = System.currentTimeMillis();
		// List<String> queryCodes = query.getIndexGridCodes(geo, 8, 16);
		// List<String> dealCodes = query.getLastLevelGridCodes();

		Map<QueryRowKey, Integer> queryCodes = getMapGridIndexCodes(geo, level);
		List<String> spatialDataKey = new ArrayList<String>();
		List<String> indexDataKey = new ArrayList<String>();

		for (Entry<QueryRowKey, Integer> entry : queryCodes.entrySet()) {
			getIndexKeyQuery(tableIndex, entry.getKey(), indexDataKey);
		}

		for (String dataKey : indexDataKey) {
			getSpatialDataKeyQuery(tableSpatial, family, qualifier, Bytes.toBytes(dataKey), geo, spatialDataKey);
		}
		long endTime = System.currentTimeMillis();
		hbase.close();

		int i = 0;
		for (String datakey : spatialDataKey) {
			logger.info(datakey);
			i++;
		}
		logger.info(String.format("查询返回记录：%s", i));
		logger.info(String.format("查询时间为:%s", endTime - startTime));
	}

	private static void getIndexKeyQuery(HTableInterface table, QueryRowKey queryKey, List<String> values)
			throws IOException {
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(queryKey.getStartRow()));
		scan.setStopRow(Bytes.toBytes(queryKey.getStopRow()));
		ResultScanner scanner = table.getScanner(scan);
		Result rs = scanner.next();
		while (rs != null) {
			for (Cell cell : rs.rawCells()) {
				String dataValue = Bytes.toString(CellUtil.cloneValue(cell));
				if (values.contains(dataValue))
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

	public static Map<QueryRowKey, Integer> getMapGridIndexCodes(Geometry geo, int gridLevel) {
		return Grid.getMapQueryRowkeys(GridIndex.getIndexGrids(geo, gridLevel));
	}
}
