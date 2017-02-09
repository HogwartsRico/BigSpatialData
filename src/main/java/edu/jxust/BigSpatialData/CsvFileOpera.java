/**   
* @Title: CsvFileOpera.java 
* @Package edu.jxust.BigSpatialData 
* @Description: TODO 
* @author 张炫铤  
* @date 2017年2月4日 下午12:08:51 
* @version V1.0   
*/
package edu.jxust.BigSpatialData;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.geometry.jts.WKTWriter2;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.WKTReader;
import edu.jxust.Indexing.Grid;
import edu.jxust.Indexing.GridCode;

/** 
* @ClassName: CsvFileOpera 
* @Description: csv文件操作
* @author 张炫铤
* @date 2017年2月4日 下午12:08:51 
*  
*/
public class CsvFileOpera {
	private String headerFilePath;
	private String dataFilePath;

	public CsvFileOpera(String headerFilePath, String dataFilePath) {
		this.headerFilePath = headerFilePath;
		this.dataFilePath = dataFilePath;
	}

	/** 
	* @Title: getHeader 
	* @Description: 提取头文件，头文件一般存储字段信息
	* @param split 分隔符
	* @return
	* @throws Exception
	*/
	public String[] getHeader(String split) throws Exception {
		String[] fields = null;
		try (BufferedReader reader = new BufferedReader(new FileReader(headerFilePath))) {
			String line = reader.readLine();
			if (line != null) {
				fields = getSplits(line, split);
			}
		}
		return fields;
	}

	public List<byte[]> getByteArrays(String[] array) throws Exception {
		List<byte[]> byteArrays = new ArrayList<byte[]>(array.length);
		for (int i = 0; i < array.length; i++) {
			byteArrays.add(array[i].getBytes());
		}
		return byteArrays;
	}

	private String[] getSplits(String line, String split) {
		if (line.trim().length() <= 0)
			return null;
		return line.split(split);
	}

	public void importData(HTableInterface table, String headerSplit, String dataSplit) throws Exception {

		List<byte[]> fields = getByteArrays(getHeader(headerSplit));

		String layerId = "1";
		byte[] geoInfoFamily = "GeometryInfo".getBytes();// 几何信息列簇
		byte[] geoColumn = "Geometry".getBytes();
		byte[] mbrColumn = "MBR".getBytes();
		byte[] featureTypeColumn = "FeatureType".getBytes();
		byte[] areaColumn = "Area".getBytes();
		byte[] lengthColumn = "Length".getBytes();
		byte[] family = "AttributeInfo".getBytes();// 属性列簇
		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
		WKTReader wktReader = new WKTReader(geometryFactory);
		WKTWriter2 wktWriter = new WKTWriter2();
		long count = 0;
		try (BufferedReader reader = new BufferedReader(new FileReader(dataFilePath))) {

			String line;
			while ((line = reader.readLine()) != null) {
				count++;
				String[] values = getSplits(line, dataSplit);
				String wkt = values[0].replace("\"", "");
				Geometry geo = wktReader.read(wkt);
				String rowkey = getRowkey(geo, layerId, count);
				Put put = new Put(rowkey.getBytes());
				put.add(geoInfoFamily, geoColumn, wkt.getBytes());
				put.add(geoInfoFamily, mbrColumn, wktWriter.write(geo.getEnvelope()).getBytes());
				put.add(geoInfoFamily, featureTypeColumn, geo.getGeometryType().getBytes());
				put.add(geoInfoFamily, areaColumn, Double.toString(geo.getArea()).getBytes());
				put.add(geoInfoFamily, lengthColumn, Double.toString(geo.getLength()).getBytes());
				for (int i = 1; i < values.length; i++) {
					if (values[i].equals(null) || values[i].trim().equals(""))
						continue;
					put.add(family, fields.get(i), values[i].getBytes());
				}
				table.put(put);
				if (values[3].equals(null) == false) {
					System.out.println(values[3]);
				} else {
					System.out.println(rowkey);
				}
			}
		}
	}

	private String getRowkey(Geometry g, String layerId, long num) {
		String gridHilbertEncode = GridCode.getHilbertEncode(16, Grid.getGridCoordinate(16, g.getCentroid()));
		return String.format("%s_%s_%s", gridHilbertEncode, layerId, num);
	}
}
