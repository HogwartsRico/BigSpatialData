/**   
* @Title: ImportCsvData.java 
* @Package edu.jxust.BigSpatialData 
* @Description: TODO 
* @author 张炫铤  
* @date 2017年3月5日 下午3:42:14 
* @version V1.0   
*/
package edu.jxust.BigSpatialData;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;

import org.geotools.geometry.jts.JTSFactoryFinder;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;

import edu.jxust.Common.DBUtil;
import edu.jxust.Indexing.Grid;
import edu.jxust.Indexing.GridCode;

/** 
* @ClassName: ImportCsvData 
* @Description: TODO
* @author 张炫铤
* @date 2017年3月5日 下午3:42:14 
*  
*/
public class ImportCsvData {

	String dataFile;

	public ImportCsvData(String file) {
		this.dataFile = file;
	}

	public void importData(String dataSplit, String tableName,Connection con) throws Exception{
		long count = 0;
		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
		WKTReader wktReader = new WKTReader(geometryFactory);
		WKBWriter wkbWriter = new WKBWriter();
		int layerId = 0;
		
		StringBuffer sqlGeoInfo = new StringBuffer("upsert into Spatial_Data (");
		sqlGeoInfo.append("DataKey,");
		sqlGeoInfo.append("Geometry,");
		sqlGeoInfo.append("MBR,");
		sqlGeoInfo.append("FeatureType,");
		sqlGeoInfo.append("Area,");
		sqlGeoInfo.append("Length,");
		sqlGeoInfo.append("Layerid");
		sqlGeoInfo.append(") values (");
		sqlGeoInfo.append("?,?,?,?,?,?,?");
		sqlGeoInfo.append(")");
		StringBuffer sqlAttributeInfo = new StringBuffer("upsert into ");
		sqlAttributeInfo.append(tableName);
		sqlAttributeInfo.append("(");
		sqlAttributeInfo.append("Datakey,");
		sqlAttributeInfo.append("Statefp,");
		sqlAttributeInfo.append("Ansicode,");
		sqlAttributeInfo.append("Areaid,");
		sqlAttributeInfo.append("Fullname,");
		sqlAttributeInfo.append("Mtfcc,");
		sqlAttributeInfo.append("Aland,");
		sqlAttributeInfo.append("Awater,");
		sqlAttributeInfo.append("Intptlat,");
		sqlAttributeInfo.append("Intptlon,");
		sqlAttributeInfo.append("Partflg");
		sqlAttributeInfo.append(") values (");
		sqlAttributeInfo.append("?,?,?,?,?,?,?,?,?,?,?");
		sqlAttributeInfo.append(")");

		PreparedStatement pstmtSpatialData = con.prepareStatement(sqlGeoInfo.toString());
		PreparedStatement pstmtAttribute = con.prepareStatement(sqlAttributeInfo.toString());
		try (BufferedReader reader = new BufferedReader(new FileReader(dataFile))) {

			String line;
			while ((line = reader.readLine()) != null) {
				count++;
				String[] values = getSplits(line, dataSplit);
				String wkt = values[0].replace("\"", "");
				Geometry geo = wktReader.read(wkt);
				String rowkey = getRowkey(geo, Integer.toString(layerId), count);
				pstmtSpatialData.setString(1, rowkey);
				pstmtSpatialData.setBytes(2, wkbWriter.write(geo));
				pstmtSpatialData.setBytes(3, wkbWriter.write(geo.getEnvelope()));
				pstmtSpatialData.setInt(4, getFeatureType(geo.getGeometryType()));
				pstmtSpatialData.setDouble(5, geo.getArea());
				pstmtSpatialData.setDouble(6, geo.getLength());
				pstmtSpatialData.setInt(7, layerId);
				pstmtSpatialData.addBatch();
				
				pstmtAttribute.setString(1, rowkey);
				pstmtAttribute.setString(2, values[1]);
				pstmtAttribute.setString(3, values[2]);
				pstmtAttribute.setString(4, values[3]);
				pstmtAttribute.setString(5, values[4]);
				pstmtAttribute.setString(6, values[5]);
				pstmtAttribute.setInt(7, Integer.valueOf(values[6]));
				pstmtAttribute.setInt(8, Integer.valueOf(values[7]));
				pstmtAttribute.setString(9, values[8]);
				pstmtAttribute.setString(10, values[9]);
				pstmtAttribute.setString(11, values[10]);
				pstmtAttribute.addBatch();
				 if(count%1000==0){
					 pstmtSpatialData.executeBatch();
	                 pstmtAttribute.executeBatch();
	             }
			}
			 pstmtSpatialData.executeBatch();
             pstmtAttribute.executeBatch();
             con.commit();
             pstmtSpatialData.close();
             pstmtAttribute.close();
             con.close();
		}

	}
	public void importData(String dataSplit, String tableName) throws Exception {
		Connection con = DBUtil.getConnection();
		importData(dataSplit,tableName,con);
	}

	private String[] getSplits(String line, String split) {
		if (line.trim().length() <= 0)
			return null;
		return line.split(split);
	}

	private String getRowkey(Geometry g, String layerId, long num) {
		String gridHilbertEncode = GridCode.getHilbertEncode(16, Grid.getGridCoordinate(16, g.getCentroid()));
		return String.format("%s_%s_%s", gridHilbertEncode, layerId, num);
	}

	private int getFeatureType(String geometryyType) {
		int type = 0;
		switch (geometryyType.toLowerCase()) {
		case "point":
			type = 1;
			break;
		case "linestring":
			type = 2;
			break;
		case "linearring":
			type = 3;
			break;
		case "polygon":
			type = 4;
			break;
		case "multipoint":
			type = 5;
			break;
		case "multilinestring":
			type = 6;
			break;
		case "multipolygon":
			type = 7;
			break;
		case "point3d":
			type = 8;
			break;
		case "linestring3d":
			type = 9;
			break;
		case "linearring3d":
			type = 10;
			break;
		case "polygon3d":
			type = 11;
			break;
		case "multipoint3d":
			type = 12;
			break;
		case "multilinestring3d":
			type = 13;
			break;
		case "multipolygon3d":
			type = 14;
			break;
		case "geometrycollection":
			type = 15;
			break;
		default:
			type = 0;
		}
		return type;
	}
}
