/**   
* @Title: ImportXZTB.java 
* @Package edu.jxust.SpatialData 
* @Description: TODO 
* @author 张炫铤  
* @date 2017年3月16日 下午8:15:12 
* @version V1.0   
*/
package edu.jxust.SpatialData;

import java.io.File;

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.util.Iterator;

import org.apache.log4j.Logger;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;

import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Geometry;

import com.vividsolutions.jts.io.WKBWriter;

import edu.jxust.Common.DBUtil;
import edu.jxust.Indexing.Grid;
import edu.jxust.Indexing.GridCode;

/**
 * @ClassName: ImportXZTB
 * @Description: TODO
 * @author 张炫铤
 * @date 2017年3月16日 下午8:15:12
 * 
 */
public class ImportGHYT {



	/**
	 * @Title: main @Description: TODO @param args @throws
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Logger log=Logger.getLogger(ImportGHYT.class);		
		String data = "J:\\浙江省土地利用规划数据\\成果数据整理\\GHYT_WGS_1984.shp";
		log.info(String.format("导入数据：%s", data));	
		Connection con = DBUtil.getDefaultConnection();
		Long start=System.currentTimeMillis();
		try {
			importData(data,  "GHYT", 1, con);
		} catch (Exception e) {
			// TODO Auto-generated catch block			
			log.error(e);;
		}
		log.info(String.format("导入时间：%s", System.currentTimeMillis()-start));	
	}

	public static void importData(String filePath, String tableName, int layerId,
			Connection con) throws Exception {

		WKBWriter wkbWriter = new WKBWriter();
		StringBuffer sqlGeoInfo = new StringBuffer("upsert into Spatial_Data_GHYT (");
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
		sqlAttributeInfo.append("SHAPE,");
		sqlAttributeInfo.append("YSDM,");		
		sqlAttributeInfo.append("BZ,");	
		sqlAttributeInfo.append("MBBSM,");	
		sqlAttributeInfo.append("MJ,");	
		sqlAttributeInfo.append("GHYTMC,");	
		sqlAttributeInfo.append("XZQHDM,");	
		sqlAttributeInfo.append("XZQHMC,");	
		sqlAttributeInfo.append("YTDKBH,");	
		sqlAttributeInfo.append("GHYTBM,");	
		sqlAttributeInfo.append("JMJ,");	
		sqlAttributeInfo.append("JNLX,");	
		sqlAttributeInfo.append("XZNTLX,");	
		sqlAttributeInfo.append("GHYTDM,");
		sqlAttributeInfo.append("PZWH");				
		sqlAttributeInfo.append(") values (");
		sqlAttributeInfo.append("?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?");
		sqlAttributeInfo.append(")");

		PreparedStatement pstmtSpatialData = con.prepareStatement(sqlGeoInfo.toString());
		PreparedStatement pstmtAttribute = con.prepareStatement(sqlAttributeInfo.toString());
		long count = 0;
		ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();
		try {
			ShapefileDataStore sds = (ShapefileDataStore) dataStoreFactory
					.createDataStore(new File(filePath).toURI().toURL());
			sds.setCharset(Charset.forName("UTF-8"));
			SimpleFeatureSource featureSource = sds.getFeatureSource();
			SimpleFeatureIterator itertor = featureSource.getFeatures().features();

			while (itertor.hasNext()) {
				count++;
				SimpleFeature feature = itertor.next();

				Geometry geo = (Geometry) feature.getDefaultGeometry();
				String rowkey = getRowkey(geo, Integer.toString(layerId), count);
				pstmtSpatialData.setString(1, rowkey);
				pstmtSpatialData.setBytes(2, wkbWriter.write(geo));
				pstmtSpatialData.setBytes(3, wkbWriter.write(geo.getEnvelope()));
				pstmtSpatialData.setInt(4, getFeatureType(geo.getGeometryType()));
				pstmtSpatialData.setInt(7, layerId);

				pstmtAttribute.setString(1, rowkey);//
				pstmtAttribute.setBytes(2, wkbWriter.write(geo));
				Iterator<Property> it = feature.getProperties().iterator();
				
				while (it.hasNext()) {
					Property pro = it.next();
					String fName = pro.getName().toString().toUpperCase();
					Object value=pro.getValue();
					if (fName.toLowerCase().equals("shape_star")) {
						pstmtSpatialData.setDouble(5, geo.getArea());
						continue;
					}

					if (fName.toLowerCase().equals("shape_stle")) {
						pstmtSpatialData.setDouble(6, geo.getLength());
						continue;
					}				
				
					if(fName.toUpperCase().equals("YSDM")){
						pstmtAttribute.setString(3, String.valueOf(value));
						continue;
					}
					if(fName.toUpperCase().equals("BZ")){
						pstmtAttribute.setString(4, String.valueOf(value));
						continue;
					}
				
					if(fName.toUpperCase().equals("MBBSM")){
						pstmtAttribute.setLong(5, Long.valueOf(String.valueOf(value)));
						continue;
					}
					
					if(fName.toUpperCase().equals("MJ")){
						pstmtAttribute.setDouble(6, (Double)value);
						continue;
					}
					if(fName.toUpperCase().equals("GHYTMC")){
						pstmtAttribute.setString(7, String.valueOf(value));
						continue;
					}
					if(fName.toUpperCase().equals("XZQHDM")){
						pstmtAttribute.setString(8, String.valueOf(value));
						continue;
					}
					if(fName.toUpperCase().equals("XZQHMC")){
						pstmtAttribute.setString(9, String.valueOf(value));
						continue;
					}
					if(fName.toUpperCase().equals("YTDKBH")){
						pstmtAttribute.setString(10, String.valueOf(value));
						continue;
					}
					if(fName.toUpperCase().equals("GHYTBM")){
						pstmtAttribute.setString(11, String.valueOf(value));
						continue;
					}
					if(fName.toUpperCase().equals("JMJ")){
						pstmtAttribute.setDouble(12, (Double)value);
						continue;
					}
					
					if(fName.toUpperCase().equals("JNLX")){
						pstmtAttribute.setString(13, String.valueOf(value));
						continue;
					}
					
					if(fName.toUpperCase().equals("XZNTLX")){
						pstmtAttribute.setString(14, String.valueOf(value));
						continue;
					}
					if(fName.toUpperCase().equals("GHYTDM")){
						pstmtAttribute.setString(15, String.valueOf(value));
						continue;
					}
					if(fName.toUpperCase().equals("PZWH")){
						pstmtAttribute.setString(16, String.valueOf(value));
						continue;
					}
					
				}
				pstmtSpatialData.addBatch();
				pstmtAttribute.addBatch();
				if (count % 1000 == 0) {
					pstmtSpatialData.executeBatch();
					pstmtAttribute.executeBatch();
				}
				if (count % 10000 == 0) {
					pstmtSpatialData.executeBatch();
					pstmtAttribute.executeBatch();
					con.commit();
					pstmtSpatialData.close();
					pstmtAttribute.close();
					con.close();
					con = DBUtil.getDefaultConnection();
					pstmtSpatialData = con.prepareStatement(sqlGeoInfo.toString());
					pstmtAttribute = con.prepareStatement(sqlAttributeInfo.toString());
				}
			}
			itertor.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		pstmtSpatialData.executeBatch();
		pstmtAttribute.executeBatch();
		con.commit();
		pstmtSpatialData.close();
		pstmtAttribute.close();
		con.close();
	}

	private static String getRowkey(Geometry g, String layerId, long num) {
		String gridHilbertEncode = GridCode.getHilbertEncode(16, Grid.getGridCoordinate(16, g.getCentroid()));
		return String.format("%s_%s_%s", gridHilbertEncode, layerId, num);
	}

	private static int getFeatureType(String geometryyType) {
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

	public static void setPreparedStatementValue(PreparedStatement pstmt, int index, Object type, Object value) {
		try {
			if (String.class.equals(type)) {
				pstmt.setString(index, String.valueOf(value));
				return;
			}
			if (Double.class.equals(type)) {
				pstmt.setDouble(index, (Double) value);
				return;
			}
			if (Integer.class.equals(type)) {
				pstmt.setInt(index, (Integer) value);
				return;
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
