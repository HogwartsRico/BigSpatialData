/**   
* @Title: QueryTest.java 
* @Package edu.jxust.BigSpatialData 
* @Description: TODO 
* @author 张炫铤  
* @date 2017年3月7日 下午2:37:23 
* @version V1.0   
*/
package edu.jxust.BigSpatialData;

import java.io.File;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.geotools.data.DataUtilities;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.Transaction;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

import edu.jxust.Indexing.Grid;
import edu.jxust.SpatialQuery.QueryGeometry;

/** 
* @ClassName: QueryTest 
* @Description: TODO
* @author 张炫铤
* @date 2017年3月7日 下午2:37:23 
*  
*/
public class QueryTest {

	/**
	 * @throws Exception 
	 * @throws ParseException  
	* @Title: main 
	* @Description: TODO
	* @param args
	* @throws 
	*/
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String geoWKT ="MULTIPOLYGON (((-87.845456936885569 33.133716798357682, -87.843843662323536 33.133752490272855, -87.844664576370576 33.135144474961294, -87.845728195440302 33.134866078023606, -87.845456936885569 33.133716798357682)))";
		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
		WKTReader wktReader = new WKTReader(geometryFactory);
		Geometry geo = wktReader.read(geoWKT);
		QueryGeometry query = new QueryGeometry();
		int gridLevel=16;
		List<String> codes=query.getIndexGridCodes(geo, 8, gridLevel);
		for(String code:codes){
			System.out.println(code);
		}
		System.out.println("最后一级");
		List<String> codes2=query.getLastLevelGridCodes();
		for(String code:codes2){
			System.out.println(code);
		}
		System.out.println("最后一级");
	}

	
	private static void test(String geoWKT) throws Exception{
		//String geoWKT = "MULTIPOLYGON (((78.750000000000114 29.531250000000114, 81.562500000000057 29.531250000000114, 81.868403606666902 31.123036855541727, 78.750000000000114 30.937500000000114, 78.750000000000114 29.531250000000114)))";
				GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
				WKTReader wktReader = new WKTReader(geometryFactory);
				Geometry geo = wktReader.read(geoWKT);
				QueryGeometry query = new QueryGeometry();
				int gridLevel=16;
				List<Grid> grids = query.getIndexGrids(geo, 8,gridLevel);
				List<Grid> gridsTemp = query.getLastLevelGrids();
				SimpleFeatureType ftype = getFeatureType();
				SimpleFeatureBuilder featureBuilder ;
				List<SimpleFeature> featureList = new ArrayList<>();
				for (int i = 0; i < grids.size(); i++) {
					System.out.println(grids.get(i).getGridCode() + "	X:" + grids.get(i).getGridCoordinate().x + "	X:"
							+ grids.get(i).getGridCoordinate().y);
//					featureBuilder=new SimpleFeatureBuilder(ftype);
//					featureBuilder.add(grids.get(i).getGridGeometry());
//					featureBuilder.add(grids.get(i).getGridLevel());
//					featureBuilder.add((int)grids.get(i).getGridCoordinate().x);
//					featureBuilder.add((int)grids.get(i).getGridCoordinate().y);
//					featureBuilder.add(grids.get(i).getGridCode());
//					featureList.add(featureBuilder.buildFeature(null));	
				}
				System.out.println("最后一级");
				
				
				for (int i = 0; i < gridsTemp.size(); i++) {
					featureBuilder=new SimpleFeatureBuilder(ftype);
					featureBuilder.add(gridsTemp.get(i).getGridGeometry());
					featureBuilder.add(gridsTemp.get(i).getGridLevel());
					featureBuilder.add((int)gridsTemp.get(i).getGridCoordinate().x);
					featureBuilder.add((int)gridsTemp.get(i).getGridCoordinate().y);
					featureBuilder.add(gridsTemp.get(i).getGridCode());
					featureList.add(featureBuilder.buildFeature(null));			
				}
				
				String p = String.format("G:\\MyFile\\研究生\\论文\\云计算\\测试数据\\test\\QueryTest_%d_LastLevel_08.shp",gridLevel);			
				URL path = new File(p).toURI().toURL();
				CreateShapefile(path, featureList, ftype);
				System.out.println("Done");
	}
	
	public static void CreateShapefile(URL path, List<SimpleFeature> features, SimpleFeatureType TYPE)
			throws Exception {
		ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();

		Map<String, Serializable> params = new HashMap<>();
		// params.put("url", newFile.toURI().toURL());
		params.put("url", path);
		params.put("create spatial index", Boolean.FALSE);
		ShapefileDataStore newDataStore = (ShapefileDataStore) dataStoreFactory.createNewDataStore(params);

		newDataStore.createSchema(TYPE);

		Transaction transaction = new DefaultTransaction("create");

		String typeName = newDataStore.getTypeNames()[0];
		SimpleFeatureSource featureSource = newDataStore.getFeatureSource(typeName);
		SimpleFeatureType SHAPE_TYPE = featureSource.getSchema();

		System.out.println("SHAPE:" + SHAPE_TYPE);

		if (featureSource instanceof SimpleFeatureStore) {
			SimpleFeatureStore featureStore = (SimpleFeatureStore) featureSource;

			SimpleFeatureCollection collection = new ListFeatureCollection(TYPE, features);
			featureStore.setTransaction(transaction);
			try {
				featureStore.addFeatures(collection);
				transaction.commit();
			} catch (Exception problem) {
				problem.printStackTrace();
				transaction.rollback();
			} finally {
				transaction.close();
			}
			// System.exit(0); // success!
		} else {
			System.out.println(typeName + " does not support read/write access");
			System.exit(1);
		}

	}

	private static SimpleFeatureType getFeatureType() throws Exception {
		SimpleFeatureType polygonType = DataUtilities.createType("Grid",
				"the_geom:Polygon:srid=4326," + "GridLevel:Integer," + "X:Integer," + "Y:Integer," + "GridCode:String");
		return polygonType;
	}
}
