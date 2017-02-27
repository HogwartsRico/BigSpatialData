/**   
* @Title: TestGridIndex.java 
* @Package edu.jxust.BigSpatialData 
* @Description: TODO 
* @author 张炫铤  
* @date 2017年2月27日 下午5:17:47 
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

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.geotools.data.DataUtilities;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.Transaction;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;

import edu.jxust.Common.HBaseHelper;
import edu.jxust.Indexing.Grid;
import edu.jxust.Indexing.MutiGridIndex;
import edu.jxust.SpatialData.ShapefileOpera;

/** 
* @ClassName: TestGridIndex 
* @Description: 测试多级网格算法分治方案与非分治方案的效率
* @author 张炫铤
* @date 2017年2月27日 下午5:17:47 
*  
*/
public class TestGridIndex {
	private static Logger logger;

	/** 
	* @Title: main 
	* @Description: TODO
	* @param args
	* @throws 
	*/
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		PropertyConfigurator.configure("src/main/resources/log4j.properties");
		logger = Logger.getLogger(HBaseHelper.class);
		int gridLevel=16;
		File f1 = new File("G:\\MyFile\\研究生\\论文\\云计算\\测试数据\\CHN_adm0.shp");
		String p = String.format("G:\\MyFile\\研究生\\论文\\云计算\\测试数据\\test\\%s_%d_stack.shp",  "China",
				gridLevel);
		File f2 = new File(p);
		URL path = f2.toURI().toURL();
		FeatureCollection<SimpleFeatureType, SimpleFeature> collection = ShapefileOpera.ReadShapefile(f1);
		TestMultiGridIndex(collection,gridLevel,path);
	}

	private static void TestMultiGridIndex(FeatureCollection<SimpleFeatureType, SimpleFeature> collection,
			int gridLevel,URL outPutURL) throws Exception {

		try (FeatureIterator<SimpleFeature> features = collection.features()) {
			GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
			List<SimpleFeature> featureList = new ArrayList<>();
			SimpleFeatureType ftype = getFeatureType();

			if (features.hasNext()) {
				SimpleFeature feature = null;
				feature = features.next();
				String name = feature.getAttribute(1).toString();
				Geometry geometry = (Geometry) feature.getDefaultGeometry();
				int pointsNum = geometry.getNumPoints();
				SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(ftype);

				long startTime = System.currentTimeMillis();
				List<Grid> grids = MutiGridIndex.Index(geometry, geometry.getEnvelope(), gridLevel);
				long calcTime = System.currentTimeMillis() - startTime;
				String message = String.format("名称：%s		构造点数量:%s		网格计算时间：%s	", name, pointsNum, calcTime);
				logger.info(message);

				featureBuilder.add(geometry);
				featureBuilder.add(name);
				featureBuilder.add(grids.size());
				featureBuilder.add(pointsNum);
				featureBuilder.add(calcTime);

				featureList.add(featureBuilder.buildFeature(null));

			}			
			createShapefile(outPutURL, featureList, ftype);
		}

	}

	public static void createShapefile(URL path, List<SimpleFeature> features, SimpleFeatureType TYPE)
			throws Exception {

		/*
		 * Get an output file name and create the new shapefile
		 */
		// File newFile = getNewShapeFile(csvFile);

		ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();

		Map<String, Serializable> params = new HashMap<>();
		// params.put("url", newFile.toURI().toURL());
		params.put("url", path);
		params.put("create spatial index", Boolean.FALSE);
		ShapefileDataStore newDataStore = (ShapefileDataStore) dataStoreFactory.createNewDataStore(params);

		/*
		 * TYPE is used as a template to describe the file contents
		 */
		newDataStore.createSchema(TYPE);

		/*
		 * Write the features to the shapefile
		 */
		Transaction transaction = new DefaultTransaction("create");

		String typeName = newDataStore.getTypeNames()[0];
		SimpleFeatureSource featureSource = newDataStore.getFeatureSource(typeName);
		SimpleFeatureType SHAPE_TYPE = featureSource.getSchema();
		/*
		 * The Shapefile format has a couple limitations: - "the_geom" is always
		 * first, and used for the geometry attribute name - "the_geom" must be
		 * of type Point, MultiPoint, MuiltiLineString, MultiPolygon - Attribute
		 * names are limited in length - Not all data types are supported
		 * (example Timestamp represented as Date)
		 * 
		 * Each data store has different limitations so check the resulting
		 * SimpleFeatureType.
		 */
		System.out.println("SHAPE:" + SHAPE_TYPE);

		if (featureSource instanceof SimpleFeatureStore) {
			SimpleFeatureStore featureStore = (SimpleFeatureStore) featureSource;
			/*
			 * SimpleFeatureStore has a method to add features from a
			 * SimpleFeatureCollection object, so we use the
			 * ListFeatureCollection class to wrap our list of features.
			 */
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

		} else {
			logger.error(typeName + " does not support read/write access");
			System.exit(1);
		}

	}

	private static SimpleFeatureType getFeatureType() throws Exception {
		SimpleFeatureType polygonType = DataUtilities.createType("Grid", "the_geom:Polygon:srid=4326," + "Name:String,"
				+ "GridNum:Integer," + "PointsNum:Integer," + "计算时间:String");
		return polygonType;
	}
}
