package edu.jxust.SpatialData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureSource;
import org.geotools.data.Transaction;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKTReader;

public class ShapefileOpera {

	public static FeatureCollection<SimpleFeatureType, SimpleFeature> ReadShapefile(File shpFile) throws Exception {
		Map<String, Object> params = new HashMap<>();
		params.put("url", shpFile.toURI().toURL());
		DataStore dataStore = DataStoreFinder.getDataStore(params);
		String typeName = dataStore.getTypeNames()[0];
		FeatureSource<SimpleFeatureType, SimpleFeature> source = dataStore.getFeatureSource(typeName);

		// FeatureCollection<SimpleFeatureType, SimpleFeature> collection =
		// source.getFeatures();
		return source.getFeatures();
	}

	public static List<SimpleFeature> GetFeaturesFromCSV(File csvfile, SimpleFeatureType featureType) throws Exception {
		/*
		 * A list to collect features as we create them.
		 */
		List<SimpleFeature> features = new ArrayList<>();

		/*
		 * GeometryFactory will be used to create the geometry attribute of each
		 * feature, using a Point object for the location.
		 */
		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();

		SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(featureType);

		try (BufferedReader reader = new BufferedReader(new FileReader(csvfile))) {
			/* First line of the data file is the header */
			String line = "";
			for (line = reader.readLine(); line != null; line = reader.readLine()) {
				if (line.trim().length() > 0) { // skip blank lines
					String tokens[] = line.split("\\\t");
					WKTReader wktReader = new WKTReader(geometryFactory);
					Geometry geom = wktReader.read(tokens[0].replace("\"", ""));
					if (geom.getGeometryType().equals("Polygon") == false)
						continue;
					Polygon polygon = (Polygon) geom;

					featureBuilder.add(polygon);

					SimpleFeature feature = featureBuilder.buildFeature(null);
					feature.getType().getTypeName();
					features.add(feature);
				}
			}
		}
		return features;
	}

	public static SimpleFeatureType Create() {
		SimpleFeatureTypeBuilder buildType = new SimpleFeatureTypeBuilder();
		buildType.setCRS(DefaultGeographicCRS.WGS84);
		return buildType.buildFeatureType();
	}

	public static void convertCSVtoShapefile(String dataFilePath,String dataSplit,String outPath) throws Exception {
	
		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
		WKTReader wktReader = new WKTReader(geometryFactory);
		SimpleFeatureType ftype = getFeatureType();
		SimpleFeatureBuilder featureBuilder ;
		List<SimpleFeature> featureList = new ArrayList<>();
		try (BufferedReader reader = new BufferedReader(new FileReader(dataFilePath))) {

			String line;
			while ((line = reader.readLine()) != null) {				
				String[] values = getSplits(line, dataSplit);
				String wkt = values[0].replace("\"", "");
				Geometry geo = wktReader.read(wkt);
				if(geo.getGeometryType().toLowerCase()=="mutipolygon"){
					System.out.println(line);
				}
					
				featureBuilder=new SimpleFeatureBuilder(ftype);
				featureBuilder.add(geo);			
				featureBuilder.add(values[1]);
				featureBuilder.add(values[2]);
				featureBuilder.add(values[3]);
				featureBuilder.add(values[4]);
				featureBuilder.add(values[5]);
				featureBuilder.add(convertToInt32(values[6]));
				featureBuilder.add(convertToInt32(values[7]));
				featureBuilder.add(values[8]);
				featureBuilder.add(values[9]);
				featureBuilder.add(values[10]);				
				featureList.add(featureBuilder.buildFeature(null));				
			}
	
		}		
		CreateShapefile(new File(outPath).toURI().toURL(), featureList, ftype);
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
	private  static int convertToInt32(String s) {

		long l = Long.parseLong(s);
		if (l > Integer.MAX_VALUE) {
			return Integer.MAX_VALUE;
		}
		if (l < Integer.MIN_VALUE) {
			return Integer.MIN_VALUE;
		}

		return (int) l;
	}
	private static String[] getSplits(String line, String split) {
		if (line.trim().length() <= 0)
			return null;
		return line.split(split);
	}
	private static SimpleFeatureType getFeatureType() throws Exception {

		StringBuffer str = new StringBuffer("the_geom:Polygon:srid=4326,");
		str.append("statefp:String,");
		str.append("ansicode:String,");
		str.append("areaid:String,");
		str.append("fullname:String,");
		str.append("mtfcc:String,");
		str.append("aland:Integer,");
		str.append("awater:Integer,");
		str.append("intptlat:String,");
		str.append("intptlon:String,");
		str.append("partflg:String");
		SimpleFeatureType polygonType = DataUtilities.createType("Grid", str.toString());
		return polygonType;
	}
}
