package edu.jxust.BigSpatialData;

import edu.jxust.Indexing.Grid;
import edu.jxust.Indexing.MutiGridIndex;
import edu.jxust.SpatialData.*;
import java.awt.HeadlessException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.swing.UIManager;
import javax.swing.UnsupportedLookAndFeelException;

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
import org.geotools.referencing.CRS;
import org.geotools.swing.data.JFileDataStoreChooser;
import org.opengis.feature.GeometryAttribute;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.feature.type.GeometryType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKTReader;

/**
 * Hello world!
 *
 */
public class App {

	public static void main(String[] args) throws Exception {

		// try {
		//
		// UIManager.setLookAndFeel(UIManager.getCrossPlatformLookAndFeelClassName());
		// } catch (UnsupportedLookAndFeelException | ClassNotFoundException |
		// IllegalAccessException
		// | InstantiationException e) {
		// System.out.println(e.getMessage());
		// }
		
		File f = new File("G:\\MyFile\\研究生\\论文\\云计算\\测试数据\\locations1.csv");
		  //File file = JFileDataStoreChooser.showOpenFile("csv", f, null);
		  ReadCSV(f);
		 
		File f1 = new File("G:\\MyFile\\研究生\\论文\\云计算\\测试数据\\CHN_adm0.shp");
//		File f2 = new File("G:\\MyFile\\研究生\\论文\\云计算\\测试数据\\test\\Export_Output_2.shp");
		// File file = JFileDataStoreChooser.showOpenFile("shp", f1, null);
		FeatureCollection<SimpleFeatureType, SimpleFeature> collection = ShapefileOpera.ReadShapefile(f1);
		// FeatureCollection<SimpleFeatureType, SimpleFeature>
		// collection1=ShapefileOpera.ReadShapefile(f2);
		// TestIntersect(collection);
		TestMultiGridIndex(collection, 7);
		System.out.println("Done");
		System.exit(0);
	}

	   /**  
     * 生成主键(16位数字) 
     * 主键生成方式,年月日时分秒毫秒的时间戳+四位随机数保证不重复 
     */    
    public static  String getId() {  
        //当前系统时间戳精确到毫秒  
        Long simple=System.currentTimeMillis();  
        //三位随机数  
        int random=new Random().nextInt(900)+100;//为变量赋随机值100-999;  
        return simple.toString()+random;    
    } 
	
	private static void ReadShapefileTest(FeatureCollection<SimpleFeatureType, SimpleFeature> collection) {
		try (FeatureIterator<SimpleFeature> features = collection.features()) {
			while (features.hasNext()) {
				SimpleFeature feature = features.next();
				SimpleFeatureType type = feature.getFeatureType();
				Geometry g = (Geometry) feature.getDefaultGeometry();
				Grid.getGridFromBox(g.getEnvelopeInternal());
				System.out.println("Geometry Type: " + g.getGeometryType());
				System.out.println("CRS Type: " + type.getCoordinateReferenceSystem().getName());
				// System.out.println("SimpleFeatureType: " + type.toString());
				System.out.print(feature.getID());
				System.out.print(": ");
				// System.out.println(feature.getDefaultGeometryProperty().getValue());
			}
		}
	}

	private static void TestMultiGridIndex(FeatureCollection<SimpleFeatureType, SimpleFeature> collection,
			int gridLevel) throws Exception {

		try (FeatureIterator<SimpleFeature> features = collection.features()) {
			GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
			List<SimpleFeature> featureList = new ArrayList<>();
			SimpleFeatureType ftype = getFeatureType();
			String name = "";
			if (features.hasNext()) {
				SimpleFeature feature = null;
				int n = 0;
				while (features.hasNext() && n <= 0) {
					feature = features.next();
					n++;
				}

				Geometry g = (Geometry) feature.getDefaultGeometry();
				System.out.println("构造点数量: " + g.getCoordinates().length);
				// Coordinate coors[]=new Coordinate[4];
				Grid grid = Grid.getGridFromBox(g.getEnvelopeInternal());
				Polygon polygon = geometryFactory.createPolygon(g.getEnvelope().getCoordinates());
				SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(ftype);
				featureBuilder.add(grid.getGridGeometry());
				featureBuilder.add(grid.getGridLevel());
				featureBuilder.add((int)grid.getGridCoordinate().x);
				featureBuilder.add((int)grid.getGridCoordinate().y);
				featureBuilder.add(grid.getGridCode());
				featureList.add(featureBuilder.buildFeature(null));
				MutiGridIndex mgrids = new MutiGridIndex();
				long startTime = System.currentTimeMillis();
				List<Grid> grids = mgrids.Index(g, gridLevel);
				long endTime = System.currentTimeMillis();
				System.out.println("网格计算时间：" + (endTime - startTime));
				for (Grid gridItem : grids) {
					featureBuilder.add(gridItem.getGridGeometry());
					featureBuilder.add(gridItem.getGridLevel());
					featureBuilder.add((int)gridItem.getGridCoordinate().x);
					featureBuilder.add((int)gridItem.getGridCoordinate().y);
					featureBuilder.add(gridItem.getGridCode());
					featureList.add(featureBuilder.buildFeature(null));
				}
				// Grid[] grids=grid.splitGrid();
				// for(int i=0;i<4;i++){
				// featureBuilder.add(grids[i].getGridGeometry());
				// featureList.add(featureBuilder.buildFeature(null));
				// }

				System.out.println("China contain Box: " + g.contains(polygon));
				System.out.println("Box contain China: " + polygon.contains(g));
				System.out.println("Geometry Type: " + g.getGeometryType());
				// System.out.println("CRS Type: " +
				// ftype.getCoordinateReferenceSystem().getName());
				// System.out.println("SimpleFeatureType: " + type.toString());
				name = feature.getAttribute(1).toString();
				System.out.println(name);
			}
			String p = String.format("G:\\MyFile\\研究生\\论文\\云计算\\测试数据\\test\\%s_%d_stack.shp", name = "China",
					gridLevel);
			File f1 = new File(p);
			URL path = f1.toURI().toURL();
			CreateShapefileFromCSV(path, featureList, ftype);
		}

	}

	private static void ReadCSV(File csvFile) {

		try {

			if (csvFile == null) {
				return;
			}
			final SimpleFeatureType TYPE = DataUtilities.createType("Location",
					"the_geom:Point:srid=4326," + "name:String," + "number:Integer");
			final SimpleFeatureType polygonType = DataUtilities.createType("AREALM",
					"the_geom:Polygon:srid=4326," + "statefp:String," + "ansicode:String," + "hydroid:String,"
							+ "fullname:String," + "mtfcc:String," + "aland:Integer," + "awater:Integer,"
							+ "intptlat:String," + "intptlon:String");
			List<SimpleFeature> features = CreateFeaturesFromCSV(csvFile, TYPE);
			URL path = getNewShapeFile(csvFile).toURI().toURL();
			// List<SimpleFeature> features = CreatePolygonsFromCSV(file,
			// polygonType);
			CreateShapefileFromCSV(path, features, polygonType);
			/*
			 * We use the DataUtilities class to create a FeatureType that will
			 * describe the data in our shapefile.
			 * 
			 * See also the createFeatureType method below for another, more
			 * flexible approach.
			 */
			System.out.println("Success");
			System.exit(0);
		} catch (Exception ex) {
			System.out.println(ex.getMessage());
		}
	}

	public static void CreateShapefileFromCSV(URL path, List<SimpleFeature> features, SimpleFeatureType TYPE)
			throws Exception {

		/*
		 * Get an output file name and create the new shapefile
		 */
		// File newFile = getNewShapeFile(csvFile);

		ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();

		Map<String, Serializable> params = new HashMap<>();
		// params.put("url", newFile.toURI().toURL());
		params.put("url", path);
		params.put("create spatial index", Boolean.TRUE);
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
			// System.exit(0); // success!
		} else {
			System.out.println(typeName + " does not support read/write access");
			System.exit(1);
		}

	}

	public static List<SimpleFeature> CreateFeaturesFromCSV(File csvfile, SimpleFeatureType TYPE) throws Exception {
		/*
		 * A list to collect features as we create them.
		 */
		List<SimpleFeature> features = new ArrayList<>();

		/*
		 * GeometryFactory will be used to create the geometry attribute of each
		 * feature, using a Point object for the location.
		 */
		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();

		SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(TYPE);

		try (BufferedReader reader = new BufferedReader(new FileReader(csvfile))) {
			/* First line of the data file is the header */
			String line = reader.readLine();
			System.out.println("Header: " + line);

			for (line = reader.readLine(); line != null; line = reader.readLine()) {
				if (line.trim().length() > 0) { // skip blank lines
					String tokens[] = line.split("\\,");

					double latitude = Double.parseDouble(tokens[0]);
					double longitude = Double.parseDouble(tokens[1]);
					String name = tokens[2].trim();
					int number = Integer.parseInt(tokens[3].trim());

					/* Longitude (= x coord) first ! */
					Point point = geometryFactory.createPoint(new Coordinate(longitude, latitude));

					featureBuilder.add(point);
					featureBuilder.add(name);
					featureBuilder.add(number);
					SimpleFeature feature = featureBuilder.buildFeature(null);
					GeometryAttribute tName=feature.getDefaultGeometryProperty();
					Geometry obj = (Geometry) tName.getValue();
					Point p=(Point)obj;
					Geometry g=(Geometry)p;
				String gType=	obj.getGeometryType();
					String nn1=tName.getName().toString();
					GeometryType tDes=feature.getFeatureType().getGeometryDescriptor().getType();
					String nn=tDes.toString();
				
					features.add(feature);
				}
			}
		}
		return features;
	}

	public static List<SimpleFeature> CreatePolygonsFromCSV(File csvfile, SimpleFeatureType TYPE) throws Exception {
		/*
		 * A list to collect features as we create them.
		 */
		List<SimpleFeature> features = new ArrayList<>();

		/*
		 * GeometryFactory will be used to create the geometry attribute of each
		 * feature, using a Point object for the location.
		 */
		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();

		SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(TYPE);

		try (BufferedReader reader = new BufferedReader(new FileReader(csvfile))) {
			/* First line of the data file is the header */
			String line = reader.readLine();
			System.out.println("Header: " + line);

			for (line = reader.readLine(); line != null; line = reader.readLine()) {
				if (line.trim().length() > 0) { // skip blank lines
					String tokens[] = line.split("\\\t");
					WKTReader wktReader = new WKTReader(geometryFactory);
					Geometry geom = wktReader.read(tokens[0].replace("\"", ""));
					String type = geom.getGeometryType();
					if (geom.getGeometryType().equals("Polygon") == false)
						continue;
					Polygon polygon = (Polygon) geom;
					String statefp = tokens[1].trim();
					// String countyfp = tokens[2].trim();
					String ansicode = tokens[2].trim();
					String hydroid = tokens[3].trim();
					String fullname = tokens[4].trim();
					String mtfcc = tokens[5].trim();
					int aland = ConvertToInt(tokens[6].trim());
					int awater = ConvertToInt(tokens[7].trim());
					String intptlat = tokens[8].trim();
					String intptlon = tokens[9].trim();
					featureBuilder.add(polygon);
					featureBuilder.add(statefp);
					// featureBuilder.add(countyfp);
					featureBuilder.add(ansicode);
					featureBuilder.add(hydroid);
					featureBuilder.add(fullname);
					featureBuilder.add(mtfcc);
					featureBuilder.add(aland);
					featureBuilder.add(awater);
					featureBuilder.add(intptlat);
					featureBuilder.add(intptlon);
					SimpleFeature feature = featureBuilder.buildFeature(null);
					features.add(feature);
				}
			}
		}
		return features;
	}

	/**
	 * Prompt the user for the name and path to use for the output shapefile
	 * 
	 * @param csvFile
	 *            the input csv file used to create a default shapefile name
	 * 
	 * @return name and path for the shapefile as a new File object
	 */
	private static File getNewShapeFile(File csvFile) {
		String path = csvFile.getAbsolutePath();
		String newPath = path.substring(0, path.length() - 4) + ".shp";

		JFileDataStoreChooser chooser = new JFileDataStoreChooser("shp");
		chooser.setDialogTitle("Save shapefile");
		chooser.setSelectedFile(new File(newPath));

		int returnVal = chooser.showSaveDialog(null);

		if (returnVal != JFileDataStoreChooser.APPROVE_OPTION) {
			// the user cancelled the dialog
			System.exit(0);
		}

		File newFile = chooser.getSelectedFile();
		if (newFile.equals(csvFile)) {
			System.out.println("Error: cannot replace " + csvFile);
			System.exit(0);
		}

		return newFile;
	}

	/*
	 * String转Integer，防止转换溢出
	 */
	private static Integer ConvertToInt(String str) {
		long pre_foo = Long.parseLong(str);
		if (pre_foo < Integer.MIN_VALUE) {
			return Integer.MIN_VALUE;
		} else if (pre_foo > Integer.MAX_VALUE) {
			return Integer.MAX_VALUE;
		} else {
			return (int) pre_foo;
		}
	}

	private static void TestIntersect(FeatureCollection<SimpleFeatureType, SimpleFeature> collection1,
			FeatureCollection<SimpleFeatureType, SimpleFeature> collection2) {
		FeatureIterator<SimpleFeature> features1 = collection1.features();
		FeatureIterator<SimpleFeature> features2 = collection2.features();
		Geometry g1 = null;
		Geometry g2 = null;
		if (features1.hasNext()) {
			SimpleFeature feature = features1.next();
			g1 = (Geometry) feature.getDefaultGeometry();
		}
		if (features2.hasNext()) {
			SimpleFeature feature = features2.next();
			g2 = (Geometry) feature.getDefaultGeometry();
		}

		Geometry g = g1.intersection(g2);

		// System.out.println(isGC);
	}

	private static void TestIntersect(FeatureCollection<SimpleFeatureType, SimpleFeature> collection) throws Exception {
		FeatureIterator<SimpleFeature> features1 = collection.features();
		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();

		Geometry g1 = null;
		Geometry g2 = null;
		String str = "POLYGON ((115.3125 25.3125, 118.125 25.3125, 118.125 23.90625, 115.3125 23.90625, 115.3125 25.3125))";
		WKTReader wktReader = new WKTReader(geometryFactory);
		g2 = wktReader.read(str);
		SimpleFeatureType type = null;
		if (features1.hasNext()) {
			SimpleFeature feature = features1.next();
			g1 = (Geometry) feature.getDefaultGeometry();
			type = feature.getFeatureType();
		}
		SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(type);
		List<SimpleFeature> featureList = new ArrayList<>();

		long startTime = System.currentTimeMillis();
		Geometry g = g1.intersection(g2);
		long endTime = System.currentTimeMillis();
		System.out.println("计算时间：" + (endTime - startTime));
		System.out.println(g.getGeometryType().equals("GeometryCollection"));
		int n = g.getNumGeometries();
		List<Polygon> PolygonList = new ArrayList<Polygon>();
		List<Coordinate> coordinates = new ArrayList<Coordinate>();
		for (int i = 0; i < n; i++) {
			Geometry geo = g.getGeometryN(i);
			String s = geo.getGeometryType().toUpperCase();

			if (s.equals("POLYGON")) {
				if (coordinates.size() >= 1) {
					Coordinate[] coors = new Coordinate[coordinates.size() + 1];
					coors = coordinates.toArray(coors);
					coors[coordinates.size()] = coordinates.get(0);
					PolygonList.add(geometryFactory.createPolygon(coors));
					coordinates = new ArrayList<Coordinate>();
				}
				PolygonList.add((Polygon) geo);
			} else {
				for (Coordinate coor : geo.getCoordinates()) {
					coordinates.add(coor);
				}
			}
		}

		Polygon[] polygons = new Polygon[PolygonList.size()];
		polygons = PolygonList.toArray(polygons);
		MultiPolygon pg = geometryFactory.createMultiPolygon(polygons);
		featureBuilder.add(pg);
		featureList.add(featureBuilder.buildFeature(null));
		String p = String.format("G:\\MyFile\\研究生\\论文\\云计算\\测试数据\\test\\test.shp");
		File f1 = new File(p);
		URL path = f1.toURI().toURL();
		CreateShapefileFromCSV(path, featureList, type);
		// boolean isGC=checkNotGeometryCollection(g);
		// System.out.println(pg.toText());
	}

	private static SimpleFeatureType getFeatureType() throws Exception {
		SimpleFeatureType polygonType = DataUtilities.createType("Grid",
				"the_geom:Polygon:srid=4326," + "GridLevel:Integer," + "X:Integer," + "Y:Integer," + "GridCode:String");
		return polygonType;
	}

}
