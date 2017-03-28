package edu.jxust.BigSpatialData;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.geotools.data.DataUtilities;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Geometry;

import edu.jxust.Indexing.Grid;
import edu.jxust.SpatialData.ShapefileOpera;

public class TestCreateMutiGrid {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int level = 18;
		String path = "G:\\张炫铤_工作空间\\测试数据\\MutiGridCreateTest\\半山村_WGS_1984.shp";
		String outputPath = String.format("G:\\张炫铤_工作空间\\测试数据\\MutiGridCreateTest\\半山村_WGS_1984-%s_Grid.shp", level);
		createMutiGridFromShapeFile(path, outputPath, level);
		System.out.println("Done");
	}

	public static void createMutiGridFromShapeFile(String path, String outputPath, int level) {
		ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();
		try {
			ShapefileDataStore sds = (ShapefileDataStore) dataStoreFactory
					.createDataStore(new File(path).toURI().toURL());
			sds.setCharset(Charset.forName("UTF-8"));
			SimpleFeatureSource featureSource = sds.getFeatureSource();
			SimpleFeatureIterator itertor = featureSource.getFeatures().features();

			List<SimpleFeature> featureList = new ArrayList<>();
			SimpleFeatureType ftype = getFeatureType();
			while (itertor.hasNext()) {
				SimpleFeature feature = itertor.next();
				Geometry geo = (Geometry) feature.getDefaultGeometry();
				List<Grid> grids = edu.jxust.Indexing.MutiGridIndex.Index(geo, geo.getEnvelope(), level);
				for (Grid grid : grids) {
					SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(ftype);
					featureBuilder.add(grid.getGridGeometry());
					featureBuilder.add(grid.getGridLevel());
					featureBuilder.add((int) grid.getGridCoordinate().x);
					featureBuilder.add((int) grid.getGridCoordinate().y);
					featureBuilder.add(grid.getGridCode());
					featureList.add(featureBuilder.buildFeature(null));
				}
			}

			ShapefileOpera.CreateShapefile(new File(outputPath).toURI().toURL(), featureList, ftype);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static SimpleFeatureType getFeatureType() throws Exception {
		SimpleFeatureType polygonType = DataUtilities.createType("Grid",
				"the_geom:Polygon:srid=4326," + "GridLevel:Integer," + "X:Integer," + "Y:Integer," + "GridCode:String");
		return polygonType;
	}

}
