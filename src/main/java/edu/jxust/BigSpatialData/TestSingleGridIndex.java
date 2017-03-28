/**   
* @Title: TestSingleGridIndex.java 
* @Package edu.jxust.BigSpatialData 
* @Description: TODO 
* @author 张炫铤  
* @date 2017年3月17日 下午6:30:43 
* @version V1.0   
*/
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

/**
 * @ClassName: TestSingleGridIndex
 * @Description: 测试网格索引构建
 * @author 张炫铤
 * @date 2017年3月17日 下午6:30:43
 * 
 */
public class TestSingleGridIndex {

	/**
	 * @Title: main @Description: 测试网格索引构建 @param args @throws
	 */
	public static void main(String[] args) {
		readZhejiangProvince();
	}

	public static void testGridIndex() {
		String path = "G:\\MyFile\\研究生\\论文\\毕业论文\\大论文\\测试数据\\Export_Output_7.shp";
		String outputPath = "G:\\MyFile\\研究生\\论文\\毕业论文\\大论文\\测试数据\\Export_Output_7-Grid.shp";
		readShapeFile(path, outputPath);
		System.out.println("Done");
	}

	public static void readZhejiangProvince() {
		String path = "G:\\张炫铤_工作空间\\测试数据\\data\\浙江省行政区划.shp";
		String outputPath = "G:\\张炫铤_工作空间\\测试数据\\outputData\\浙江省行政区划-Grid.shp";
		writeShapeFile(path, outputPath);
		System.out.println("Done");
	}

	public static Geometry getDefaultGeo(String filePath) {
		ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();
		try {
			ShapefileDataStore sds = (ShapefileDataStore) dataStoreFactory
					.createDataStore(new File(filePath).toURI().toURL());
			sds.setCharset(Charset.forName("GBK"));
			SimpleFeatureSource featureSource = sds.getFeatureSource();
			SimpleFeatureIterator itertor = featureSource.getFeatures().features();

			if (itertor.hasNext()) {
				return (Geometry) itertor.next().getDefaultGeometry();
			} else {
				return null;
			}

		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public static void writeShapeFile(String path, String outputPath) {
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
				Grid startGrid = Grid.getGridFromBox(geo.getEnvelopeInternal());
				SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(ftype);
				featureBuilder.add(startGrid.getGridGeometry());
				featureBuilder.add(startGrid.getGridLevel());
				featureBuilder.add((int) startGrid.getGridCoordinate().x);
				featureBuilder.add((int) startGrid.getGridCoordinate().y);
				featureBuilder.add(startGrid.getGridCode());
				featureList.add(featureBuilder.buildFeature(null));

			}

			ShapefileOpera.CreateShapefile(new File(outputPath).toURI().toURL(), featureList, ftype);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void readShapeFile(String path, String outputPath) {
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
				List<Grid> grids = edu.jxust.Indexing.GridIndex.getIndexGrids(geo, 16);
				// List<Grid> grids=edu.jxust.Indexing.MutiGridIndex.Index(geo,
				// geo.getEnvelope(), 16);
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
