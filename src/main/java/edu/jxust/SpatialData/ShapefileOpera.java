package edu.jxust.SpatialData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
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

}
