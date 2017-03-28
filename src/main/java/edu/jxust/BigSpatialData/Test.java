/**   
* @Title: Test.java 
* @Package edu.jxust.BigSpatialData 
* @Description: TODO 
* @author 张炫铤  
* @date 2017年3月6日 下午6:36:59 
* @version V1.0   
*/
package edu.jxust.BigSpatialData;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.geotools.geojson.geom.GeometryJSON;
import org.geotools.geometry.jts.JTSFactoryFinder;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.WKTReader;

import edu.jxust.Common.QueryRowKey;
import edu.jxust.Indexing.Grid;

/**
 * @ClassName: Test
 * @Description: TODO
 * @author 张炫铤
 * @date 2017年3月6日 下午6:36:59
 * 
 */
public class Test {
	public static void main(String[] args) throws Exception {
		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
		WKTReader wktReader = new WKTReader(geometryFactory);
		String geoWKT = "POLYGON ((-110.92 -61.22, -110.92 65.18, 49.01 65.18, 49.01 -61.22, -110.92 -61.22))";
		Geometry geo = wktReader.read(geoWKT);
		GeometryJSON gJson = new GeometryJSON();
		String geoJson = gJson.toString(geo);
		Grid g = new Grid(16, new Coordinate(10, 16));
		Grid g1 = new Grid(16, new Coordinate(10, 17));
		Grid g2 = new Grid(16, new Coordinate(11, 16));

		Grid g3 = new Grid(15, new Coordinate(11, 18));
		Grid g4 = new Grid(15, new Coordinate(11, 19));
		Grid g5 = new Grid(15, new Coordinate(11, 20));
		Grid g6 = new Grid(7, new Coordinate(11, 16));
		Grid g7 = new Grid(15, new Coordinate(11, 18));
		Grid g8 = new Grid(16, new Coordinate(12, 17));
		Grid g9 = new Grid(16, new Coordinate(19, 15));
		List<Grid> grids = new ArrayList<>();
		grids.add(g);
		grids.add(g1);
		grids.add(g2);
		grids.add(g3);
		grids.add(g4);
		grids.add(g5);
		grids.add(g6);
		grids.add(g7);
		grids.add(g8);
		grids.add(g9);
		Map<QueryRowKey, Integer> map = Grid.getMapQueryRowkeys(grids);

		for (Map.Entry<QueryRowKey, Integer> entry : map.entrySet()) {
			System.out.println(String.format("层级：%s，起始编码：%s，终止编码：%s", entry.getValue(), entry.getKey().getStartRow(),
					entry.getKey().getStopRow()));
		}
		Collections.sort(grids);
		for (Grid gg : grids) {
			System.out.println(
					String.format("层级：%s，编码：%s,网格编码：%s", gg.getGridLevel(), gg.getHilbertNumber(), gg.getGridCode()));
		}
		Logger log = Logger.getLogger(Test.class);
		String dataFile = "G:\\MyFile\\研究生\\论文\\云计算\\测试数据\\AREALM.csv";
		try (BufferedReader reader = new BufferedReader(new FileReader(dataFile))) {

			String line;
			int count = 0;
			while ((line = reader.readLine()) != null) {
				if (count >= 10)
					break;
				log.info(line);
			}
		} catch (Exception exc) {
			exc.printStackTrace();
		}
	}

}
