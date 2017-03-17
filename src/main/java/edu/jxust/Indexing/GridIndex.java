/**   
* @Title: GridIndex.java 
* @Package edu.jxust.Indexing 
* @Description: TODO 
* @author 张炫铤  
* @date 2017年3月17日 下午4:56:55 
* @version V1.0   
*/
package edu.jxust.Indexing;

import java.util.ArrayList;
import java.util.List;

import org.geotools.geometry.jts.JTSFactoryFinder;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

/** 
* @ClassName: GridIndex 
* @Description: TODO
* @author 张炫铤
* @date 2017年3月17日 下午4:56:55 
*  
*/
public class GridIndex {

	/** 
	* @Title: getIndexGrids 
	* @Description: 网格索引
	* @param geo
	* @param gridLevel 网格索引层级，
	* @return
	* @throws 
	*/
	public static List<Grid> getIndexGrids(Geometry geo, Integer gridLevel) {
		Envelope envelope = geo.getEnvelopeInternal();
		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();

		Point rightTopPoint = geometryFactory.createPoint(new Coordinate(envelope.getMaxX(), envelope.getMaxY()));
		Point leftBottompoint = geometryFactory.createPoint(new Coordinate(envelope.getMinX(), envelope.getMinY()));
		List<Grid> grids = new ArrayList<>();
		Grid gridMax = new Grid(gridLevel, rightTopPoint);
		Grid gridmin = new Grid(gridLevel, leftBottompoint);
		Coordinate maxCoordinate = gridMax.getGridCoordinate();
		Coordinate minCoordinate = gridmin.getGridCoordinate();

		int maxX = (int) maxCoordinate.x;
		int maxY = (int) maxCoordinate.y;
		int minX = (int) minCoordinate.x;
		int minY = (int) minCoordinate.y;

		if (maxX < minX) {
			int tmp = maxX;
			maxX = minX;
			minX = tmp;
		}
		if (maxY < minY) {
			int tmp = maxY;
			maxY = minY;
			minY = tmp;
		}

		for (int x = minX; x <= maxX; x++) {
			for (int y = minY; y <= maxY; y++) {
				Grid grid = new Grid(gridLevel, new Coordinate(x, y));
				if (geo.intersects(grid.getGridGeometry())) {
					grids.add(grid);
				}
			}
		}
		return grids;
	}
}
