/**   
* @Title: QueryGeometry.java 
* @Package edu.jxust.SpatialQuery 
* @Description: TODO 
* @author 张炫铤  
* @date 2017年3月7日 下午1:52:51 
* @version V1.0   
*/
package edu.jxust.SpatialQuery;

import java.util.ArrayList;
import java.util.List;
import org.geotools.geometry.jts.JTSFactoryFinder;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import edu.jxust.Indexing.Grid;

/** 
* @ClassName: QueryGeometry 
* @Description: TODO
* @author 张炫铤
* @date 2017年3月7日 下午1:52:51 
*  
*/
public class QueryGeometry {
	List<Grid> lastLevelGrids;
	List<String> lastLevelGridCodes;
	
	public List<Grid> getLastLevelGrids() {		
		return this.lastLevelGrids;
	}
	public List<String> getLastLevelGridCodes() {
		return this.lastLevelGridCodes;
	}
	/** 
	* @Title: Index 
	* @Description: 多级网格索引分治策略剖分
	* @param geometry
	* @param mbr
	* @param lastGridLevel
	* @return
	* @throws 
	*/
	public List<Grid> getIndexGrids(Geometry queryGeometry, Integer startLevel, Integer lastGridLevel) {
		List<Grid> grids = new ArrayList<>();
		lastLevelGrids = new ArrayList<>();
		Grid startGrid = Grid.getGridFromBox(queryGeometry.getEnvelopeInternal());
		int geoStartLevel = startGrid.getGridLevel();
		if (geoStartLevel > startLevel) {
			Point centerPoint = queryGeometry.getCentroid();
			for (int i = geoStartLevel; i < startLevel; i++) {
				grids.add(new Grid(i, centerPoint));
			}
		}
		return getGridsOnRecursive(queryGeometry, startGrid, lastGridLevel, grids);
	}

	public List<String> getIndexGridCodes(Geometry queryGeometry, Integer startLevel, Integer lastGridLevel) {
		List<String> grids = new ArrayList<>();
		lastLevelGridCodes = new ArrayList<>();
		Grid startGrid = Grid.getGridFromBox(queryGeometry.getEnvelopeInternal());
		int geoStartLevel = startGrid.getGridLevel();
		if (geoStartLevel > startLevel) {
			Point centerPoint = queryGeometry.getCentroid();
			for (int i = geoStartLevel; i < startLevel; i++) {
				grids.add(new Grid(i, centerPoint).getGridCode());
			}
		}
		return getGridCodesOnRecursive(queryGeometry, startGrid, lastGridLevel, grids);
	}

	

	/** 
	* @Title: getGridsOnRecursive 
	* @Description: 递归遍历网格,采用分治策略进行多级网格索引构建
	* @param geometry 几何
	* @param grid 网格
	* @param lastGridLevel 终止层级
	* @param grids 网格集合
	* @return
	* @throws 
	*/
	private List<Grid> getGridsOnRecursive(Geometry geometry, Grid grid, Integer lastGridLevel, List<Grid> grids) {
		if (grid.getGridLevel() < lastGridLevel) {
			if (geometry.contains(grid.getGridGeometry())) {
				grids.add(grid);
				return grids;
			} else {
				TravereseSubGrids(geometry, grid.splitGrid(), lastGridLevel, grids);
			}
		} else if (grid.getGridGeometry().intersects(geometry)) {//
			if (geometry.contains(grid.getGridGeometry())) {
				grids.add(grid);
			} else
				lastLevelGrids.add(grid);// ||
		}
		return grids;
	}

	private List<String> getGridCodesOnRecursive(Geometry geometry, Grid grid, Integer lastGridLevel,
			List<String> gridCodes) {
		if (grid.getGridLevel() < lastGridLevel) {
			if (geometry.contains(grid.getGridGeometry())) {
				gridCodes.add(grid.getGridCode());
				return gridCodes;
			} else {
				TravereseSubGridCodes(geometry, grid.splitGrid(), lastGridLevel, gridCodes);
			}
		} else if (grid.getGridGeometry().intersects(geometry)) {//
			if (geometry.contains(grid.getGridGeometry())) {
				gridCodes.add(grid.getGridCode());
			} else
				lastLevelGridCodes.add(grid.getGridCode());// ||
		}
		return gridCodes;
	}

	/** 
	* @Title: TravereseSubGrids 
	* @Description: 遍历子网格
	* @param geometry 待剖分的几何
	* @param splitGrids 分割网格数组
	* @param lastGridLevel 终止网格层级
	* @param grids 网格集合
	* @throws 
	*/
	private void TravereseSubGrids(Geometry geometry, Grid[] splitGrids, Integer lastGridLevel, List<Grid> grids) {
		for (int i = 0; i < 4; i++) {
			if (geometry.intersects(splitGrids[i].getGridGeometry()) == false)
				continue;
			getGridsOnRecursive(intersection(geometry, splitGrids[i].getGridGeometry()), splitGrids[i], lastGridLevel,
					grids);
		}
	}

	private void TravereseSubGridCodes(Geometry geometry, Grid[] splitGrids, Integer lastGridLevel,
			List<String> gridCodes) {
		for (int i = 0; i < 4; i++) {
			if (geometry.intersects(splitGrids[i].getGridGeometry()) == false)
				continue;
			getGridCodesOnRecursive(intersection(geometry, splitGrids[i].getGridGeometry()), splitGrids[i],
					lastGridLevel, gridCodes);
		}
	}

	/** 
	* @Title: intersection 
	* @Description: 取两个Geometry交集
	* @param geometry1
	* @param geometry2
	* @return 相交部分Geometry
	* @throws 
	*/
	private Geometry intersection(Geometry geometry1, Geometry geometry2) {
		Geometry geometry = geometry1.intersection(geometry2);
		if (geometry.getGeometryType().equals("GeometryCollection") == false)
			return geometry;
		int n = geometry.getNumGeometries();
		List<Polygon> polygonList = new ArrayList<Polygon>();
		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
		for (int i = 0; i < n; i++) {
			Geometry g = geometry.getGeometryN(i);
			if (g.getGeometryType().equals("Polygon"))
				polygonList.add((Polygon) g);
			// if (g.getGeometryType().toUpperCase().equals("MULTIPOLYGON"))
			// System.out.println("MULTIPOLYGON");
		}
		Polygon[] polygons = new Polygon[polygonList.size()];
		return geometryFactory.createMultiPolygon(polygonList.toArray(polygons));
	}

}
