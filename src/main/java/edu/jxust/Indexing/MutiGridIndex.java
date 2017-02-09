package edu.jxust.Indexing;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Stack;

import org.geotools.geometry.jts.JTSFactoryFinder;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;

/**
 * @ClassName: MutiGridIndex
 * @Description: 多级网格索引
 * @author 张炫铤
 * @date 2017年1月3日 下午3:55:30
 * 
 */
public class MutiGridIndex {

	public List<Grid> Index(Geometry geometry, Integer lastGridLevel) {
		// List<Grid> grids = new ArrayList<>();
		Grid startGrid = Grid.getGridFromBox(geometry.getEnvelopeInternal());
		return getGridsOnStack(geometry, startGrid, lastGridLevel);
		// return Index(geometry, startGrid, lastGridLevel, grids);
	}

	public List<Grid> getGridsOnRecursive(Geometry geometry, Grid grid, Integer lastGridLevel, List<Grid> grids) {
		System.out.println("网格层级：" + grid.getGridLevel() + " 网格坐标：（" + grid.getGridCoordinate().x + "，"
				+ grid.getGridCoordinate().y + "）");

		if (grid.getGridLevel() < lastGridLevel) {
			if (geometry.contains(grid.getGridGeometry())) {
				grids.add(grid);
				return grids;
			} else {
				TravereseSubGrids(geometry, grid.splitGrid(), lastGridLevel, grids);
			}
		} else if (geometry.intersects(grid.getGridGeometry())) {
			grids.add(grid);
		}
		return grids;
	}

	public void TravereseSubGrids(Geometry geometry, Grid[] splitGrids, Integer lastGridLevel, List<Grid> grids) {
		for (int i = 0; i < 4; i++) {
			if (geometry.intersects(splitGrids[i].getGridGeometry()) == false)
				continue;
			getGridsOnRecursive(intersection(geometry, splitGrids[i].getGridGeometry()), splitGrids[i], lastGridLevel,
					grids);
		}
	}

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

	// 使用堆栈遍历网格
	public List<Grid> getGridsOnStack(Geometry geometry, Grid grid, Integer lastGridLevel) {
		List<Grid> grids = new ArrayList<Grid>();
		Stack<Grid> parentGrid = new Stack<Grid>();
		Stack<Geometry> parentGeometry = new Stack<Geometry>();
		parentGrid.push(grid);
		parentGeometry.push(geometry);
		while (parentGrid.isEmpty() == false) {
			grid = parentGrid.pop();
			geometry = parentGeometry.pop();
			System.out.println("网格层级：" + grid.getGridLevel() + " 网格坐标：（" + grid.getGridCoordinate().x + "，"
					+ grid.getGridCoordinate().y + "）");
			if (grid.getGridLevel() < lastGridLevel) {
				if (geometry.contains(grid.getGridGeometry())) {
					grids.add(grid);
				} else {
					Grid sGrids[] = grid.splitGrid();
					for (Grid subGrid : sGrids) {
						if (geometry.intersects(subGrid.getGridGeometry()) == false)
							continue;
						parentGeometry.push(intersection(geometry, subGrid.getGridGeometry()));
						parentGrid.push(subGrid);
					}
				}
			} else if (geometry.intersects(grid.getGridGeometry())) {
				grids.add(grid);
			}
		}
		return grids;
	}

	// 使用队列遍历网格
	public List<Grid> getGridsOnQueue(Geometry geometry, Grid grid, Integer lastGridLevel) {
		List<Grid> grids = new ArrayList<Grid>();
		Queue<Grid> parentGrid = new LinkedList<Grid>();
		Queue<Geometry> parentGeometry = new LinkedList<Geometry>();
		parentGrid.offer(grid);
		parentGeometry.offer(geometry);
		while (parentGrid.isEmpty() == false) {
			grid = parentGrid.poll();
			geometry = parentGeometry.poll();
			System.out.println("网格层级：" + grid.getGridLevel() + " 网格坐标：（" + grid.getGridCoordinate().x + "，"
					+ grid.getGridCoordinate().y + "）");
			if (grid.getGridLevel() < lastGridLevel) {
				if (geometry.contains(grid.getGridGeometry())) {
					grids.add(grid);
				} else {
					Grid sGrids[] = grid.splitGrid();
					for (Grid subGrid : sGrids) {
						if (geometry.intersects(subGrid.getGridGeometry()) == false)
							continue;
						parentGeometry.offer(intersection(geometry, subGrid.getGridGeometry()));
						parentGrid.offer(subGrid);
					}
				}
			} else if (geometry.intersects(grid.getGridGeometry())) {
				grids.add(grid);
			}
		}
		return grids;
	}

}
