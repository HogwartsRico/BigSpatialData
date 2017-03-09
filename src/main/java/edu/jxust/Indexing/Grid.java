/**   
* @Title: Grid.java 
* @Package edu.jxust.Indexing 
* @Description: 多级网格空间下网格单元 
* @author 张炫铤  
* @date 2017年1月4日 下午1:34:26 
* @version V1.0   
*/
package edu.jxust.Indexing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.geotools.geometry.jts.JTSFactoryFinder;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

import edu.jxust.Common.Hilbert2D;
import edu.jxust.Common.QueryRowKey;

/**
 * @ClassName: Grid
 * @Description: 网格
 * @author 张炫铤
 * @date 2017年1月4日 下午1:34:26
 * 
 */
public class Grid implements Comparable<Grid> {
	private Integer gridLevel;
	private Coordinate gridCoordinate;
	private Geometry gridGeometry;
	private GridSize gridSize;
	private String gridCode;

	private Integer hilbertNumber = -1;

	public Grid(Integer gridLevel, Coordinate gridCoordinate, Geometry gridGeometry) {
		Initial(gridLevel, gridCoordinate, gridGeometry);
	}

	public Grid(Integer gridLevel, Coordinate gridCoordinate) {
		Initial(gridLevel, gridCoordinate, getGridGeomtry(gridLevel, gridCoordinate));
	}

	/** 
	* <p>Title:Grid </p> 
	* <p>Description:获取点P在gridLevel层级网格空间下单元网格
	*  </p> 
	* @param gridLevel 多级网格空间层级
	* @param p 点空间对象
	*/
	public Grid(Integer gridLevel, Point p) {
		this.gridCoordinate = getGridCoordinate(gridLevel, p);
		Initial(gridLevel, this.gridCoordinate, getGridGeomtry(gridLevel, gridCoordinate));
	}

	public Grid(Integer gridLevel, double x, double y) {
		this.gridCoordinate = getGridCoordinate(gridLevel, x, y);
		Initial(gridLevel, this.gridCoordinate, getGridGeomtry(gridLevel, this.gridCoordinate));
	}

	private void Initial(Integer gridLevel, Coordinate gridCoordinate, Geometry gridGeometry) {
		this.gridLevel = gridLevel;
		this.gridCoordinate = gridCoordinate;
		this.gridGeometry = gridGeometry;
	}

	public Integer getGridLevel() {
		return gridLevel;
	}

	public GridSize getGridSize() {
		if (this.gridSize == null) {
			return this.gridSize = new GridSize(this.gridLevel);
		}
		return this.gridSize;
	}

	public String getGridCode() {
		if (this.gridCode == null)
			return this.gridCode = GridCode.getHilbertCode(this.gridLevel, getHilbertNumber());
		return this.gridCode;
	}

	public Integer getHilbertNumber() {
		if (this.hilbertNumber == -1) {
			Hilbert2D h = new Hilbert2D();
			return h.xy2d((int) this.gridCoordinate.x, (int) this.gridCoordinate.y, this.gridLevel);
		}
		return this.hilbertNumber;
	}

	public void setGridLevel(Integer gridLevel) {
		this.gridLevel = gridLevel;
	}

	public Coordinate getGridCoordinate() {
		return gridCoordinate;
	}

	public void setGridCoordinate(Coordinate gridCoordinate) {
		this.gridCoordinate = gridCoordinate;
	}

	public Geometry getGridGeometry() {
		return gridGeometry;
	}

	public void setGrid(Geometry gridGeometry) {
		this.gridGeometry = gridGeometry;
	}

	/** 
	* @Title: getGridGeomtry 
	* @Description: 根据网格层级以及网格坐标获取网格在地里空间的几何实体
	* @param gridLevel 网格层级
	* @param gridCoor 网格坐标
	* @return 网格在地里空间的几何实体
	* @throws 
	*/
	private Geometry getGridGeomtry(int gridLevel, Coordinate gridCoor) {
		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
		return geometryFactory.createPolygon(getGridMBRCoordinates(gridLevel, gridCoor));
	}

	private Geometry getGridGeomtry(Coordinate gridMBRMaxCoor, Coordinate gridMBRMinCoor) {
		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
		return geometryFactory.createPolygon(getGridMBRCoordinates(gridMBRMaxCoor, gridMBRMinCoor));
	}

	/** 
	* @Title: getGridMBRCoordinates 
	* @Description: 根据网格层级以及网格坐标获取网格在地里空间的几何实体坐标数组
	* @param gridLevel 网格层级
	* @param gridCoor 网格坐标
	* @return 网格在地里空间的几何实体坐标数组
	* @throws 
	*/
	private Coordinate[] getGridMBRCoordinates(int gridLevel, Coordinate gridCoor) {
		return getGridMBRCoordinates(new GridSize(gridLevel), gridCoor);
	}

	/** 
	* @Title: getGridMBRCoordinates 
	* @Description: 根据网格大小以及网格坐标获取网格在地里空间的几何实体坐标数组
	* @param gridSize 网格大小
	* @param gridCoor 网格坐标
	* @return 网格在地里空间的几何实体坐标数组
	* @throws 
	*/
	private Coordinate[] getGridMBRCoordinates(GridSize gridSize, Coordinate gridCoor) {
		Coordinate leftTopCoor = new Coordinate(gridCoor.x * gridSize.getLength() - 180,
				90 - gridCoor.y * gridSize.getBreadth());
		Coordinate rightTopCoor = new Coordinate(leftTopCoor.x + gridSize.getLength(), leftTopCoor.y);
		Coordinate leftBottomCoor = new Coordinate(leftTopCoor.x, leftTopCoor.y - gridSize.getBreadth());
		return getGridMBRCoordinates(rightTopCoor, leftBottomCoor);
	}

	/** 
	* @Title: getGridMBRCoordinates 
	* @Description: 根据网格在地里空间几何实体最大、最小坐标对获取网格在地里空间的几何实体坐标数组
	* @param gridMBRMaxCoor 网格在地里空间几何实体最大坐标对，即右上角坐标
	* @param gridMBRMinCoor 网格在地里空间几何实体最小坐标对，即左下角坐标
	* @return 网格在地里空间的几何实体坐标数组
	* @throws 
	*/
	private Coordinate[] getGridMBRCoordinates(Coordinate gridMBRMaxCoor, Coordinate gridMBRMinCoor) {
		Coordinate gridMBRCoordinates[] = new Coordinate[5];
		Coordinate leftTopCoor = new Coordinate(gridMBRMinCoor.x, gridMBRMaxCoor.y);
		gridMBRCoordinates[0] = leftTopCoor;
		gridMBRCoordinates[1] = gridMBRMaxCoor;
		gridMBRCoordinates[2] = new Coordinate(gridMBRMaxCoor.x, gridMBRMinCoor.y);
		gridMBRCoordinates[3] = gridMBRMinCoor;
		gridMBRCoordinates[4] = leftTopCoor;
		return gridMBRCoordinates;
	}

	/** 
	* @Title: splitGrid 
	* @Description: 将平均分割四个网格
	* @return 分割网格数组
	* @throws 
	*/
	public Grid[] splitGrid() {
		Grid grids[] = new Grid[4];
		double deltaX = this.gridCoordinate.x * 2;
		double deltaY = this.gridCoordinate.y * 2;
		int gLevel = this.gridLevel + 1;
		Envelope gridEnvelope = this.gridGeometry.getEnvelopeInternal();
		Coordinate gridMBRcentreCoor = gridEnvelope.centre();
		Coordinate gridMBRminCoor = new Coordinate(gridEnvelope.getMinX(), gridEnvelope.getMinY());
		Coordinate gridMBRmaxCoor = new Coordinate(gridEnvelope.getMaxX(), gridEnvelope.getMaxY());
		Geometry leftTopGridGeo = getGridGeomtry(new Coordinate(gridMBRcentreCoor.x, gridMBRmaxCoor.y),
				new Coordinate(gridMBRminCoor.x, gridMBRcentreCoor.y));
		Geometry rightTopGridGeo = getGridGeomtry(gridMBRmaxCoor, gridMBRcentreCoor);
		Geometry rightBottomGridGeo = getGridGeomtry(new Coordinate(gridMBRmaxCoor.x, gridMBRcentreCoor.y),
				new Coordinate(gridMBRcentreCoor.x, gridMBRminCoor.y));
		Geometry leftBottomGridGeo = getGridGeomtry(gridMBRcentreCoor, gridMBRminCoor);
		Grid leftTopGrid = new Grid(gLevel, new Coordinate(deltaX, deltaY), leftTopGridGeo);
		Grid rightTopGrid = new Grid(gLevel, new Coordinate(deltaX + 1, deltaY), rightTopGridGeo);
		Grid rightBottomGrid = new Grid(gLevel, new Coordinate(deltaX + 1, deltaY + 1), rightBottomGridGeo);
		Grid leftBottomGrid = new Grid(gLevel, new Coordinate(deltaX, deltaY + 1), leftBottomGridGeo);
		grids[0] = leftTopGrid;
		grids[1] = rightTopGrid;
		grids[2] = rightBottomGrid;
		grids[3] = leftBottomGrid;
		return grids;
	}

	/**
	 * @Title: getGridFromBox 
	 * @Description: 获取多级网格空间中包含外包矩形Envelope最小网格
	 * @param envelope 空间对象外包矩形
	 * @return Geometry 多级网格空间的网格MBR 
	 * @throws
	 */
	public static Grid getGridFromBox(Envelope envelope) {
		double deltaX = envelope.getMaxX() - envelope.getMinX();
		double deltaY = envelope.getMaxY() - envelope.getMinY();
		int bLevel = getLevelFromDeltaX(deltaX);
		int lLevel = getLevelFromDeltaY(deltaY);
		int gridLevel = bLevel <= lLevel ? bLevel : lLevel;
		Coordinate maxCoordinate = new Coordinate(envelope.getMaxX(), envelope.getMaxY());
		Coordinate minCoordinate = new Coordinate(envelope.getMinX(), envelope.getMinY());
		return getGridFromBox(maxCoordinate, minCoordinate, gridLevel);

	}

	/**
	 * @Title: getGridFromBox 
	 * @Description:计算由两个坐标点围成矩形所坐落多级网格空间中的网格坐标，该网格完全包含两个坐标点围成矩形
	 * @param coor1 外包矩形右上角坐标或左下角坐标
	 * @param coor2 外包矩形左下角坐标或右上角坐标
	 * @param gridLevel 
	 * @return Grid 多级网格空间单元网格
	 * @throws
	 */
	public static Grid getGridFromBox(Coordinate coor1, Coordinate coor2, int gridLevel) {

		GridSize size = new GridSize(gridLevel);
		Coordinate gridCoor1 = getGridCoordinate(coor1, size);
		Coordinate gridCoor2 = getGridCoordinate(coor2, size);

		if (gridCoor1.equals2D(gridCoor2) == true) {
			return new Grid(gridLevel, gridCoor1);
		}
		return getGridFromBox(coor1, coor2, gridLevel - 1);
	}

	/**
	 * 
	 * 返回的网格层级中的网格不一定存在该层级网格单元完全包含delta所构成的范围
	 * 
	 * @Title: getLevel 
	 * @Description: 获取多级网格空间下包含指定范围层级
	 * @param size多级网格空间总宽度或长度
	 * @param delta 空间对象外包矩形长或宽 
	 * @return gridLevel 能包含指定范围deltaX最小网格单元层级 
	 * @throws
	 */
	private static int getLevel(double size, double delta) {
		double level = Math.log(size / delta) / Math.log(2);
		return (int) Math.floor(level);
	}

	/**
	 * 返回的网格层级中的网格不一定存在该层级网格单元完全包含deltaX所构成的范围
	 * @Title: getLevelFromDeltaX 
	 * @Description:获取网格空间下X轴中能包含指定宽度范围deltaX最小网格单元层级 
	 * @param deltaX 空间对象外包矩形X轴宽度 
	 * @return gridLevel 能包含指定范围deltaX最小网格单元层级 
	 * @throws
	 */
	public static int getLevelFromDeltaX(double deltaX) {
		return getLevel(360, deltaX);
	}

	/**
	 * 返回的网格层级中的网格不一定存在该层级网格单元完全包含deltaY所构成的范围
	 * @Title: getLevelFromDeltaY 
	 * @Description: 获取网格空间下Y轴中能包含指定长度范围deltaY最下网格单元层级 
	 * @param deltaY 空间对象外包矩形Y轴长度 
	 * @return gridLevel 能包含指定范围deltaY最小网格单元层级 
	 * @throws
	 */
	public static int getLevelFromDeltaY(double deltaY) {
		return getLevel(180, deltaY);
	}

	/**
	 * @Title: getGridCoordinate 
	 * @Description: 根据网格大小，计算坐标所在多级网格空间中网格坐标
	 * @param coor 
	 * @param size 
	 * @return Coordinate 多级网格空间中的网格坐标
	 * @throws
	 */
	public static Coordinate getGridCoordinate(Coordinate coor, GridSize size) {
		double row = Math.floor((90 - coor.y) / size.getBreadth());
		double col = Math.floor((180 + coor.x) / size.getLength());
		return new Coordinate(col, row);
	}

	public static Coordinate getGridCoordinate(Integer gridLevel, Coordinate coor) {
		return getGridCoordinate(gridLevel, coor.x, coor.y);
	}

	public static Coordinate getGridCoordinate(Integer gridLevel, Point p) {
		return getGridCoordinate(gridLevel, p.getX(), p.getY());
	}

	/** 
	* @Title: getGridCoordinate 
	* @Description: TODO
	* @param gridLevel
	* @param x 即 Longitude
	* @param y 即 Latitude
	* @return
	* @throws 
	*/
	public static Coordinate getGridCoordinate(Integer gridLevel, double x, double y) {
		GridSize gridSize = new GridSize(gridLevel);
		int row = getGridY(gridSize, y);
		int col = getGridX(gridSize, x);
		return new Coordinate(col, row);
	}

	public static Integer getGridX(Integer gridLevel, double x) {
		GridSize gridSize = new GridSize(gridLevel);
		return getGridX(gridSize, x);
	}

	public static Integer getGridX(GridSize gridSize, double x) {
		double col = Math.floor((180 + x) / gridSize.getLength());
		return (int) col;
	}

	public static Integer getGridY(Integer gridLevel, double y) {
		GridSize gridSize = new GridSize(gridLevel);
		return getGridX(gridSize, y);
	}

	public static Integer getGridY(GridSize gridSize, double y) {
		double row = Math.floor((90 - y) / gridSize.getBreadth());
		return (int) row;
	}

	/*
	 * Title: compareTo Description:
	 * 
	 * @param o
	 * 
	 * @return
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(Grid otherGrid) {
		int cop = this.getGridLevel() - otherGrid.getGridLevel();
		if (cop != 0)
			return cop;
		else
			return this.getHilbertNumber().compareTo(otherGrid.getHilbertNumber());

	}

	public static Map<QueryRowKey, Integer> getMapQueryRowkeys(List<Grid> grids) {
		Map<QueryRowKey, Integer> mapCodes = new HashMap<>();
		Map<Integer, List<Integer>> mapGrids = new HashMap<>();
		for (Grid grid : grids) {
			List<Integer> girdsHilbertNum = mapGrids.get(grid.getGridLevel());
			if (girdsHilbertNum == null) {
				girdsHilbertNum = new ArrayList<>();
				mapGrids.put(grid.getGridLevel(), girdsHilbertNum);
			}
			girdsHilbertNum.add(grid.getHilbertNumber());
		}
		Set<Integer> keySet = mapGrids.keySet();
		Iterator<Integer> iter = keySet.iterator();
		while (iter.hasNext()) {
			int key = iter.next();
			List<Integer> hNums = mapGrids.get(key);

			if (hNums.size() == 1) {
				String startRowKey = GridCode.getHilbertCode(key, hNums.get(0));
				String stopRowKey = GridCode.getHilbertCode(key, hNums.get(0) + 1);
				mapCodes.put(new QueryRowKey(startRowKey, stopRowKey), key);
				continue;
			}
			Collections.sort(hNums);

			int startNum = hNums.get(0);
			int endNum = hNums.get(0);
			for (int i = 1; i < hNums.size(); i++) {
				int hLast = hNums.get(i) - endNum;
				endNum = hNums.get(i);
				if (hLast >= 2) {
					mapCodes.put(new QueryRowKey(GridCode.getHilbertCode(key, startNum),
							GridCode.getHilbertCode(key, endNum)), key);
					startNum = endNum;
				}
				if (i + 1 < hNums.size()) {
					continue;
				}

				mapCodes.put(new QueryRowKey(GridCode.getHilbertCode(key, startNum),
						GridCode.getHilbertCode(key, endNum + 1)), key);
			}
		}

		return mapCodes;
	}

}
