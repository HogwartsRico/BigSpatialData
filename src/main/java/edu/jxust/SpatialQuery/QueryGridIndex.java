/**   
* @Title: QueryGridIndex.java 
* @Package edu.jxust.SpatialQuery 
* @Description: TODO 
* @author 张炫铤  
* @date 2017年3月18日 上午12:40:34 
* @version V1.0   
*/
package edu.jxust.SpatialQuery;

import java.util.Map;

import com.vividsolutions.jts.geom.Geometry;

import edu.jxust.Common.QueryRowKey;
import edu.jxust.Indexing.Grid;
import edu.jxust.Indexing.GridIndex;

/** 
* @ClassName: QueryGridIndex 
* @Description: 查询网格索引，查询算法与多级网格索引不同，但同样采取网格编码合并策略
* @author 张炫铤
* @date 2017年3月18日 上午12:40:34 
*  
*/
public class QueryGridIndex {
	public static Map<QueryRowKey, Integer> getMapGridIndexCodes(Geometry geo, int gridLevel) {
		return Grid.getMapQueryRowkeys(GridIndex.getIndexGrids(geo, gridLevel));
	}
}
