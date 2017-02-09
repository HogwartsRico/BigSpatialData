/**   
* @Title: GeometryDef.java 
* @Package edu.jxust.Geometry 
* @Description: TODO 
* @author 张炫铤  
* @date 2017年2月6日 上午10:59:27 
* @version V1.0   
*/
package edu.jxust.Geometry;

import com.vividsolutions.jts.geom.Geometry;

/** 
* @ClassName: GeometryDef 
* @Description: TODO
* @author 张炫铤
* @date 2017年2月6日 上午10:59:27 
*  
*/
public class GeometryDef {
	public static int getGeometryType(Geometry g) {
		if (g.getGeometryType().equals("Point"))
			return 1;
		if (g.getGeometryType().equals("LineString"))
			return 2;
		if (g.getGeometryType().equals("LinearRing"))
			return 3;
		if (g.getGeometryType().equals("Polygon"))
			return 4;
		if (g.getGeometryType().equals("MultiPoint"))
			return 5;
		if (g.getGeometryType().equals("MultiLineString"))
			return 6;
		if (g.getGeometryType().equals("MultiPolygon"))
			return 7;
		if (g.getGeometryType().equals("GeometryCollection"))
			return 8;
		return 0;
	}
}
