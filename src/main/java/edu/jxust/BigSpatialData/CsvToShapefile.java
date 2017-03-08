package edu.jxust.BigSpatialData;

import edu.jxust.SpatialData.ShapefileOpera;

public class CsvToShapefile {

	public static void main(String[] args) throws Exception {
		try{
			// TODO Auto-generated method stub
			String data = "J:\\美国开放矢量数据\\AREALM\\AREALM.csv";
			Long start = System.currentTimeMillis();
			ShapefileOpera.convertCSVtoShapefile(data, "\\\t", "J:\\美国开放矢量数据\\AREALM\\AREALM.shp");
			System.out.println(String.format("导入时间：%s", System.currentTimeMillis() - start));

		}catch(Exception e){
			e.printStackTrace();
		}
	}

}
