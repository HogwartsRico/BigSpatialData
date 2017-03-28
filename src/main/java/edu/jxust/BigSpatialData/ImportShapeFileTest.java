package edu.jxust.BigSpatialData;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.jxust.Common.DBUtil;
import edu.jxust.SpatialData.ImportXZTB;

public class ImportShapeFileTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Logger log=Logger.getLogger(ImportTest.class);		
		String data = "J:\\浙江省土地利用规划数据\\综合数据\\XZTB.shp";
		Map<String, Object> mapFields=new HashMap<>();
		mapFields.put("MBBSM", Integer.class);
		mapFields.put("YSDM", String.class);
		mapFields.put("DLDM", String.class);
		mapFields.put("DLMC", String.class);
		mapFields.put("QSXZ", String.class);
		mapFields.put("SQBM", String.class);
		mapFields.put("SQMC", String.class);
		mapFields.put("ZQBM", String.class);
		mapFields.put("ZQMC", String.class);
		mapFields.put("TBBH", String.class);
		mapFields.put("PDJB", String.class);
		
		mapFields.put("TKXS", Double.class);
		mapFields.put("BSMJ", Double.class);
		mapFields.put("KSXM", Double.class);		
		mapFields.put("KLWM", Double.class);
		mapFields.put("KKSM", Double.class);
		mapFields.put("BSJM", Double.class);		
		Connection con = DBUtil.getConnection();
		Long start=System.currentTimeMillis();
		try {
			ImportXZTB.importData(data, mapFields, "XZTB", 0, con);
		} catch (Exception e) {
			// TODO Auto-generated catch block			
			log.error(e);;
		}
		log.info(String.format("导入时间：%s", System.currentTimeMillis()-start));	
	}

}
