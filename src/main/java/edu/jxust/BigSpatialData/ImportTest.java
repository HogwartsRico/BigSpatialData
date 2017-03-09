/**   
* @Title: ImportTest.java 
* @Package edu.jxust.BigSpatialData 
* @Description: TODO 
* @author 张炫铤  
* @date 2017年3月5日 下午4:48:48 
* @version V1.0   
*/
package edu.jxust.BigSpatialData;

import java.sql.Connection;

import org.apache.log4j.Logger;

import edu.jxust.Common.DBUtil;


/** 
* @ClassName: ImportTest 
* @Description: TODO
* @author 张炫铤
* @date 2017年3月5日 下午4:48:48 
*  
*/
public class ImportTest {

	/**
	 * @throws Exception  
	* @Title: main 
	* @Description: TODO
	* @param args
	* @throws 
	*/
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Logger log=Logger.getLogger(ImportTest.class);		
		String data = "G:\\MyFile\\研究生\\论文\\云计算\\测试数据\\AREALM.csv";
		ImportCsvData importData=new ImportCsvData(data);
		Connection con = DBUtil.getConnection();
		Long start=System.currentTimeMillis();
		importData.importData("\\\t","AREALM", con);
		log.info(String.format("导入时间：", System.currentTimeMillis()-start));	
	}

}
