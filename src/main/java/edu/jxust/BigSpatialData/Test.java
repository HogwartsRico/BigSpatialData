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

import org.apache.log4j.Logger;

/** 
* @ClassName: Test 
* @Description: TODO
* @author 张炫铤
* @date 2017年3月6日 下午6:36:59 
*  
*/
public class Test {
	public static void main(String[] args) throws Exception {
		Logger log=Logger.getLogger(Test.class);
		String dataFile = "G:\\MyFile\\研究生\\论文\\云计算\\测试数据\\AREALM.csv";
		try (BufferedReader reader = new BufferedReader(new FileReader(dataFile))) {

			String line;
			int count=0;
			while ((line = reader.readLine()) != null) {
				if(count>=10)
					break;
				log.info(line);
			}
		}catch(Exception exc){
			exc.printStackTrace();
		}
	}

}
