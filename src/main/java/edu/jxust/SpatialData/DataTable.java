/**   
* @Title: DataTable.java 
* @Package edu.jxust.SpatialData 
* @Description: TODO 
* @author 张炫铤  
* @date 2017年2月3日 上午11:09:27 
* @version V1.0   
*/
package edu.jxust.SpatialData;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;

/** 
* @ClassName: DataTable 
* @Description: TODO
* @author 张炫铤
* @date 2017年2月3日 上午11:09:27 
*  
*/
public class DataTable {

	public void importCSVFile(HTableInterface table, String file, String fileHeader) {

	}

	public void insertRecord(HTableInterface table, String rowkey, byte[] family, Map<byte[], byte[]> mapValue)
			throws IOException {
		Put put = new Put(rowkey.getBytes());

		for (Entry<byte[], byte[]> entry : mapValue.entrySet()) {
			put.add(family, entry.getKey(), entry.getValue());
		}

		table.put(put);
	}
}
