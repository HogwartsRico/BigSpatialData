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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;


/** 
* @ClassName: DataTable 
* @Description: TODO
* @author 张炫铤
* @date 2017年2月3日 上午11:09:27 
*  
*/
public class DataTable {

	public static void importNoneIndexData(HTableInterface tableSpatial, HTableInterface tableNoneIndexData)
			throws Exception {
		Scan scan = new Scan();
		ResultScanner scanner = tableSpatial.getScanner(scan);
		Result result = scanner.next();
		int count = 0;
		long total = 0;
		byte[] fDataKey = "DataKey".getBytes();
		byte[] cKey = "Key".getBytes();
		List<Put> puts = new ArrayList<>();
		while (result != null) {
			count++;
			String numStr = StringUtils.leftPad(Long.toString(total), 18, '0');
			Put put = new Put(numStr.getBytes());
			put.add(fDataKey, cKey, result.getRow());
			puts.add(put);

			result = scanner.next();
			if (count >= 10000 && result != null) {
				tableNoneIndexData.put(puts);
				scan.setStartRow(result.getRow());
				scanner.close();
				scanner = tableSpatial.getScanner(scan);
				scanner.next();
				count = 0;
				puts = new ArrayList<>();
				System.out.println(String.format("共导入记录：%s", total));
			}
			total++;
		}
		tableNoneIndexData.put(puts);
		System.out.println(String.format("共导入记录：%s", total));
		scanner.close();
		tableSpatial.close();
		tableNoneIndexData.close();
	}

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
