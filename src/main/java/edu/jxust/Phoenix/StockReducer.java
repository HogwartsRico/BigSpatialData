/**   
* @Title: StockReducer.java 
* @Package edu.jxust.Phoenix 
* @Description: TODO 
* @author 张炫铤  
* @date 2017年3月6日 下午5:20:21 
* @version V1.0   
*/
package edu.jxust.Phoenix;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** 
* @ClassName: StockReducer 
* @Description: TODO
* @author 张炫铤
* @date 2017年3月6日 下午5:20:21 
*  
*/
public class StockReducer extends Reducer<Text, DoubleWritable, NullWritable , StockWritable>{
	@Override
    protected void reduce(Text key, Iterable<DoubleWritable> recordings, Context context) throws IOException, InterruptedException {
       double maxPrice = Double.MIN_VALUE;
       for(DoubleWritable recording : recordings) {
         if(maxPrice < recording.get()) {
          maxPrice = recording.get(); 
         }
       } 
        final StockWritable stock = new StockWritable();
        stock.setStockName(key.toString());
       stock.setMaxPrice(maxPrice);
        context.write(NullWritable.get(),stock);
    }
}
