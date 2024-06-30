package org.olatunbosun.hadoop;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

import java.io.IOException;
/**
 * @author olulodeolatunbosun
 * @created 25/06/2024/06/2024 - 14:09
 */


public class MaxCityMapper extends Mapper<Object, BSONObject, Text, IntWritable> {

    @Override
    public void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
        String city = (String) value.get("city");
        int count = (Integer) value.get("number of patients");
        context.write(new Text(city), new IntWritable(count));
    }
}