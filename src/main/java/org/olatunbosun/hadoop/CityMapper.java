package org.olatunbosun.hadoop;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

import java.io.IOException;

/**
 * @author olulodeolatunbosun
 * @created 25/06/2024/06/2024 - 12:52
 */
public  class CityMapper extends Mapper<Object, BSONObject, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text city = new Text();

    public void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
        String val = (String) value.get("city");

        if (val != null){
            city.set(val);
            context.write(city, one);
        }

    }
}
