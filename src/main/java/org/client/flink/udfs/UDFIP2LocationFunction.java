package org.client.flink.udfs;

import net.ipip.ipdb.City;
import net.ipip.ipdb.CityInfo;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class UDFIP2LocationFunction extends ScalarFunction {

    public static final String functionName = "ip2location";

    private City cityDb;
    private FileSystem fs;

    @Override
    public void open(FunctionContext context) throws Exception {
        String uri = "/user/impala/resources/ip/ipipfree.ipdb";
        Configuration conf = new Configuration();
        this.fs = FileSystem.get(URI.create(uri), conf);
        FSDataInputStream hdfsInStream = fs.open(new Path(uri));
        this.cityDb = new City(hdfsInStream);
    }

    @Override
    public void close() throws Exception {
        this.fs.close();
    }

    public String eval(String IPStr) {
        try {
            CityInfo cityInfo = cityDb.findInfo(IPStr, "CN");
            return cityInfo.getCountryName() + "," + cityInfo.getRegionName() + "," + cityInfo.getCityName();
        } catch (Exception ioe) {
            return null;
        }
    }
}
