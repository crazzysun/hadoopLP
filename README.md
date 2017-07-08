# Flume conf
```
# Sources, channels, and sinks are defined per
# agent name, in this case 'tier2'.
tier2.sources  = source2
tier2.channels = channel2 channel3
tier2.sinks    = sinkLOG sinkHDFS

# For each source, channel, and sink, set
# standard properties.
tier2.sources.source2.type     = netcat
tier2.sources.source2.bind     = 127.0.0.1
tier2.sources.source2.port     = 12121
tier2.sources.source2.channels = channel2 channel3

tier2.sources.source2.interceptors = i1
tier2.sources.source2.interceptors.i1.type = regex_extractor
tier2.sources.source2.interceptors.i1.regex = (\\d{4})-(\\d{2})-(\\d{2})
tier2.sources.source2.interceptors.i1.serializers = s1 s2 s3

tier2.sources.source2.interceptors.i1.serializers.s1.name = year
tier2.sources.source2.interceptors.i1.serializers.s2.name = month
tier2.sources.source2.interceptors.i1.serializers.s3.name = day

tier2.channels.channel2.type   = memory
tier2.channels.channel3.type   = memory

tier2.sinks.sinkLOG.type       = logger
tier2.sinks.sinkLOG.channel    = channel3

tier2.sinks.sinkHDFS.type      = HDFS
tier2.sinks.sinkHDFS.hdfs.fileType  = DataStream
tier2.sinks.sinkHDFS.writeFormat= Text
tier2.sinks.sinkHDFS.hdfs.fileSuffix = .csv
tier2.sinks.sinkHDFS.channel   = channel2
tier2.sinks.sinkHDFS.hdfs.path = hdfs://localhost:8020/user/cloudera/flume/events/%{year}/%{month}/%{day}
tier2.sinks.sinkHDFS.hdfs.useLocalTimeStamp = true
tier2.sinks.sinkHDFS.hdfs.hdfs.batchSize = 50
tier2.sinks.sinkHDFS.hdfs.rollInterval = 10
tier2.sinks.sinkHDFS.hdfs.rollCount = 0
tier2.sinks.sinkHDFS.hdfs.rollSize = 0

# Other properties are specific to each type of## source, channel, or sink. In this case, we
# source, channel, or sink. In this case, we
# specify the capacity of the memory channel.
tier2.channels.channel2.capacity = 10000
```
```sudo /usr/bin/flume-ng agent -c /etc/flume-ng/conf -f /etc/flume-ng/conf/myflume.conf -n tier2 ```
# generator

```
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;


public class Solution{
    static String name = "product";
    static String type = "category";
    Long startTime = 1493668800000L;

    Long msInDay = 86400000L;
    Random rand;
    String[] IP = {"1.0.84.0/23", "1.10.222.0/23", "2.24.63.0/24", "2.24.108.128/25", "2.25.206.0/25",
            "2.27.216.0/24", "2.28.178.0/25", "2.28.181.0/25", "2.38.121.0/24", "2.156.17.0/24",
            "2.168.0.0/15", "2.171.43.0/24", "2.221.83.128/25", "2.222.74.0/25", "2.223.68.0/23",
            "2.224.129.0/24", "2.227.80.0/23", "8.28.69.0/24", "8.33.3.224/27", "8.36.253.0/24"};


    void start() throws InterruptedException, IOException  {
    	
    	Socket socket = new Socket("127.0.0.1", 12121);
		
		DataInputStream in = new DataInputStream(socket.getInputStream());
		DataOutputStream out = new DataOutputStream(socket.getOutputStream());
		System.out.println("is connected " + (socket.isConnected() ? "true" : "false")); 
	
        rand = new Random();
        
        for (int i = 0; i < 100; i++) {
//        	String res = "\"" + name + rand.nextInt(20) + "\"\t"
//                  + "\"" + type + rand.nextInt(20) + "\"\t"
//                  + "\"" + IP[rand.nextInt(20)] + "\"\t"
//                  + getRandDate() + "\t"
//                  + getPrice() 
//                  + "\n";
           	        	
//        	String res = "\"" + name + rand.nextInt(20) + "\","
//                    + "\"" + type + rand.nextInt(20) + "\","
//                    + "\"" + IP[rand.nextInt(20)] + "\","
//                    + getRandDate() + ","
//                    + getPrice() 
//                    + "\n";
        	
        	String res = name + rand.nextInt(20) + ","
                    + type + rand.nextInt(3) + ","
                    + IP[rand.nextInt(20)] + ","
                    + getRandDate() + ","
                    + getPrice() 
                    + "\n";
        	
        	
          out.writeBytes(" " + res);
    		  System.out.print(i + " " + res);
        }        
        out.flush();              
    	  TimeUnit.SECONDS.sleep(1);
        out.close();
    }

    int getPrice() {
        int price = (int) (rand.nextGaussian() * 500 + 1000);

        while (price <= 0) {
            price = (int) (rand.nextGaussian() * 500 + 1000);
        }

        return price;
    }

    String getRandDate() {
        int day = rand.nextInt(6);
        long time = (long) (rand.nextGaussian() * 43200000 + 43200000);

        while (time <= 0 || time >= msInDay) {
            time = (long) (rand.nextGaussian() * 43200000 + 43200000);
        }

        Date d = new Date(startTime + day * msInDay + time);
        SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        
        return dt.format(d);
    }

    public static void main(String[] args) throws UnknownHostException, IOException, InterruptedException {
        new Solution().start();
    }
}

```


# create product table
```
CREATE EXTERNAL TABLE IF NOT EXISTS products (product STRING, category STRING, ip STRING, date STRING, price INT)
PARTITIONED BY (year INT, month INT, day INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/cloudera/flume/events/2017/05/01/';

ALTER TABLE test2 ADD PARTITION(year=2017, month=05, day=01)
LOCATION  'hdfs://localhost:8020/user/cloudera/flume/events/2017/05/01';

ALTER TABLE test2 ADD PARTITION(year=2017, month=05, day=02)
LOCATION  'hdfs://localhost:8020/user/cloudera/flume/events/2017/05/02';

ALTER TABLE test2 ADD PARTITION(year=2017, month=05, day=03)
LOCATION  'hdfs://localhost:8020/user/cloudera/flume/events/2017/05/03';

ALTER TABLE test2 ADD PARTITION(year=2017, month=05, day=04)
LOCATION  'hdfs://localhost:8020/user/cloudera/flume/events/2017/05/04';

ALTER TABLE test2 ADD PARTITION(year=2017, month=05, day=05)
LOCATION  'hdfs://localhost:8020/user/cloudera/flume/events/2017/05/05';

ALTER TABLE test2 ADD PARTITION(year=2017, month=05, day=06)
LOCATION  'hdfs://localhost:8020/user/cloudera/flume/events/2017/05/06';

ALTER TABLE test2 ADD PARTITION(year=2017, month=05, day=07)
LOCATION  'hdfs://localhost:8020/user/cloudera/flume/events/2017/05/07';
```

# create IP table

```
hadoop fs -put ~/ip.csv  /user/cloudera/ip/
```

```
CREATE EXTERNAL TABLE IF NOT EXISTS ipTable (
network STRING,
geoname_id STRING,
registered_country_geoname_id STRING,
represented_country_geoname_id STRING,
is_anonymous_proxy STRING,
is_satellite_provider STRING,
postal_code STRING,
latitude STRING,
longitude STRING,
accuracy_radius STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/cloudera/ip/';
```

# 5.1
```
INSERT OVERWRITE DIRECTORY '/user/cloudera/results/1'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
  category,
  COUNT(*) AS cnt
FROM
  test2
GROUP BY
  category
ORDER BY
  cnt DESC
LIMIT 10;
```

### table for mysql:





# 5.2
```
INSERT OVERWRITE DIRECTORY '/user/cloudera/results/2'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT category, product, cnt, rank
FROM (
    SELECT category, product, cnt, row_number() 
           over (PARTITION BY category ORDER BY cnt DESC) as rank
    FROM (
        SELECT category, product, sum(product) AS cnt
        FROM test2
        GROUP BY category, product
    ) a
) ranked_mytable
WHERE ranked_mytable.rank <= 10
ORDER BY category, rank;
```

# 6
```
INSERT OVERWRITE DIRECTORY '/user/cloudera/result/3'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT t1.ip, t2.geoname_id, t1.s
FROM 
    (SELECT ip, sum(price) as s FROM test2 GROUP BY ip) t1 
LEFT JOIN
    (SELECT network, geoname_id FROM iptable) t2
ON
    t1.ip = t2.network
ORDER BY t1.s DESC
LIMIT 10;
```

