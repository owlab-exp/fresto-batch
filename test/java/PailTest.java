import java.util.Map;
import java.util.HashMap;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import jcascalog.Api;
import com.backtype.hadoop.pail.Pail;
import fresto.data.FrestoData;

public class PailTest {
	public static void main(String[] args) throws Exception {
		moveTest();
	}
	
	public static void moveTest() throws IOException {
		// Common block of every job
		System.setProperty("HADOOP_USER_NAME", "hdfs");

		Map conf = new HashMap();
		String sers = "backtype.hadoop.ThriftSerialization";
		sers += ",";
		sers += "org.apache.hadoop.io.serializer.WritableSerializatio";
		conf.put("io.serializations", sers);
		Api.setApplicationConf(conf);


		Configuration hadoopConfig = new Configuration();
		hadoopConfig.set("fs.default.name", "hdfs://fresto1.owlab.com:9000");
		
		// Initialize swap directory
		FileSystem fs = FileSystem.get(hadoopConfig);
		fs.delete(new Path("/fresto/tmp/swa"), true);
		fs.mkdirs(new Path("/fresto/tmp/swa"));

		Pail<FrestoData> newDataPail = new Pail<FrestoData>("hdfs://fresto1.owlab.com:9000/fresto/new");
		Pail<FrestoData> snapshotPail = newDataPail.snapshot("hdfs://fresto1.owlab.com:9000/fresto/tmp/swa/newDataSnapshot");
		// TODO to something 
		newDataPail.deleteSnapshot(snapshotPail);


	}
}
