package org.cluxis.hbase_clj.helper;
import org.apache.hadoop.hbase.util.Bytes;

public class ByteConversion {
	public static byte[] fromLong(Long x) {
		return Bytes.toBytes(x);
	}
}
