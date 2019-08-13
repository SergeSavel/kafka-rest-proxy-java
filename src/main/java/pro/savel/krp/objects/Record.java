package pro.savel.krp.objects;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

public class Record {

	public final long timestamp;
	public final long offset;
	public final String key;
	private static ThreadLocal<MessageDigest> threadLocalMD = new ThreadLocal<>();
	public final Map<String, String> headers;
	public final String value;
	public final UUID keyUUID;

	public Record(long timestamp, long offset, String key, Map<String, String> headers, String value) {
		this.timestamp = timestamp;
		this.offset = offset;
		this.key = key;
		this.headers = Collections.unmodifiableMap(headers);
		this.value = value;

		if (key == null)
			keyUUID = null;
		else {
			MessageDigest md = threadLocalMD.get();
			if (md == null) {
				try {
					md = MessageDigest.getInstance("MD5");
					threadLocalMD.set(md);
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
				threadLocalMD.set(md);
			}
			keyUUID = UUID.nameUUIDFromBytes(md.digest(key.getBytes()));
			md.reset();
		}
	}
}
