package pro.savel.krp.objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

@JacksonXmlRootElement(localName = "r")
public class Record {

	private static ThreadLocal<MessageDigest> threadLocalMD = new ThreadLocal<>();

	@JsonProperty("t")
	public final long timestamp;
	@JsonProperty("o")
	public final long offset;
	@JsonProperty("k")
	public final String key;
	@JsonProperty("h")
	public final Map<String, String> headers;
	@JsonProperty("v")
	public final String value;
	@JsonProperty("kid")
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
