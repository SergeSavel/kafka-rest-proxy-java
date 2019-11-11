package pro.savel.krp.objects;

import java.util.Collections;
import java.util.Map;

public class Record {

	public final long timestamp;
	public final long offset;
	public final String key;
	public final Map<String, String> headers;
	public final String value;

	public Record(long timestamp, long offset, String key, Map<String, String> headers, String value) {
		this.timestamp = timestamp;
		this.offset = offset;
		this.key = key;
		this.headers = Collections.unmodifiableMap(headers);
		this.value = value;
	}
}
