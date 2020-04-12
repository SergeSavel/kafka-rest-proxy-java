// Copyright 2019-2020 Sergey Savelev
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pro.savel.krp.objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

@JacksonXmlRootElement(localName = "r")
public class Record {

	private static final ThreadLocal<MessageDigest> threadLocalMD = new ThreadLocal<>();

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

	@JsonProperty("id")
	private UUID id = null;

	public Record(long timestamp, long offset, String key, Map<String, String> headers, String value) {
		this.timestamp = timestamp;
		this.offset = offset;
		this.key = key;
		this.headers = Collections.unmodifiableMap(headers);
		this.value = value;
	}

	public UUID getId() {
		return id;
	}

	public void calcID() {
		calcID(null);
	}

	public void calcID(String headerKey) {

		String source = headerKey == null ? key : headers.get(headerKey);

		if (source != null) {
			MessageDigest md = threadLocalMD.get();
			if (md == null) {
				try {
					md = MessageDigest.getInstance("MD5");
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
				threadLocalMD.set(md);
			}
			id = UUID.nameUUIDFromBytes(md.digest(source.getBytes(StandardCharsets.UTF_8)));
			md.reset();
		}
	}
}
