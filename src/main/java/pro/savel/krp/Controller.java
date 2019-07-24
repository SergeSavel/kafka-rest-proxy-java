package pro.savel.krp;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import pro.savel.krp.objects.Record;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/")
public class Controller {

	private final Service service;

	public Controller(Service service) {
		this.service = service;
	}

	@PostMapping(path = "/{topic}", consumes = MediaType.APPLICATION_XML_VALUE)
	@ResponseStatus(HttpStatus.CREATED)
	public void postTopic(@PathVariable String topic,
	                      @RequestParam(required = false) String key,
	                      @RequestHeader("Content-Length") long contentLength,
	                      @RequestHeader(name = "Content-Encoding", required = false) String encoding,
	                      @RequestBody String body,
	                      @RequestHeader Map<String, String> headers) {

		var filteredHeaders = headers.entrySet().stream()
				.filter(entry -> entry.getKey().startsWith("k-"))
				.collect(Collectors.toMap(stringStringEntry -> stringStringEntry.getKey().substring(2), Map.Entry::getValue));

		service.post(topic, key, filteredHeaders, body);
	}

	@GetMapping(path = "/{topic}")
	public int[] getTopic(@PathVariable String topic) {
		return service.getTopicPartitions(topic);
	}

	@GetMapping(path = "/{topic}/{partition}")
	public Collection<Record> getPartition(@PathVariable String topic,
	                                       @PathVariable int partition,
	                                       @RequestParam(required = false) String group,
	                                       @RequestParam Long offset,
	                                       @RequestParam(required = false) Long limit) {

		return service.getData(topic, partition, group, offset, limit);
	}
}
