package pro.savel.krp;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import pro.savel.krp.objects.Record;
import pro.savel.krp.objects.Topic;

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

	@PostMapping(path = "/{topic}")
	@ResponseStatus(HttpStatus.CREATED)
	public void postTopic(@PathVariable String topic,
	                      @RequestParam(required = false) String key,
	                      @RequestBody String body,
	                      @RequestHeader Map<String, String> headers) {

		Map<String, String> filteredHeaders = headers.entrySet().stream()
				.filter(entry -> entry.getKey().startsWith("k-"))
				.collect(Collectors.toMap(entry -> entry.getKey().substring(2), Map.Entry::getValue));

		service.post(topic, key, filteredHeaders, body);
	}

	@GetMapping(path = "/{topic}")
	public Topic getTopic(@PathVariable String topic,
	                      @RequestHeader(required = false) String consumerGroup,
	                      @RequestHeader(required = false) String clientIdPrefix,
	                      @RequestHeader(required = false) String clientIdSuffix) {
		return service.getTopic(topic, consumerGroup, clientIdPrefix, clientIdSuffix);
	}

	@GetMapping(path = "/{topic}/{partition}")
	public Collection<Record> getPartition(@PathVariable String topic,
	                                       @PathVariable int partition,
	                                       @RequestParam long offset,
	                                       @RequestParam(required = false) Long limit,
	                                       @RequestHeader(required = false) String consumerGroup,
	                                       @RequestHeader(required = false) String clientIdPrefix,
	                                       @RequestHeader(required = false) String clientIdSuffix) {

		return service.getData(topic, partition, offset, limit, consumerGroup, clientIdPrefix, clientIdSuffix);
	}
}
