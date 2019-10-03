package pro.savel.krp;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import pro.savel.krp.objects.Message;
import pro.savel.krp.objects.Record;
import pro.savel.krp.objects.Topic;

import java.util.Collection;

@RestController
@RequestMapping("/")
public class Controller {

	private final Service service;

	public Controller(Service service) {
		this.service = service;
	}

	@GetMapping(path = "/{topic}")
	public Topic getTopicInfo(@PathVariable String topic,
	                          @RequestHeader(required = false) String consumerGroup,
	                          @RequestHeader(required = false) String clientIdPrefix,
	                          @RequestHeader(required = false) String clientIdSuffix) {

		return service.getTopicInfo(topic, consumerGroup, clientIdPrefix, clientIdSuffix);
	}

	@GetMapping(path = "/{topic}/{partition}")
	public Collection<Record> getData(@PathVariable String topic,
	                                  @PathVariable int partition,
	                                  @RequestParam long offset,
	                                  @RequestParam(required = false) Long timeout,
	                                  @RequestParam(required = false) Long limit,
	                                  @RequestParam(required = false) String idHeader,
	                                  @RequestHeader(required = false) String commit,
	                                  @RequestHeader(required = false) String consumerGroup,
	                                  @RequestHeader(required = false) String clientIdPrefix,
	                                  @RequestHeader(required = false) String clientIdSuffix) {

		return service.getData(topic, partition, offset, commit,
				timeout, limit, idHeader, consumerGroup,
				clientIdPrefix, clientIdSuffix);
	}

	@PostMapping(path = "/{topic}")
	@ResponseStatus(HttpStatus.CREATED)
	public void postData(@PathVariable String topic,
	                     @RequestBody Message message) {

		service.postData(topic, message.getKey(), message.getHeaders(), message.getValue());
	}
}
