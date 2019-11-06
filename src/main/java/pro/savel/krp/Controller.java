package pro.savel.krp;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import pro.savel.krp.objects.Message;
import pro.savel.krp.objects.Record;
import pro.savel.krp.objects.TopicInfo;
import reactor.core.publisher.Mono;

import java.util.Collection;

@RestController
@RequestMapping("/")
public class Controller {

	@Autowired
	private Service service;

	@GetMapping(path = "/")
	public String getVersion() {
		return "2.0.0";
	}

	@GetMapping(path = "/{topic}")
	public Mono<TopicInfo> getTopicInfo(@PathVariable String topic,
	                                    @RequestParam(required = false) Integer partition,
	                                    @RequestHeader(required = false) String groupId,
	                                    @RequestHeader(required = false) String clientId) {
		return service.getTopicInfo(topic, partition, groupId, clientId);
	}

	@GetMapping(path = "/{topic}/{partition}")
	public Mono<Collection<Record>> getData(@PathVariable String topic,
	                                        @PathVariable int partition,
	                                        @RequestParam long offset,
	                                        @RequestParam(required = false) Long timeout,
	                                        @RequestParam(required = false) String idHeader,
	                                        @RequestHeader(required = false) String groupId,
	                                        @RequestHeader(required = false) String clientId) {
		return service.getData(topic, partition, offset, timeout, idHeader, groupId, clientId);
	}

	@PostMapping(path = "/{topic}")
	@ResponseStatus(HttpStatus.CREATED)
	public Mono<Void> postData(@PathVariable String topic, @RequestBody Mono<Message> message) {
		service.postData(topic, message);
	}
}
