package pro.savel.krp;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import pro.savel.krp.objects.Message;
import pro.savel.krp.objects.Record;
import pro.savel.krp.objects.TopicInfo;

import java.util.Collection;

@RestController
@RequestMapping("/")
public class Controller {

	private final Service service;

	public Controller(Service service) {
		this.service = service;
	}

	@GetMapping(path = "/")
	public String getVersion() {
		return "1.10.1";
	}

	@GetMapping(path = "/{topic}")
	public TopicInfo getTopicInfo(@PathVariable String topic) {
		return service.getTopicInfo(topic);
	}

	@GetMapping(path = "/{topic}/{partition}")
	public Collection<Record> getData(@PathVariable String topic,
	                                  @PathVariable int partition,
	                                  @RequestParam long offset,
	                                  @RequestParam(required = false) Long timeout,
	                                  @RequestParam(required = false) Long limit,
	                                  @RequestParam(required = false) String idHeader,
	                                  @RequestHeader(required = false) String groupId,
	                                  @RequestHeader(required = false) String clientId) {
		return service.getData(topic, partition, offset, timeout, limit, idHeader, groupId, clientId);
	}

	@PostMapping(path = "/{topic}")
	@ResponseStatus(HttpStatus.CREATED)
	public void postData(@PathVariable String topic, @RequestBody Message message) {
		service.postData(topic, message);
	}
}
