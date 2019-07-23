package pro.savel.krp;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

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
	                        @RequestBody String body) {

		service.postTopic(topic, key, body);
	}

	@GetMapping(path = "/{topic}")
	public String getTopic(@PathVariable String topic,
	                      @RequestParam long offset,
	                      @RequestParam long limit) {

		return service.getTopicInfo(topic);
	}

	@GetMapping(path = "/{topic}/{partition}")
	public String getPartition(@PathVariable String topic, @PathVariable int partition,
	                       @RequestParam long offset, @RequestParam long limit) {

		return service.getPartition(topic, offset, limit);
	}
}
