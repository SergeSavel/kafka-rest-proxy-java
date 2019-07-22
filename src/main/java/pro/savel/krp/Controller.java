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
	public void postAccount(@PathVariable String topic,
	                        @RequestParam String key,
	                        @RequestHeader("Content-Length") long contentLength,
	                        @RequestHeader(name = "Content-Encoding", required = false) String encoding,
	                        @RequestBody byte[] body) {

		service.postTopic(topic, key, body);
	}
}
