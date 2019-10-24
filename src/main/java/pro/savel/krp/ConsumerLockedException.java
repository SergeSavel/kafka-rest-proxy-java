package pro.savel.krp;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(code = HttpStatus.CONFLICT, reason = "Consumer is locked")
public class ConsumerLockedException extends RuntimeException {
}
