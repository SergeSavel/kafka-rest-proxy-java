package pro.savel.krp;

import org.springframework.context.annotation.Bean;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.web.server.SecurityWebFilterChain;

@org.springframework.context.annotation.Configuration
public class Configuration {
	@Bean
	public SecurityWebFilterChain springSecurityFilterChain(ServerHttpSecurity http) {
		http
				.csrf(csrfSpec -> csrfSpec.disable());
		return http.build();
	}
}
