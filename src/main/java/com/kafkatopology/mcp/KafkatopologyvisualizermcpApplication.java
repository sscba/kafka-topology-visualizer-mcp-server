package com.kafkatopology.mcp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

@SpringBootApplication
@EnableCaching
public class KafkatopologyvisualizermcpApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkatopologyvisualizermcpApplication.class, args);
	}

}
