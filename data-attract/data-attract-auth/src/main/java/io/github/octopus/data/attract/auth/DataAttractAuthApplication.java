package io.github.octopus.data.attract.auth;

import io.github.octopus.data.attract.common.core.feign.EnableOctopusFeignClients;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@EnableOctopusFeignClients
@EnableDiscoveryClient
@SpringBootApplication
public class DataAttractAuthApplication {

	public static void main(String[] args) {
		SpringApplication.run(DataAttractAuthApplication.class, args);
	}

}
