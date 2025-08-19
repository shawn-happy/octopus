package io.github.octopus.data.attract.admin;

import io.github.octopus.data.attract.common.core.feign.EnableOctopusFeignClients;
import io.github.octopus.data.attract.common.core.swagger.EnablePigDoc;
import io.github.octopus.data.attract.common.security.annotation.EnableResourceServer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@EnablePigDoc(value = "admin")
@EnableOctopusFeignClients
@EnableResourceServer
@EnableDiscoveryClient
@SpringBootApplication
public class DataAttractAdminApplication {

	public static void main(String[] args) {
		SpringApplication.run(DataAttractAdminApplication.class, args);
	}

}
