package com.javatechie.kafka_producer_example;

import com.javatechie.dto.Customer;
import com.javatechie.service.KafkaMessagePublisher;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class KafkaProducerExampleApplicationTests {

	@Container
	static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0")
			.asCompatibleSubstituteFor("apache/kafka")
	);


	@DynamicPropertySource
	public static void initKafkaProperties(DynamicPropertyRegistry registry) {
		registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
	}

	@Autowired
	private KafkaMessagePublisher publisher;

	@Test
	public void testSendEventsToTopic(){
		publisher.sendEventsToTopic(new Customer(123124, "test user", "test@gmail.com", "1523465262"));
		Awaitility.await().pollInterval(Duration.ofSeconds(3)).atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
			// assert statement
		});
	}
}
