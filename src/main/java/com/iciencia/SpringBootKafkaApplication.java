package com.iciencia;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaSendCallback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

@SpringBootApplication
public class SpringBootKafkaApplication implements CommandLineRunner {

	private static final Logger log = LoggerFactory.getLogger(SpringBootKafkaApplication.class);

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private KafkaListenerEndpointRegistry registryKafka;

	// ****
	// **** ACCESO A LA INFORMACION COMPLETA
	// Se define la propiedad para poder contrar la ejecucion del listener. Pausar y
	// renudar el consumo. id="culquierId", autoStartup = "false"
	// para esto se debe definir una clase tipo KafkaListenerEndpointRegistry y
	// posterior llamar desde registryKafka.getListenerContainer("cualquierId");

	@KafkaListener(id = "cualquierId", autoStartup = "false", topics = "TutorialTopic", containerFactory = "kafkaListenerContainerFactory", groupId = "iciencia-group", properties = {
			"max.poll.interval.ms:4000", "max.poll.records:10" })
	public void listen(List<ConsumerRecord<String, String>> messages) {
		log.info("Inicio de captura de mensaje completo");

		for (ConsumerRecord<String, String> consumerRecord : messages) {
			log.info("Partition = {}, Offset = {}, Key ={}, Value = {}", consumerRecord.partition(),
					consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());
		}

		log.info("Bath complete");

	}

	// CONSUMO NORMAL DE MENSAJE
//	@KafkaListener(topics = "TutorialTopic", groupId = "iciencia-group")
//	public void listen(String message) {
//		log.info("Mensaje Kafka {}", message);
//	}
	/**
	 * El nombre container proviene desde el nombre de la clase de configuracion
	 * Consumo por particiones
	 * 
	 * @param message
	 */
//	@KafkaListener(topics = "TutorialTopic", containerFactory = "kafkaListenerContainerFactory", groupId = "iciencia-group", properties = {
//			"max.poll.interval.ms:4000", "max.poll.records:10" })
//	public void listen(List<String> message) {
//		log.info("Start reading messages");
//
//		for (String string : message) {
//			log.info("Mensaje leido = {}", string);
//		}
//
//		log.info("Batch complete");
//	}

	public static void main(String[] args) {
		SpringApplication.run(SpringBootKafkaApplication.class, args);
	}

	/**
	 * Manejon de callback aviso de mensaje recibido.
	 * 
	 * Para envio de mensajes sincronos recordar el uso de .get en send
	 */
	@Override
	public void run(String... args) throws Exception {
		for (int i = 0; i < 100; i++) {

			ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send("TutorialTopic", String.valueOf(i),
					String.format(" SMS generado numero %d", i));
			future.addCallback(new KafkaSendCallback<String, String>() {

				@Override
				public void onSuccess(SendResult<String, String> result) {
					log.info("Mensaje enviado con exito {}", result.getRecordMetadata().offset());
				}

				@Override
				public void onFailure(Throwable ex) {
					log.info("Mensaje no enviado {}", ex);
				}

				@Override
				public void onFailure(KafkaProducerException ex) {
					log.info("Mensaje no enviado {}", ex);
				}
			});
		}
		Thread.sleep(5000);
		registryKafka.getListenerContainer("cualquierId").start();
	}
}
