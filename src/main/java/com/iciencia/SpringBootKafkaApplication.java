package com.iciencia;

import java.util.Iterator;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;

@SpringBootApplication
public class SpringBootKafkaApplication  {

	private static final Logger log = LoggerFactory.getLogger(SpringBootKafkaApplication.class);

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private KafkaListenerEndpointRegistry registryKafka;

	@Autowired
	private MeterRegistry micrometer; 
	// ****
	// **** ACCESO A LA INFORMACION COMPLETA
	// Se define la propiedad para poder contrar la ejecucion del listener. Pausar y
	// renudar el consumo. id="culquierId", autoStartup = "false"
	// para esto se debe definir una clase tipo KafkaListenerEndpointRegistry y
	// posterior llamar desde registryKafka.getListenerContainer("cualquierId");

	@KafkaListener(id = "cualquierId", autoStartup = "true", topics = "TutorialTopic", containerFactory = "kafkaListenerContainerFactory", groupId = "iciencia-group", properties = {
			"max.poll.interval.ms:4000", "max.poll.records:50" })
	public void listen(List<ConsumerRecord<String, String>> messages) {
		log.info("Inicio de captura de mensaje completo");

//		for (ConsumerRecord<String, String> consumerRecord : messages) {
//			log.info("Partition = {}, Offset = {}, Key ={}, Value = {}", consumerRecord.partition(),
//					consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());
//		}

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
//	@Override
//	public void run(String... args) throws Exception {
//		for (int i = 0; i < 100; i++) {
//
//			ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send("TutorialTopic", String.valueOf(i),
//					String.format(" SMS generado numero %d", i));
//			future.addCallback(new KafkaSendCallback<String, String>() {
//
//				@Override
//				public void onSuccess(SendResult<String, String> result) {
//					log.info("Mensaje enviado con exito {}", result.getRecordMetadata().offset());
//				}
//
//				@Override
//				public void onFailure(Throwable ex) {
//					log.info("Mensaje no enviado {}", ex);
//				}
//
//				@Override
//				public void onFailure(KafkaProducerException ex) {
//					log.info("Mensaje no enviado {}", ex);
//				}
//			});
//		}
//		Thread.sleep(5000);
//		registryKafka.getListenerContainer("cualquierId").start();
//	}
	
	@Scheduled(fixedDelay = 2000, initialDelay = 100)
	public void print() {
		for (int i = 0; i < 100; i++) {
			kafkaTemplate.send("TutorialTopic", String.valueOf(i), String.format("Sample message %d", i));
		}
	}
	
	
	@Scheduled(fixedDelay = 2000, initialDelay = 500)
	public void printMetric() {
		double count = micrometer.get("kafka.producer.record.send.total").functionCounter().count();
		
		log.info("Count = {}", count);
		
		//OBtener metricas de kafka
		
		List<Meter> metrics = micrometer.getMeters();
		
		for (Meter meter : metrics) {
			log.info("Meter = {}",meter.getId());
		}
	}
	
	
}
