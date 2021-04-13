package com.iciencia.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.MicrometerProducerListener;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
public class KafkaConfiguration {

	@Autowired
	private Micrometer micrometer;
	
	
	@Bean
	public Map<String, Object> consumerProperties() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.86:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "iciencia-group");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		return props;
	}

	@Bean
	public ConsumerFactory<String, String> consumerFactory() {
		return new DefaultKafkaConsumerFactory<>(consumerProperties());
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		//Pemite recibir los mensajes en bash en grupo
		factory.setBatchListener(true);
		
		// consumir mensajes de forma concurrente
		factory.setConcurrency(3);
		return factory;
	}

	
	
	// Producer
	private Map<String, Object> producerProps() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.86:9092");
		// G:Define los reintentos que se realizarán en caso de error
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		// El producer agrupará los registros en batches, mejorando el performance
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		// Los batches se agruparan de acuerdo de un periodo de tiempo, está definido en
		// milisegundos
		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		// Define el espacio de memoria que se asignará para colocar los mensajes que
		// están pendientes por enviar
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		return props;
	}

	@Bean
	public KafkaTemplate<String, String> createTemplate() {
		Map<String, Object> senderProps = producerProps();
		ProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		//Para llamar al consumer  de las metricas
		pf.addListener(new MicrometerProducerListener<String, String>(micrometer.meterRegistry()));
		KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);
		return template;
	}

}
