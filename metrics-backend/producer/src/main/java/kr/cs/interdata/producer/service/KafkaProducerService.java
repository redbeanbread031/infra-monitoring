package kr.cs.interdata.producer.service;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Gson gson = new Gson();
    private final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);

    @Value("${KAFKA_TOPIC_NAME}")
    private String topic_name;

    @Autowired
    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void routeMessageBasedOnType(String jsonPayload) {
        kafkaTemplate.send(topic_name, jsonPayload);
        kafkaTemplate.flush();
    }
}