package kr.cs.interdata.consumer.listener;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import kr.cs.interdata.consumer.infra.MetricApiClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public class KafkaMetricListener {

    private final Logger logger = LoggerFactory.getLogger(KafkaMetricListener.class);
    private final ObjectMapper objectMapper;

    private final MetricApiClient metricApiClient;

    public KafkaMetricListener(MetricApiClient metricApiClient, ObjectMapper objectMapper) {
        this.metricApiClient = metricApiClient;
        this.objectMapper = objectMapper;
    }

    /**
     * 	- Kafka - 배치 메시지를 수신하고 처리하는 메서드
     * 	type : BATCH
     * 	listener Type : BatchMessageListener
     * 	method parameter : onMessage(ConsumerRecords<K, V> data)
     *
     * @param records   지정 토픽에서 받아온 데이터 list
     */
    @KafkaListener(
            topics = "${KAFKA_TOPIC_NAME}",
            groupId = "${KAFKA_CONSUMER_GROUP_ID}",
            containerFactory = "customContainerFactory"
    )
    public void batchListener(ConsumerRecords<String, String> records, Acknowledgment ack) {

        for (ConsumerRecord<String, String> record : records) {
            String json = record.value();

            try {
                JsonNode metricsNode = parseJson(json);

                // *******************************
                //     transmit to API-server
                // *******************************
                metricApiClient.sendThresholdViolation(json);

                logger.info("Kafka Record 처리 성공: {}", metricsNode);

            } catch (InvalidJsonException e) {
                logger.error("잘못된 JSON 형식 - key: {}, value: {}, error: {}", record.key(), record.value(), e.getMessage());
            } catch (IllegalArgumentException e) {
                logger.warn("JSON 필드 누락 - key: {}, value: {}, 원인: {}", record.key(), record.value(), e.getMessage());
            } catch (Exception e) {
                logger.error("예상치 못한 예외 발생 - key: {}, value: {}", record.key(), record.value(), e);
            }
        }
        // 수동 커밋
        ack.acknowledge();
    }

    // json 파싱
    private JsonNode parseJson(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception e) {
            throw new InvalidJsonException("JSON 파싱 실패", e);
        }
    }
    
    // 사용자 정의 예외
    public static class InvalidJsonException extends RuntimeException {
        public InvalidJsonException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}