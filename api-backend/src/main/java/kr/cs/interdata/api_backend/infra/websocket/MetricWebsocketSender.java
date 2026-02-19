package kr.cs.interdata.api_backend.infra.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class MetricWebsocketSender {

    private static final Logger logger = LoggerFactory.getLogger(MetricWebsocketSender.class);
    private final ObjectMapper objectMapper;

    private final MetricWebsocketHandler metricWebsocketHandler;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    public MetricWebsocketSender(ObjectMapper objectMapper,
                                 MetricWebsocketHandler metricWebsocketHandler) {
        this.objectMapper = objectMapper;
        this.metricWebsocketHandler = metricWebsocketHandler;
    }

    public void handleMessage(Object metricData) {
        if (metricData == null) {
            logger.error("Null parameter detected - metricData: {}", metricData);
            return;
        }

        executorService.submit(() -> {
            try {
                String jsonString = objectMapper.writeValueAsString(metricData);
                metricWebsocketHandler.sendMetricMessage(jsonString);
            } catch (Exception e) {
                logger.error("Failed to send metric message for Machine Error: {}", e.getMessage());
            }
        });
    }

    /**
     * 애플리케이션 종료 시 ExecutorService 정리
     */
    @PreDestroy
    public void shutdownExecutor() {
        executorService.shutdown();
        logger.info("WebSocket sender executor service shut down.");
    }

}
