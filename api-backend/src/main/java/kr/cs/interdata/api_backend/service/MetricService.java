package kr.cs.interdata.api_backend.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import kr.cs.interdata.api_backend.infra.websocket.MetricWebsocketSender;
import kr.cs.interdata.api_backend.service.repository_service.MachineInventoryService;
import kr.cs.interdata.api_backend.service.threshold_service.ThresholdEvaluationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * MetricService는 Kafka Consumer로부터 수신된 메트릭 데이터를 처리하는 서비스입니다.
 * - 웹소켓으로 클라이언트에 전송
 * - 임계값(Threshold) 초과 여부 계산
 * - 수신된 메트릭 로그 출력
 */
@Service
public class MetricService {

    private final Logger logger = LoggerFactory.getLogger(MetricService.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final ThresholdEvaluationService thresholdEvaluationService;
    private final MetricWebsocketSender metricWebsocketSender;
    private final MachineInventoryService machineInventoryService;
    private final MetricMonitorService metricMonitorService;

    public MetricService(ThresholdEvaluationService thresholdEvaluationService,
                         MetricWebsocketSender metricWebsocketSender,
                         MachineInventoryService machineInventoryService,
                         MetricMonitorService metricMonitorService) {
        this.thresholdEvaluationService = thresholdEvaluationService;
        this.metricWebsocketSender = metricWebsocketSender;
        this.machineInventoryService = machineInventoryService;
        this.metricMonitorService = metricMonitorService;
    }

    /**
     * Kafka Consumer에서 수신된 메트릭 JSON 문자열을 처리합니다.
     * 1. JSON 파싱
     * 2. 웹소켓으로 클라이언트에 전송
     * 3. 임계값 초과 여부 계산
     * 4. 로그 출력
     *
     * @param metric JSON 문자열 형태의 메트릭 데이터
     */
    public void sendMetric(String metric) {
        JsonNode metricsNode = parseJson(metric);

        // 1. 실시간 웹소켓 전송
        metricWebsocketSender.handleMessage(metricsNode);

        // 2. Inventory 등록
        machineInventoryService.registerMachineIfAbsent(metric);

        // 3. 캐시 갱신: 호스트 + 모든 컨테이너
        metricMonitorService.updateTimestamps(metricsNode);

        // 4. 임계값 초과 및 미달 확인
        thresholdEvaluationService.calcThreshold(metric);

        // 5. 로그 출력
        logger.info("Metrics sent to Websocket: {}", metric);
    }

    /**
     * JSON 문자열을 Jackson의 JsonNode 객체로 파싱합 니다.
     * 유효하지 않은 JSON의 경우 사용자 정의 예외를 발생시킵니다.
     *
     * @param json 문자열 형태의 JSON
     * @return JsonNode 파싱 결과
     */
    private JsonNode parseJson(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception e) {
            throw new InvalidJsonException("JSON 파싱 실패", e);
        }
    }

    /**
     * JSON 파싱 실패 시 사용되는 사용자 정의 런타임 예외
     */
    public static class InvalidJsonException extends RuntimeException {
        public InvalidJsonException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
