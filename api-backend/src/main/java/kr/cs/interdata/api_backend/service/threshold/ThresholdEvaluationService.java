package kr.cs.interdata.api_backend.service.threshold;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ThresholdEvaluationService {

    private final Map<String, Boolean> zeroStateCache = new ConcurrentHashMap<>();

    private final ObjectMapper objectMapper;
    private final Logger logger = LoggerFactory.getLogger(ThresholdEvaluationService.class);

    private final ThresholdEventService thresholdEventService;
    private final ThresholdPolicyService thresholdPolicyService;

    public ThresholdEvaluationService(ObjectMapper objectMapper, ThresholdEventService thresholdEventService,
                                      ThresholdPolicyService thresholdPolicyService) {
        this.objectMapper = objectMapper;
        this.thresholdEventService = thresholdEventService;
        this.thresholdPolicyService = thresholdPolicyService;
    }

    /**
     * - 수집된 메트릭 데이터를 비동기로 파싱 및 임계값 평가 실행
     * @param metric JSON 문자열 형식의 메트릭 데이터
     */
    @Async
    public void calcThreshold(String metric) {
        JsonNode root = parseJson(metric);

        String type = root.path("type").asText();       // "host"
        String hostId = root.path("hostId").asText();   // host id
        String hostName = root.path("name").asText();   // host name
        String violationTime = root.path("timeStamp").asText(); // timestamp

        // 1. Host 자체 메트릭 처리
        processMetricAnomaly(
                type,                  // "host"
                hostId,                // host id
                hostName,                  // hostName
                LocalDateTime.parse(violationTime),
                root                   // 전체 JSON에서 host 메트릭은 root 자체
        );

        // 2. Container 각각 메트릭 처리
        JsonNode containersNode = root.path("containers");
        if (containersNode != null && containersNode.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = containersNode.fields();

            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                String containerId = entry.getKey();             // container id
                JsonNode containerNode = entry.getValue();       // 그 안의 메트릭 정보

                String containerName = containerNode.path("name").asText(); // ex. "app1"

                processMetricAnomaly(
                        "container",
                        containerId,
                        containerName,
                        LocalDateTime.parse(violationTime),
                        containerNode
                );
            }
        }
    }

    /**
     * 개별 장비 및 컨테이너의 주요 메트릭 값에 대해 임계값 비교 및 이상 판단 처리.
     * @param type 타입("host" 또는 "container")
     * @param machineId 장비 또는 컨테이너 ID
     * @param machineName 장비 또는 컨테이너 이름
     * @param violationTime 데이터 수집 시각
     * @param metricsNode 분석할 메트릭 데이터(JSON Node)
     */
    public void processMetricAnomaly(String type, String machineId, String machineName, LocalDateTime violationTime, JsonNode metricsNode) {
        double metricValue = 0.0;
        int zeroValueCnt = 0;
        String metricName = null;
        boolean isNormal;
        String cacheKey = type + ":" + machineId + ":" + machineName;

        // CPU, Memory, DiskReadDelta, DiskWriteDelta
        for (int i = 0;i < 4;i++){
            if (i == 0) {
                metricName = "cpu";
                metricValue = metricsNode.has("cpuUsagePercent")
                        ? metricsNode.get("cpuUsagePercent").asDouble()
                        : 0.0;
            }
            if (i == 1) {
                metricName = "memory";
                metricValue = metricsNode.has("memoryUsedBytes")
                        ? metricsNode.get("memoryUsedBytes").asDouble()
                        : 0.0;
            }
            if (i == 2) {
                metricName = "diskReadDelta";
                metricValue = metricsNode.has("diskReadBytesDelta")
                        ? metricsNode.get("diskReadBytesDelta").asDouble()
                        : 0.0;
            }
            if (i == 3) {
                metricName = "diskWriteDelta";
                metricValue = metricsNode.has("diskReadBytesDelta")
                        ? metricsNode.get("diskWriteBytesDelta").asDouble()
                        : 0.0;
            }

            if (metricValue == 0.0) {
                zeroValueCnt++;
            } else {
                // 정상값이 들어오면 캐시 해제
                zeroStateCache.remove(cacheKey);
            }

            // 각 메트릭별 threshold를 조회해 초과하면 db저장을 위해 api-backend로 데이터 보낸 후, 로깅함.
            isNormal = thresholdPolicyService.evaluateThresholdAndLogViolation(type , machineId, machineName,
                    metricName, metricValue, violationTime);
        }


        // 모든 메트릭이 0일 경우 → 캐시에 없을 때만 로그 저장
        if (zeroValueCnt == 4 && !zeroStateCache.containsKey(cacheKey)) {
            thresholdEventService.storeZeroValueLog(type, machineId, machineName, violationTime);
            zeroStateCache.put(cacheKey, true);
        }

        // Network
        JsonNode networkNode = metricsNode.path("networkDelta");

        if (!networkNode.isMissingNode() && networkNode.isObject()) {
            // [1] Tx 기준 평가
            metricName = "networkTx";
            Iterator<Map.Entry<String, JsonNode>> txInterfaces = networkNode.fields();
            while (txInterfaces.hasNext()) {
                Map.Entry<String, JsonNode> entry = txInterfaces.next();
                JsonNode interfaceData = entry.getValue();

                metricValue = interfaceData.has("txBytesDelta")
                        ? interfaceData.get("txBytesDelta").asDouble()
                        : 0.0;

                isNormal = thresholdPolicyService.evaluateThresholdAndLogViolation(
                        type, machineId, machineName,
                        metricName, metricValue, violationTime
                );

                if (!isNormal) {
                    break; // Tx 기준 비정상이면 루프 종료
                }
            }

            // [2] Rx 기준 평가
            metricName = "networkRx";
            Iterator<Map.Entry<String, JsonNode>> rxInterfaces = networkNode.fields();
            while (rxInterfaces.hasNext()) {
                Map.Entry<String, JsonNode> entry = rxInterfaces.next();
                JsonNode interfaceData = entry.getValue();

                metricValue = interfaceData.has("rxBytesDelta")
                        ? interfaceData.get("rxBytesDelta").asDouble()
                        : 0.0;

                isNormal = thresholdPolicyService.evaluateThresholdAndLogViolation(
                        type, machineId, machineName,
                        metricName, metricValue, violationTime
                );

                if (!isNormal) {
                    break; // Rx 기준 비정상이면 루프 종료
                }
            }
        } else {
            logger.warn("{}: {} - network 데이터를 찾을 수 없습니다.", type, machineId);
        }

        // Temperature
        if (type.equals("host")) {
            metricName = "temperature";

            JsonNode tempsNode = metricsNode.get("temperatures");
            if (tempsNode != null && tempsNode.isObject()) {
                double maxTemp = Double.MIN_VALUE;

                Iterator<Map.Entry<String, JsonNode>> fields = tempsNode.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> entry = fields.next();
                    double temp = entry.getValue().asDouble();
                    if (temp > maxTemp) {
                        maxTemp = temp;

                    }
                }
                metricValue = maxTemp;
            } else {
                metricValue = 0.0; // fallback
            }

            // threshold를 조회해 초과하면 DB에 저장 후, 로깅함.
            isNormal = thresholdPolicyService.evaluateThresholdAndLogViolation(
                    type, machineId, machineName,
                    metricName, metricValue, violationTime
            );
        }


    }

    /**
     * JSON 문자열을 Jackson JsonNode로 파싱
     * @param json 파싱할 JSON 문자열
     * @return 파싱된 JsonNode
     * @throws InvalidJsonException 파싱 실패 시
     */
    private JsonNode parseJson(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception e) {
            throw new InvalidJsonException("JSON 파싱 실패", e);
        }
    }

    /**
     * JSON 파싱 오류를 처리하기 위한 사용자 정의 예외 클래스
     */
    public static class InvalidJsonException extends RuntimeException {
        public InvalidJsonException(String message, Throwable cause) {
            super(message, cause);
        }
    }

}
