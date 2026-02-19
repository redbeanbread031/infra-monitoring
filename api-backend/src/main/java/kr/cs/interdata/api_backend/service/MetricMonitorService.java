package kr.cs.interdata.api_backend.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.benmanes.caffeine.cache.Cache;
import kr.cs.interdata.api_backend.infra.cache.MachineMetricTimestamp;
import kr.cs.interdata.api_backend.service.repository_service.AbnormalDetectionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.Map;

@Service
public class MetricMonitorService {

    private final Cache<String, MachineMetricTimestamp> metricTimestampCache;
    private final Logger logger = LoggerFactory.getLogger(MetricMonitorService.class);
    private final AbnormalDetectionService abnormalDetectionService;

    public MetricMonitorService(
            Cache<String, MachineMetricTimestamp> metricTimestampCache,
            AbnormalDetectionService abnormalDetectionService) {
        this.metricTimestampCache = metricTimestampCache;
        this.abnormalDetectionService = abnormalDetectionService;
    }

    // 메트릭 수신 시 호출: 캐시에 시간 저장
    public void updateMetricTimestampWithNameKey(String type, String id, String name, @Nullable String parentHostName) {
        String key;

        if ("container".equals(type)) {
            key = "container:" + name + ":" + parentHostName;
        } else { // type.equals("host")
            key = "host:" + name;
        }

        MachineMetricTimestamp existing = metricTimestampCache.getIfPresent(key);

        if (existing != null) {
            // 기존 값이 있을 경우: ID가 변경된 경우만 업데이트
            metricTimestampCache.put(key, new MachineMetricTimestamp(LocalDateTime.now(), id, name, parentHostName));
            // 같다면 갱신 필요 없음
        } else {
            // 없으면 새로 등록
            metricTimestampCache.put(key, new MachineMetricTimestamp(LocalDateTime.now(), id, name, parentHostName));
            logger.info("Add cache -> {} | {} | {}", type, id, name);
        }
    }


    @Async
    public void updateTimestamps(JsonNode metricsNode) {
        String type = metricsNode.get("type").asText(); // "host"
        String hostId = metricsNode.get("hostId").asText();
        String hostName = metricsNode.get("name").asText();

        // 1. 호스트
        updateMetricTimestampWithNameKey(type, hostId, hostName, null);

        // 2. 컨테이너
        JsonNode containersNode = metricsNode.get("containers");
        if (containersNode != null && containersNode.isObject()) {
            Iterator<String> fieldNames = containersNode.fieldNames();
            while (fieldNames.hasNext()) {
                String containerId = fieldNames.next();
                JsonNode containerNode = containersNode.get(containerId);
                String containerName = containerNode.get("name").asText();
                updateMetricTimestampWithNameKey("container", containerId, containerName, hostName);
            }
        }
    }

    // 감시용 스케줄러
    @Scheduled(fixedRate = 10_000)
    public void checkMetricTimeouts() {
        LocalDateTime now = LocalDateTime.now();
        Map<String, MachineMetricTimestamp> snapshot = metricTimestampCache.asMap();

        for (Map.Entry<String, MachineMetricTimestamp> entry : snapshot.entrySet()) {
            String key = entry.getKey();
            MachineMetricTimestamp data = entry.getValue();

            if (Duration.between(data.getTimestamp(), now).toSeconds() >= 10) {
                // key 형식: type:machineName:parentHostName
                String[] parts = key.split(":", 3);
                String type = parts[0];
                String parentHostName = parts.length == 3 ? parts[2] : null;

                abnormalDetectionService.storeTimeout(type, data.getMachineId(), data.getMachineName(), now);

                metricTimestampCache.invalidate(key);

                logger.warn("store timeout for machineName={} type={} parentHostName={}", parts[1], type, parentHostName);
            }
        }
    }


}
