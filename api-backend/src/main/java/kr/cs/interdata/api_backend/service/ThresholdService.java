package kr.cs.interdata.api_backend.service;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import kr.cs.interdata.api_backend.dto.*;
import kr.cs.interdata.api_backend.dto.abnormal_log_dto.*;
import kr.cs.interdata.api_backend.dto.history_dto.HistoryFilter;
import kr.cs.interdata.api_backend.dto.history_dto.HistoryForMachineId;
import kr.cs.interdata.api_backend.entity.AbnormalMetricLog;
import kr.cs.interdata.api_backend.infra.ThresholdStore;
import kr.cs.interdata.api_backend.infra.websocket.ThresholdSsePublisher;
import kr.cs.interdata.api_backend.repository.AbnormalMetricLogRepository;
import kr.cs.interdata.api_backend.service.repository_service.AbnormalDetectionService;
import kr.cs.interdata.api_backend.service.repository_service.ContainerInventoryService;
import kr.cs.interdata.api_backend.service.repository_service.MonitoringDefinitionService;
import kr.cs.interdata.api_backend.service.threshold.ThresholdEventService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@Service
public class ThresholdService {

    private final Map<String, Boolean> zeroStateCache = new ConcurrentHashMap<>();

    @Autowired
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Logger logger = LoggerFactory.getLogger(ThresholdService.class);

    private final AbnormalDetectionService abnormalDetectionService;
    private final MonitoringDefinitionService monitoringDefinitionService;
    private final ContainerInventoryService containerInventoryService;
    private final ThresholdStore thresholdStore;
    private final AbnormalMetricLogRepository abnormalMetricLogRepository;

    private final ThresholdEventService thresholdEventService;

    @Autowired
    public ThresholdService(ThresholdStore thresholdStore,
                            AbnormalDetectionService abnormalDetectionService,
                            MonitoringDefinitionService monitoringDefinitionService,
                            ContainerInventoryService containerInventoryService,
                            AbnormalMetricLogRepository abnormalMetricLogRepository,
                            ThresholdEventService thresholdEventService) {
        this.thresholdStore = thresholdStore;
        this.abnormalDetectionService = abnormalDetectionService;
        this.monitoringDefinitionService = monitoringDefinitionService;
        this.containerInventoryService = containerInventoryService;
        this.abnormalMetricLogRepository = abnormalMetricLogRepository;
        this.thresholdEventService = thresholdEventService;
    }

    // ==============================
    // 1. Threshold Setting: Get & Set (Over/Under)
    // ==============================

    /**
     * - 현재 설정된 over-threshold(임계 초과) 값을 조회
     * @return 각 메트릭의 over-threshold 값을 포함한 ThresholdSetting 객체
     */
    public ThresholdSetting getThreshold() {
        /*
         * "container"와 "host" 타입의 임계값은 같으므로
         * "host" 타입의 임계값을 조회해 가져온다.
         */
        return monitoringDefinitionService.findThresholdByType("overThresholdValue", "host");
    }

    /**
     *  - 새로운 over-threshold(임계 초과) 값을 설정
     * @param dto   각 메트릭의 over-threshold 값이 담긴 DTO
     * @return  오류가 있으면 ThresholdErrorResponse 객체, 없으면 null
     */
    public ThresholdErrorResponse setThreshold(ThresholdSetting dto) {
        Map<String, Double> underThresholdMap = new LinkedHashMap<>(thresholdStore.getUnderThresholdValues());

        Map<String, String> underThresholdStrMap = new LinkedHashMap<>();
        for (Map.Entry<String, Double> entry : underThresholdMap.entrySet()) {
            underThresholdStrMap.put(entry.getKey(), String.valueOf(entry.getValue()));
        }

        try {
            if (Double.parseDouble(dto.getCpuPercent()) < underThresholdMap.get("cpuPercent")) {
                return new ThresholdErrorResponse(errorMessage("overThresholdValue"), underThresholdStrMap);
            }
            if (Double.parseDouble(dto.getMemoryUsage()) < underThresholdMap.get("memoryUsage")) {
                return new ThresholdErrorResponse(errorMessage("overThresholdValue"), underThresholdStrMap);
            }
            if (Double.parseDouble(dto.getDiskReadDelta()) < underThresholdMap.get("diskReadDelta")) {
                return new ThresholdErrorResponse(errorMessage("overThresholdValue"), underThresholdStrMap);
            }
            if (Double.parseDouble(dto.getDiskWriteDelta()) < underThresholdMap.get("diskWriteDelta")) {
                return new ThresholdErrorResponse(errorMessage("overThresholdValue"), underThresholdStrMap);
            }
            if (Double.parseDouble(dto.getNetworkRx()) < underThresholdMap.get("networkRx")) {
                return new ThresholdErrorResponse(errorMessage("overThresholdValue"), underThresholdStrMap);
            }
            if (Double.parseDouble(dto.getNetworkTx()) < underThresholdMap.get("networkTx")) {
                return new ThresholdErrorResponse(errorMessage("overThresholdValue"), underThresholdStrMap);
            }
            if (Double.parseDouble(dto.getTemperature()) < underThresholdMap.get("temperature")) {
                return new ThresholdErrorResponse(errorMessage("overThresholdValue"), underThresholdStrMap);
            }
        } catch (NumberFormatException e) {
            return new ThresholdErrorResponse("Invalid number format in one or more fields.", underThresholdStrMap);
        }

        // 각 메트릭에 대한 임계값 업데이트
        monitoringDefinitionService.updateThresholdByMetricName("overThresholdValue", "cpu", Double.parseDouble(dto.getCpuPercent()));
        monitoringDefinitionService.updateThresholdByMetricName("overThresholdValue", "memory", Double.parseDouble(dto.getMemoryUsage()));
        monitoringDefinitionService.updateThresholdByMetricName("overThresholdValue", "diskReadDelta", Double.parseDouble(dto.getDiskReadDelta()));
        monitoringDefinitionService.updateThresholdByMetricName("overThresholdValue", "diskWriteDelta", Double.parseDouble(dto.getDiskWriteDelta()));
        monitoringDefinitionService.updateThresholdByMetricName("overThresholdValue", "networkRx", Double.parseDouble(dto.getNetworkRx()));
        monitoringDefinitionService.updateThresholdByMetricName("overThresholdValue", "networkTx", Double.parseDouble(dto.getNetworkTx()));
        monitoringDefinitionService.updateThresholdByMetricName("overThresholdValue", "temperature", Double.parseDouble(dto.getTemperature()));

        // 임계값 ThresholdStore에 저장 - over 값
        thresholdStore.updateOverThreshold("host", "cpu", Double.parseDouble(dto.getCpuPercent()));
        thresholdStore.updateOverThreshold("host", "memory", Double.parseDouble(dto.getMemoryUsage()));
        thresholdStore.updateOverThreshold("host", "diskReadDelta", Double.parseDouble(dto.getDiskReadDelta()));
        thresholdStore.updateOverThreshold("host", "diskWriteDelta", Double.parseDouble(dto.getDiskWriteDelta()));
        thresholdStore.updateOverThreshold("host", "networkRx", Double.parseDouble(dto.getNetworkRx()));
        thresholdStore.updateOverThreshold("host", "networkTx", Double.parseDouble(dto.getNetworkTx()));
        thresholdStore.updateOverThreshold("host", "temperature", Double.parseDouble(dto.getTemperature()));

        thresholdStore.updateOverThreshold("container", "cpu", Double.parseDouble(dto.getCpuPercent()));
        thresholdStore.updateOverThreshold("container", "memory", Double.parseDouble(dto.getMemoryUsage()));
        thresholdStore.updateOverThreshold("container", "diskReadDelta", Double.parseDouble(dto.getDiskReadDelta()));
        thresholdStore.updateOverThreshold("container", "diskWriteDelta", Double.parseDouble(dto.getDiskWriteDelta()));
        thresholdStore.updateOverThreshold("container", "networkRx", Double.parseDouble(dto.getNetworkRx()));
        thresholdStore.updateOverThreshold("container", "networkTx", Double.parseDouble(dto.getNetworkTx()));

        return null;
    }

    /**
     * - 현재 설정된 under-threshold(임계 미달) 값을 조회
     * @return 각 메트릭의 under-threshold 값을 포함한 ThresholdSetting 객체
     */
    public ThresholdSetting getUnderThreshold() {
        /*
         * "container"와 "host" 타입의 임계값은 같으므로
         * "host" 타입의 임계값을 조회해 가져온다.
         */
        return monitoringDefinitionService.findThresholdByType("underThresholdValue", "host");
    }

    /**
     * - 새로운 under-threshold(임계 미달) 값을 설정
     * @param dto 각 메트릭의 under-threshold 값이 담긴 DTO
     * @return 오류가 있으면 ThresholdErrorResponse 객체, 없으면 null
     */
    public ThresholdErrorResponse setUnderThreshold(ThresholdSetting dto) {
        Map<String, Double> overThresholdMap = new LinkedHashMap<>(thresholdStore.getOverThresholdValues());

        Map<String, String> overThresholdStrMap = new LinkedHashMap<>();
        for (Map.Entry<String, Double> entry : overThresholdMap.entrySet()) {
            overThresholdStrMap.put(entry.getKey(), String.valueOf(entry.getValue()));
        }

        try {
            if (Double.parseDouble(dto.getCpuPercent()) > overThresholdMap.get("cpuPercent")) {
                return new ThresholdErrorResponse(errorMessage("underThresholdValue"), overThresholdStrMap);
            }
            if (Double.parseDouble(dto.getMemoryUsage()) > overThresholdMap.get("memoryUsage")) {
                return new ThresholdErrorResponse(errorMessage("underThresholdValue"), overThresholdStrMap);
            }
            if (Double.parseDouble(dto.getDiskReadDelta()) > overThresholdMap.get("diskReadDelta")) {
                return new ThresholdErrorResponse(errorMessage("underThresholdValue"), overThresholdStrMap);
            }
            if (Double.parseDouble(dto.getDiskWriteDelta()) > overThresholdMap.get("diskWriteDelta")) {
                return new ThresholdErrorResponse(errorMessage("underThresholdValue"), overThresholdStrMap);
            }
            if (Double.parseDouble(dto.getNetworkRx()) > overThresholdMap.get("networkRx")) {
                return new ThresholdErrorResponse(errorMessage("underThresholdValue"), overThresholdStrMap);
            }
            if (Double.parseDouble(dto.getNetworkTx()) > overThresholdMap.get("networkTx")) {
                return new ThresholdErrorResponse(errorMessage("underThresholdValue"), overThresholdStrMap);
            }
            if (Double.parseDouble(dto.getTemperature()) > overThresholdMap.get("temperature")) {
                return new ThresholdErrorResponse(errorMessage("underThresholdValue"), overThresholdStrMap);
            }
        } catch (NumberFormatException e) {
            return new ThresholdErrorResponse("Invalid number format in one or more fields.", overThresholdStrMap);
        }

        // 각 메트릭에 대한 임계값 업데이트
        monitoringDefinitionService.updateThresholdByMetricName("underThresholdValue", "cpu", Double.parseDouble(dto.getCpuPercent()));
        monitoringDefinitionService.updateThresholdByMetricName("underThresholdValue", "memory", Double.parseDouble(dto.getMemoryUsage()));
        monitoringDefinitionService.updateThresholdByMetricName("underThresholdValue", "diskReadDelta", Double.parseDouble(dto.getDiskReadDelta()));
        monitoringDefinitionService.updateThresholdByMetricName("underThresholdValue", "diskWriteDelta", Double.parseDouble(dto.getDiskWriteDelta()));
        monitoringDefinitionService.updateThresholdByMetricName("underThresholdValue", "networkRx", Double.parseDouble(dto.getNetworkRx()));
        monitoringDefinitionService.updateThresholdByMetricName("underThresholdValue", "networkTx", Double.parseDouble(dto.getNetworkTx()));
        monitoringDefinitionService.updateThresholdByMetricName("underThresholdValue", "temperature", Double.parseDouble(dto.getTemperature()));

        // 임계값 ThresholdStore에 저장 - under 값
        thresholdStore.updateUnderThreshold("host", "cpu", Double.parseDouble(dto.getCpuPercent()));
        thresholdStore.updateUnderThreshold("host", "memory", Double.parseDouble(dto.getMemoryUsage()));
        thresholdStore.updateUnderThreshold("host", "diskReadDelta", Double.parseDouble(dto.getDiskReadDelta()));
        thresholdStore.updateUnderThreshold("host", "diskWriteDelta", Double.parseDouble(dto.getDiskWriteDelta()));
        thresholdStore.updateUnderThreshold("host", "networkRx", Double.parseDouble(dto.getNetworkRx()));
        thresholdStore.updateUnderThreshold("host", "networkTx", Double.parseDouble(dto.getNetworkTx()));
        thresholdStore.updateUnderThreshold("host", "temperature", Double.parseDouble(dto.getTemperature()));

        thresholdStore.updateUnderThreshold("container", "cpu", Double.parseDouble(dto.getCpuPercent()));
        thresholdStore.updateUnderThreshold("container", "memory", Double.parseDouble(dto.getMemoryUsage()));
        thresholdStore.updateUnderThreshold("container", "diskReadDelta", Double.parseDouble(dto.getDiskReadDelta()));
        thresholdStore.updateUnderThreshold("container", "diskWriteDelta", Double.parseDouble(dto.getDiskWriteDelta()));
        thresholdStore.updateUnderThreshold("container", "networkRx", Double.parseDouble(dto.getNetworkRx()));
        thresholdStore.updateUnderThreshold("container", "networkTx", Double.parseDouble(dto.getNetworkTx()));

        return null;
    }

    /**
     * 임계값 상하관계 위반 시 반환할 에러 메시지 생성
     * @param type "underThresholdValue" 또는 "overThresholdValue"
     * @return 에러 메시지 문자열
     */
    private String errorMessage(String type) {
        if (type.equals("underThresholdValue")) {
            return "A value below the threshold cannot be greater than a value above the threshold.";
        }
        else if (type.equals("overThresholdValue")) {
            return "A value above the threshold cannot be lower than a value below the threshold.";
        }
        else {
            return null;
        }
    }

    // ==============================
    // 2. Abnormal Log History Retrieval
    // ==============================

    /**
     *  - 특정 machine Id의 이상 로그 이력 조회
     * @param machineId  조회할 machine Id
     * @return  이력 리스트
     */
    public List<Map<String, Object>> getThresholdHistoryforMachineId(HistoryForMachineId machineId) {
        // Service를 통해 DB 조회
        List<AbnormalMetricLog> logs = abnormalDetectionService.getLatestAbnormalMetricsByMachineId(machineId.getTargetId());

        // 결과를 클라이언트에 맞게 매핑 및 반환
        return getMapList(logs);
    }

    /**
     * 이상 로그 이력(AbnormalMetricLog) 조회
     * - 날짜만 받으면 해당 날짜의 이상 로그를 최신순 50개까지 반환.
     * - 여러 필터(머신 유형, 호스트명, 메시지타입 등)를 복합적으로 받으면, 조건에 맞는 로그 중 최신순 50개를 반환.
     * - API 응답은 Map<String, Object> 형태의 리스트로 반환. timestamp는 "yyyy-MM-dd'T'HH:mm:ss" 포맷의 문자열로 변환하여 제공.
     *
     * @param filter HistoryFilter (date, machineType, hostName, machineName, messageType, metricName 등)
     * @return 최근순 50개 이력 로그 Map List
     */

    public List<Map<String, Object>> getThresholdHistory(HistoryFilter filter) {
        //로그 파라미터 및 filter 객체 상태 로그 출력
        logger.info("[LOG] getThresholdHistory 메서드 진입");
        logger.info("[LOG] 전달된 파라미터: {}", filter);

        LocalDateTime start = null;
        LocalDateTime end = null;

        //날짜 파라미터가 있으면 LocalDateTime 범위 파싱
        if (filter.getDate() != null) {
            try {
                LocalDate date = LocalDate.parse(filter.getDate());
                start = date.atStartOfDay();// 00:00:00
                end = date.atTime(LocalTime.MAX);// 23:59:59.999...
            } catch (Exception e) {
                logger.error("[ERROR] 날짜 변환 오류: {}", filter.getDate());
            }
        }

        //파라미터가 오젝 날짜만 있는 경우(다른 필터 조건은 모두 null)
        boolean isOnlyDate = filter.getMachineType() == null &&
                filter.getHostName() == null &&
                filter.getMachineName() == null &&
                filter.getMessageType() == null &&
                filter.getMetricName() == null;

        //실제 쿼리 실행 전, 사용된 파라미터 값 로그 출력
        //잘 되는지 확인하기 위해 넣어놓음.
        logger.info("[LOG] findFilteredLogs 호출 파라미터: " +
                        "start={}, end={}, machineType={}, hostName={}," +
                        " machineName={}, messageType={}, metricName={}",
                start, end, filter.getMachineType(), filter.getHostName(),
                filter.getMachineName(), filter.getMessageType(), filter.getMetricName());


        try {
            List<AbnormalMetricLog> logs;

            //날짜만 필터 시 : 해당 날짜 범위 내 이상 로그를 최신순(내림차순) 50개로 조회
            if (isOnlyDate) {
                logger.info("[INFO] 날짜만 들어온 요청입니다 ");
                logs=abnormalMetricLogRepository.findByTimestampBetweenOrderByTimestampDesc(start, end);
            } else {
                //복합필터 사용 시 : 여러 조건을 조합한 쿼리로 최신순 50개 조회
                logs = abnormalMetricLogRepository.findFilteredLogs(
                        start,
                        end,
                        filter.getMachineType(),
                        filter.getHostName(),
                        filter.getMachineName(),
                        filter.getMessageType(),
                        filter.getMetricName()
                );
            }

            logger.info("[LOG] 로그 쿼리 결과 개수: {}", logs.size());

            //AdnormalMetricLog 엔티티 -> Map<String,Object> 형식으로 변환(최대 50개)
            return logs.stream()
                    .limit(50)
                    .map(log -> {
                        try {
                            Map<String, Object> map = new LinkedHashMap<>();
                            map.put("messageType", log.getMessageType());
                            map.put("machineType", log.getMachineType());
                            map.put("machineId", log.getMachineId());
                            map.put("machineName", log.getMachineName());
                            map.put("hostName", log.getHostName());
                            map.put("metricName", log.getMetricName());
                            map.put("threshold", log.getThreshold());
                            map.put("value", log.getValue());
                            map.put("timestamp", log.getTimestamp());

                            if (log.getTimestamp() != null) {
                                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
                                map.put("timestamp", log.getTimestamp().format(formatter));
                            } else {
                                map.put("timestamp", null);
                            }
                            return map;
                        } catch (Exception e) {
                            //매핑 변환 중 예외 발생 시, 로그 출력 후 무시
                            logger.error("[ERROR] map 변환 중 오류 발생: {}", e.getMessage());
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)//변환 실패 row 제외
                    .collect(Collectors.toList());

        } catch (Exception e) {
            //전체 처리 과정에서 예외 발생 시 빈 리스트 반환
            logger.error("[ERROR] getThresholdHistory 전체 처리 중 오류 발생: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     *  - 모든 머신의 이상 로그 이력 조회
     *
     * @return  이력 리스트 (최신 기준으로 최대 50개)
     */
    public List<Map<String, Object>> getThresholdHistortForAll() {
        // Service를 통해 DB 조회
        List<AbnormalMetricLog> logs = abnormalDetectionService.getLatestAbnormalMetrics();

        // 결과를 클라이언트에 맞게 매핑 및 반환
        return getMapList(logs);
    }

    /**
     * AbnormalMetricLog 리스트를 클라이언트에 반환할 Map 리스트 형식으로 변환한다.
     * <p>
     * 각 로그의 정보를 Map으로 변환하며,
     * 만약 machineType이 "container"인 경우 containerInventoryService를 통해 해당 컨테이너의 hostName을 추가한다.
     * 기타 정보(timestamp, messageType, machineType, machineId, machineName, metricName, threshold, value)도 포함된다.
     * </p>
     *
     * @param logs AbnormalMetricLog 객체 리스트
     * @return 각 로그를 Map<String, Object>로 변환한 리스트
     */
    private List<Map<String, Object>> getMapList(List<AbnormalMetricLog> logs) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (AbnormalMetricLog log : logs) {
            Map<String, Object> record = new HashMap<>();

            // ContainerInventory 엔티티의 machineId와 machineName을 파싱해서 둘 조합이 있으면 종속된 hostName을 넘겨줌
            String hostName;
            if (log.getMachineType().equals("container")) {
                hostName = containerInventoryService.getHostNameByContainerId(log.getMachineId());
            } else {
                hostName = log.getMachineName();
            }

            record.put("timestamp", log.getTimestamp().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")));
            record.put("messageType", log.getMessageType());
            record.put("machineType", log.getMachineType());
            record.put("machineId", log.getMachineId());
            record.put("machineName", log.getMachineName());
            record.put("metricName", log.getMetricName());
            record.put("hostName", hostName);
            record.put("threshold", log.getThreshold());
            record.put("value", log.getValue());

            result.add(record);
        }
        return result;
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
            isNormal = evaluateThresholdAndLogViolation(type , machineId, machineName,
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

                    isNormal = evaluateThresholdAndLogViolation(
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

                isNormal = evaluateThresholdAndLogViolation(
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
            isNormal = evaluateThresholdAndLogViolation(
                type, machineId, machineName,
                metricName, metricValue, violationTime
            );
        }


    }

    /**
     * - 주어진 메트릭 값이 임계값(threshold)을 초과 및 미달했는지 판단하고,
     * 초과 시 로그 출력 및 위반 기록을 저장합니다.
     *
     * @param type           대상 종류 (예: host, container 등)
     * @param machineId      대상 ID (hostId 또는 containerId)
     * @param metricName     메트릭 이름 (예: cpuUsagePercent 등)
     * @param value          현재 측정된 메트릭 값
     * @param violationTime  측정 시각 또는 위반 발생 시각
     * @return true  - 임계값 미초과 또는 미미달 또는 임계값이 없음<br>
     *         false - 임계값 초과 및 미달 (위반 저장됨)
     */
    public boolean evaluateThresholdAndLogViolation(String type, String machineId, String machineName,
                                    String metricName, Double value, LocalDateTime violationTime) {
        // thresholdStore에서 해당 메트릭의 임계값을 조회
        Double overThreshold = thresholdStore.getOverThreshold(type, metricName);
        Double underThreshold = thresholdStore.getUnderThreshold(type, metricName);

        // 1. 임계값이 존재하고, 메트릭이 임계값을 초과한 경우
        if (overThreshold != null && value > overThreshold) {
            logger.warn("임계값 초과: {} | {} | {} -> {} = {} (임계값: {})"
                    , type, machineId, machineName, metricName, value, overThreshold);

            // 위반 정보 객체 생성 및 필드 설정
            StoreThresholdViolated storeThresholdViolated = new StoreThresholdViolated();
            storeThresholdViolated.setType(type);
            storeThresholdViolated.setMachineId(machineId);
            storeThresholdViolated.setMachineName(machineName);
            storeThresholdViolated.setMetricName(metricName);
            storeThresholdViolated.setValue(String.valueOf(value));
            storeThresholdViolated.setThreshold(String.valueOf(overThreshold));
            storeThresholdViolated.setTimestamp(violationTime);

            // 위반 기록 저장
            thresholdEventService.storeThresholdExceededLog(storeThresholdViolated);

            return false;
        }
        // 2. 임계값 존재하고, 메트릭이 임계값에 미달된 경우
        else if (underThreshold != null && (value < underThreshold)) {
            logger.warn("임계값 미달: {} | {} | {} -> {} = {} (임계값: {})"
                , type, machineId, machineName, metricName, value, underThreshold);

            // 위반 정보 객체 생성 및 필드 설정
            StoreThresholdViolated storeThresholdViolated = new StoreThresholdViolated();
            storeThresholdViolated.setType(type);
            storeThresholdViolated.setMachineId(machineId);
            storeThresholdViolated.setMachineName(machineName);
            storeThresholdViolated.setMetricName(metricName);
            storeThresholdViolated.setValue(String.valueOf(value));
            storeThresholdViolated.setThreshold(String.valueOf(underThreshold));
            storeThresholdViolated.setTimestamp(violationTime);

            // 위반 기록 저장
            thresholdEventService.storeThresholdDeceededLog(storeThresholdViolated);

            return false;
        }
        // 3.
        else if (overThreshold != null && underThreshold != null) {
            // 임계값에 미달되거나 초과하지 않음 -> 아무것도 하지 않는 상태
        }
        // 4. 임계값 자체가 존재하지 않는 경우
        else {
            logger.warn("임계값이 조회되지 않았습니다.");
        }

        // 임계값을 초과하지 않았거나, 임계값에 미달되지 않았거나, 임계값이 존재하지 않을 때 true 반환
        return true;
    }

    // ==============================
    // 6. JSON Parsing & Exception Handling
    // ==============================

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
