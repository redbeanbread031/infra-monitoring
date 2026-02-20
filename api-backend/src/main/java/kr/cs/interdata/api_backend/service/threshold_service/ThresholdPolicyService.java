package kr.cs.interdata.api_backend.service.threshold_service;

import kr.cs.interdata.api_backend.dto.StoreThresholdViolated;
import kr.cs.interdata.api_backend.dto.ThresholdErrorResponse;
import kr.cs.interdata.api_backend.dto.ThresholdSetting;
import kr.cs.interdata.api_backend.infra.ThresholdStore;
import kr.cs.interdata.api_backend.service.repository_service.MonitoringDefinitionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class ThresholdPolicyService {

    private final MonitoringDefinitionService monitoringDefinitionService;
    private final ThresholdStore thresholdStore;
    private final ThresholdEventService thresholdEventService;
    private final Logger logger = LoggerFactory.getLogger(ThresholdPolicyService.class);

    public ThresholdPolicyService(MonitoringDefinitionService monitoringDefinitionService,
                                  ThresholdStore thresholdStore,
                                  ThresholdEventService thresholdEventService) {
        this.monitoringDefinitionService = monitoringDefinitionService;
        this.thresholdStore = thresholdStore;
        this.thresholdEventService = thresholdEventService;
    }

    // ==============================
    //  Threshold Setting: Get & Set (Over/Under)
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


}
