package kr.cs.interdata.api_backend.service.repository_service;

import kr.cs.interdata.api_backend.dto.ThresholdSetting;
import kr.cs.interdata.api_backend.entity.MetricsByType;
import kr.cs.interdata.api_backend.entity.TargetType;
import kr.cs.interdata.api_backend.repository.MetricsByTypeRepository;
import kr.cs.interdata.api_backend.repository.TargetTypeRepository;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class MonitoringDefinitionService {

    private final Logger logger = LoggerFactory.getLogger(MonitoringDefinitionService.class);
    private final TargetTypeRepository targetTypeRepository;
    private final MetricsByTypeRepository metricsByTypeRepository;

    public MonitoringDefinitionService(
            TargetTypeRepository targetTypeRepository,
            MetricsByTypeRepository metricsByTypeRepository) {
        this.targetTypeRepository = targetTypeRepository;
        this.metricsByTypeRepository = metricsByTypeRepository;
    }

    // 타입 등록 - targetType
    public void registerType(String type) {
        TargetType targetType = new TargetType();
        targetType.setType(type);
        targetTypeRepository.save(targetType);

        logger.info("Register type {} successfully.", type);
    }

    // 새로운 메트릭 등록
    public void saveMetric(String metricName, String unit, Double threshold){
        MetricsByType metric = new MetricsByType();
        metric.setMetricName(metricName);
        metric.setUnit(unit);
        metric.setOverThresholdValue(threshold);
        metricsByTypeRepository.save(metric);

        logger.info("Save new metric : {}({})", metricName, unit);
    }

    /**
     *  - 특정 타입의 메트릭 임계값을 ThresholdSetting으로 매핑하여 조회
     *
     * @param typeName "host" 또는 "container"와 같은 타입명
     * @return ThresholdSetting 객체
     */
    public ThresholdSetting findThresholdByType(String type, String typeName) {
        List<MetricsByType> metrics = metricsByTypeRepository.findByType_Type(typeName);

        ThresholdSetting thresholdSetting = new ThresholdSetting();
        metrics.forEach(metric -> {
            switch (metric.getMetricName()) {
                case "cpu":
                    if (type.equals("overThresholdValue")) {
                        thresholdSetting.setCpuPercent(String.valueOf(metric.getOverThresholdValue()));
                    }
                    else {
                        thresholdSetting.setCpuPercent(String.valueOf(metric.getUnderThresholdValue()));
                    }
                    break;
                case "memory":
                    if (type.equals("overThresholdValue")) {
                        thresholdSetting.setMemoryUsage(String.valueOf(metric.getOverThresholdValue()));
                    }
                    else {
                        thresholdSetting.setMemoryUsage(String.valueOf(metric.getUnderThresholdValue()));
                    }
                    break;
                case "diskReadDelta":
                    if (type.equals("overThresholdValue")) {
                        thresholdSetting.setDiskReadDelta(String.valueOf(metric.getOverThresholdValue()));
                    }
                    else {
                        thresholdSetting.setDiskReadDelta(String.valueOf(metric.getUnderThresholdValue()));
                    }
                    break;
                case "diskWriteDelta":
                    if (type.equals("overThresholdValue")) {
                        thresholdSetting.setDiskWriteDelta(String.valueOf(metric.getOverThresholdValue()));
                    }
                    else {
                        thresholdSetting.setDiskWriteDelta(String.valueOf(metric.getUnderThresholdValue()));
                    }
                    break;
                case "networkRx":
                    if (type.equals("overThresholdValue")) {
                        thresholdSetting.setNetworkRx(String.valueOf(metric.getOverThresholdValue()));
                    }
                    else {
                        thresholdSetting.setNetworkRx(String.valueOf(metric.getUnderThresholdValue()));
                    }
                    break;
                case "networkTx":
                    if (type.equals("overThresholdValue")) {
                        thresholdSetting.setNetworkTx(String.valueOf(metric.getOverThresholdValue()));
                    }
                    else {
                        thresholdSetting.setNetworkTx(String.valueOf(metric.getUnderThresholdValue()));
                    }
                    break;
                case "temperature":
                    if (type.equals("overThresholdValue")) {
                        thresholdSetting.setTemperature(String.valueOf(metric.getOverThresholdValue()));
                    }
                    else {
                        thresholdSetting.setTemperature(String.valueOf(metric.getUnderThresholdValue()));
                    }
                    break;
                default:
                    logger.warn("Unknown metric name found: {}", metric.getMetricName());
            }
        });

        return thresholdSetting;
    }

    /**
     * - 특정 메트릭의 임계값 업데이트
     *
     * @param metricName 메트릭 이름 (cpu, memory, disk, network)
     * @param thresholdValue 업데이트할 임계값
     */
    public void updateThresholdByMetricName(String type, String metricName, double thresholdValue) {
        // 해당 메트릭 이름을 가진 모든 MetricsByType 조회
        List<MetricsByType> metrics = metricsByTypeRepository.findByMetricName(metricName);

        // 각각의 임계값 업데이트
        if(type.equals("overThresholdValue")){
            metrics.forEach(metric -> metric.setOverThresholdValue(thresholdValue));
        }
        else if (type.equals("underThresholdValue")){
            metrics.forEach(metric -> metric.setUnderThresholdValue(thresholdValue));
        }
        else {
            logger.error("Unknown type found: {}, please -underThresholdValue- or -overThresholdValue-", type);
        }

        // 일괄 저장
        metricsByTypeRepository.saveAll(metrics);

        logger.info("Updated threshold for {} - {} metrics to {}", type, metricName, thresholdValue);
    }

    /**
     *  - 모든 타입의 모든 메트릭 임계값을 조회하여 Map 형태로 반환
     *
     * @return Map<type(String), Map<metric_name(String), threshold(Double)>> resultMap
     */
    public Map<String, Map<String, Double>> findAllThresholdsGroupedByType() {
        // Key는 'host' 또는 'container', Value는 해당 타입에 대한 메트릭들에 대한 Map을 선언한다.
        Map<String, Map<String, Double>> resultMap = new ConcurrentHashMap<>();

        // 모든 MetricsByType 데이터를 가져옴
        List<MetricsByType> metricsList = metricsByTypeRepository.findAll();

        // 데이터를 순차적으로 처리하여 결과 Map에 추가한다.
        metricsList.forEach(metric -> {
            String typeKey = metric.getType().getType(); // type (host, container)
            Map<String, Double> metricMap = resultMap.computeIfAbsent(typeKey, k -> new ConcurrentHashMap<>());
            metricMap.put(metric.getMetricName(), metric.getOverThresholdValue());
        });

        return resultMap;
    }

}
