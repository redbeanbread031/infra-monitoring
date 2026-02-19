package kr.cs.interdata.api_backend.service.threshold;

import kr.cs.interdata.api_backend.dto.StoreThresholdViolated;
import kr.cs.interdata.api_backend.dto.abnormal_log_dto.*;
import kr.cs.interdata.api_backend.infra.websocket.ThresholdSsePublisher;
import kr.cs.interdata.api_backend.service.repository_service.AbnormalDetectionService;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;

@Component
public class ThresholdEventService {

    private final ThresholdSsePublisher thresholdSsePublisher;
    private final AbnormalDetectionService abnormalDetectionService;

    public ThresholdEventService(ThresholdSsePublisher thresholdSsePublisher,
                                 AbnormalDetectionService abnormalDetectionService) {
        this.thresholdSsePublisher = thresholdSsePublisher;
        this.abnormalDetectionService = abnormalDetectionService;
    }

    // ==============================
    //  Store Abnormal Events (Threshold Violation/Change/Timeout)
    // ==============================

    // ------- 1. Over/Under Threshold Events -------
    /*
     *  - threshold를 넘은 값이 생길 시 이를 처리하는 메서드
     *
     * @param dto
     *        - type            : 이상 로그 발생 머신의 type
     *        - machineId       : 이상 로그 발생 머신의 ID
     *        - machineName     : 이상 로그 발생 머신의 name
     *        - metricName      : (임계초과)메트릭 이름
     *        - value           : 임계값을 넘은 값
     *        - timestamp       : 임계값을 넘은 시각
     */
    public void storeThresholdExceededLog(StoreThresholdViolated dto) {
        String machineId = dto.getMachineId();
        String type = dto.getType();
        String machineName = dto.getMachineName();
        String metricName = dto.getMetricName();
        String threshold = dto.getThreshold();
        String value = dto.getValue();
        LocalDateTime timestamp = dto.getTimestamp();

        abnormalDetectionService.storeThresholdExceeded(
                type,
                machineId,
                machineName,
                metricName,
                threshold,
                value,
                timestamp
        );

        // 실시간 전송 준비
        AlertThresholdExceeded alert = new AlertThresholdExceeded();
        alert.setMachineId(machineId);
        alert.setMachineName(machineName);
        alert.setMetricName(metricName);
        alert.setValue(value);
        alert.setThreshold(threshold);
        alert.setTimestamp(timestamp);

        // 실시간 전송 (비동기 처리)
        CompletableFuture.runAsync(() ->
                thresholdSsePublisher.publishThresholdExceeded(alert));

    }

    /*
     *  - threshold에 미달된 값이 생길 시 이를 처리하는 메서드
     *
     * @param dto
     *        - type            : 이상 로그 발생 머신의 type
     *        - machineId       : 이상 로그 발생 머신의 ID
     *        - machineName     : 이상 로그 발생 머신의 name
     *        - metricName      : (임계미달)메트릭 이름
     *        - value           : 임계값에 미달된 값
     *        - timestamp       : 임계값에 미달된 시각
     */
    public void storeThresholdDeceededLog(StoreThresholdViolated dto) {
        String machineId = dto.getMachineId();
        String type = dto.getType();
        String machineName = dto.getMachineName();
        String metricName = dto.getMetricName();
        String threshold = dto.getThreshold();
        String value = dto.getValue();
        LocalDateTime timestamp = dto.getTimestamp();

        abnormalDetectionService.storeThresholdDeceeded(
                type,
                machineId,
                machineName,
                metricName,
                threshold,
                value,
                timestamp
        );

        // 실시간 전송 준비
        AlertThresholdDeceeded alert = new AlertThresholdDeceeded();
        alert.setMachineId(machineId);
        alert.setMachineName(machineName);
        alert.setMetricName(metricName);
        alert.setValue(value);
        alert.setThreshold(threshold);
        alert.setTimestamp(timestamp);

        // 실시간 전송 (비동기 처리)
        CompletableFuture.runAsync(() ->
                thresholdSsePublisher.publishThresholdDeceeded(alert));

    }


    // ------- 2. Special Events -------
    /**
     *  - container가 꺼졌다 판단되면 이상로그를 발생시키고 이를 처리하는 메서드
     *
     * @param type          이상 로그 발생 머신의 type
     * @param machineId     이상 로그 발생 머신의 ID
     * @param machineName   이상 로그 발생 머신의 name
     * @param violationTime 이상 로그가 발생한 시각
     */
    public void storeZeroValueLog(String type, String machineId, String machineName, LocalDateTime violationTime) {
        abnormalDetectionService.storeZeroValue(
                type,
                machineId,
                machineName,
                violationTime);

        // 실시간 전송 준비
        AlertZerovalue alertZerovalue = new AlertZerovalue();
        alertZerovalue.setMachineId(machineId);
        alertZerovalue.setMachineName(machineName);
        alertZerovalue.setTimestamp(violationTime);

        // 실시간 전송 (비동기 처리)
        CompletableFuture.runAsync(() ->
                thresholdSsePublisher.publishZeroValue(alertZerovalue));
    }

    /**
     *  - container가 꺼졌다 켜진 후, containerId가 바뀌었다고 판단되면 이상로그를 발생시키고 이를 처리하는 메서드
     *
     * @param containerId       이상 로그 발생 머신의 ID
     * @param containerName     이상 로그 발생 머신의 name
     * @param violationTime     이상 로그가 발생한 시각
     */
    public void storeContainerIdChanged(String containerId, String containerName, LocalDateTime violationTime) {
        abnormalDetectionService.storeContainerIdChanged(
                "container",
                containerId,
                containerName,
                violationTime
        );

        // 실시간 전송 준비
        AlertContainerIdChanged alertContainerIdChanged = new AlertContainerIdChanged();
        alertContainerIdChanged.setMachineId(containerId);
        alertContainerIdChanged.setMachineName(containerName);
        alertContainerIdChanged.setTimestamp(violationTime);

        // 실시간 전송 (비동기 처리)
        CompletableFuture.runAsync(() ->
                thresholdSsePublisher.publishContainerIdChanged(alertContainerIdChanged));
    }

    /**
     *  - 해당 데이터에 대해 1분이상 데이터가 조회되지 않을 시 이에 대한 이상 로그를 발생시키고 이를 처리하는 메서드
     *
     * @param type              이상 로그 발생 머신의 type
     * @param machineId         이상 로그 발생 머신의 ID
     * @param machineName       이상 로그 발생 머신의 name
     * @param violationTime     이상 로그가 발생한 시각
     */
    public void storeTimeout(String type, String machineId, String machineName, LocalDateTime violationTime) {
        abnormalDetectionService.storeTimeout(
                type,
                machineId,
                machineName,
                violationTime
        );

        // 실시간 전송 준비
        AlertTimeout alertTimeout = new AlertTimeout();
        alertTimeout.setMachineId(machineId);
        alertTimeout.setMachineName(machineName);
        alertTimeout.setTimestamp(violationTime);

        // 실시간 전송 (비동기 처리)
        CompletableFuture.runAsync(() ->
                thresholdSsePublisher.publishTimeout(alertTimeout));
    }

}
