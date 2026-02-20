package kr.cs.interdata.api_backend.service.threshold_service.mapper;

import kr.cs.interdata.api_backend.entity.AbnormalMetricLog;
import kr.cs.interdata.api_backend.service.repository_service.ContainerInventoryService;
import org.springframework.stereotype.Service;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class AbnormalMetricLogMapper {

    private final ContainerInventoryService containerInventoryService;

    public AbnormalMetricLogMapper(ContainerInventoryService containerInventoryService) {
        this.containerInventoryService = containerInventoryService;
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
    public List<Map<String, Object>> getMapList(List<AbnormalMetricLog> logs) {
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

}
