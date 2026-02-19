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
import kr.cs.interdata.api_backend.service.threshold.ThresholdPolicyService;
import kr.cs.interdata.api_backend.service.threshold.mapper.AbnormalMetricLogMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@Service
public class ThresholdService {

    @Autowired
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Logger logger = LoggerFactory.getLogger(ThresholdService.class);

    private final AbnormalDetectionService abnormalDetectionService;
    private final AbnormalMetricLogMapper abnormalMetricLogMapper;
    private final AbnormalMetricLogRepository abnormalMetricLogRepository;

    @Autowired
    public ThresholdService(AbnormalDetectionService abnormalDetectionService,
                            AbnormalMetricLogMapper abnormalMetricLogMapper,
                            AbnormalMetricLogRepository abnormalMetricLogRepository) {
        this.abnormalDetectionService = abnormalDetectionService;
        this.abnormalMetricLogMapper = abnormalMetricLogMapper;
        this.abnormalMetricLogRepository = abnormalMetricLogRepository;
    }

    /**
     *  - 특정 machine Id의 이상 로그 이력 조회
     * @param machineId  조회할 machine Id
     * @return  이력 리스트
     */
    public List<Map<String, Object>> getThresholdHistoryforMachineId(HistoryForMachineId machineId) {
        // Service를 통해 DB 조회
        List<AbnormalMetricLog> logs = abnormalDetectionService.getLatestAbnormalMetricsByMachineId(machineId.getTargetId());

        // 결과를 클라이언트에 맞게 매핑 및 반환
        return abnormalMetricLogMapper.getMapList(logs);
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
        return abnormalMetricLogMapper.getMapList(logs);
    }


}
