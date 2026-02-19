package kr.cs.interdata.api_backend.infra.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import kr.cs.interdata.api_backend.dto.abnormal_log_dto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class ThresholdSsePublisher {

    // 클라이언트의 Emitter를 저장할 ConcurrentHashMap
    private final Map<String, SseEmitter> emitters = new ConcurrentHashMap<>();
    private final Logger logger = LoggerFactory.getLogger(ThresholdSsePublisher.class);
    private final ObjectMapper objectMapper;

    public ThresholdSsePublisher(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    // ==============================
    //  SSE (Server-Sent Events) Real-time Alert Handling
    // ==============================

    // ------- 1. SSE Connection Management -------
    /**
     *  - sse방식을 사용하기 위해 비동기로 emitter를 연결한다.
     *  -> SSE 연결을 생성하고 Emitter를 관리한다.
     *
     * @return emitter 연결
     */
    public SseEmitter alertThreshold() {
        String emitterId = "emitter_" + System.currentTimeMillis();
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);

        // Emitter 저장
        emitters.put(emitterId, emitter);

        // 연결이 끊어지면 맵에서 제거
        // 즉, 클라이언트가 페이지를 벗어나거나 연결을 끊으면, SseEmitter의 콜백이 실행됨.
        emitter.onCompletion(() -> emitters.remove(emitterId));
        emitter.onTimeout(() -> emitters.remove(emitterId));
        emitter.onError((e) -> emitters.remove(emitterId));

        logger.info("Client Connected: {}", emitterId);
        return emitter;
    }

    /**
     * 일정한 주기로 SSE Emitter의 상태를 체크하여 끊어진 연결을 정리한다.
     * (5분마다 실행)
     */
    @Scheduled(fixedRate = 5 * 60 * 1000) // 5분마다
    public void cleanUpEmitters() {
        for (Map.Entry<String, SseEmitter> entry : emitters.entrySet()) {
            try {
                entry.getValue().send(SseEmitter.event().name("ping").data("keepalive"));
            } catch (Exception e) {
                emitters.remove(entry.getKey());
                logger.info("Cleaned up dead emitter: {}", entry.getKey());
            }
        }
    }

    /**
     * 서비스 종료 시 모든 SSE Emitter 연결을 정리한다.
     */
    @PreDestroy
    public void cleanUpAllEmitters() {
        emitters.forEach((id, emitter) -> emitter.complete());
        emitters.clear();
    }


    // ------- 2. Alert Broadcasting Methods -------
    /**
     *  - 임계값을 초과한 데이터가 발생하면 실시간으로 전송한다.
     *      -> 이상값이 생길 시, 5번 메서드와 함께 데이터를 처리하며 실행된다.
     *
     * @param alert     실시간 전송할 임계치를 넘은 데이터
     */
    public void publishThresholdExceeded(AlertThresholdExceeded alert) {
        String jsonData;
        try {
            jsonData = objectMapper.writeValueAsString(alert);
        } catch (IOException e) {
            // 변환에 실패하면 로깅만 하고 기본 메시지 설정
            logger.error("Failed to convert AlertThresholdExceeded to JSON. Sending default error message.", e);
            jsonData = "{\"error\": \"Failed to convert AlertThresholdExceeded to JSON\"}";
        }

        // 모든 Emitter에 브로드캐스트 전송
        for (Map.Entry<String, SseEmitter> entry : emitters.entrySet()) {
            try {
                entry.getValue().send(jsonData);
            } catch (IOException e) {
                logger.warn("Failed to send data to client. Removing emitter: {}", entry.getKey());
                entry.getValue().completeWithError(e);
                emitters.remove(entry.getKey());
            }
        }
    }

    /**
     *  - 임계값에 미달된 데이터가 발생하면 실시간으로 전송한다.
     *      -> 이상값이 생길 시, 5번 메서드와 함께 데이터를 처리하며 실행된다.
     *
     * @param alert     실시간 전송할 임계치에 미달된 데이터
     */
    public void publishThresholdDeceeded(AlertThresholdDeceeded alert) {
        String jsonData;
        try {
            jsonData = objectMapper.writeValueAsString(alert);
        } catch (IOException e) {
            // 변환에 실패하면 로깅만 하고 기본 메시지 설정
            logger.error("Failed to convert AlertThresholdDeceeded to JSON. Sending default error message.", e);
            jsonData = "{\"error\": \"Failed to convert AlertThresholdDeceeded to JSON\"}";
        }

        // 모든 Emitter에 브로드캐스트 전송
        for (Map.Entry<String, SseEmitter> entry : emitters.entrySet()) {
            try {
                entry.getValue().send(jsonData);
            } catch (IOException e) {
                logger.warn("Failed to send data to client. Removing emitter: {}", entry.getKey());
                entry.getValue().completeWithError(e);
                emitters.remove(entry.getKey());
            }
        }
    }

    /**
     *  - container가 꺼졌다 판단되면 실시간으로 전송한다.
     *      -> 이상로그가 생길 시, 5번 메서드와 함께 데이터를 처리하며 실행된다.
     *
     * @param alert  실시간 전송할 데이터
     */
    public void publishZeroValue(AlertZerovalue alert) {
        String jsonData;

        try {
            jsonData = objectMapper.writeValueAsString(alert);
        } catch (IOException e) {
            // 변환에 실패하면 로깅만 하고 기본 메시지 설정
            logger.error("Failed to convert AlertZerovalue to JSON. Sending default error message.", e);
            jsonData = "{\"error\": \"Failed to convert AlertZerovalue to JSON\"}";
        }

        // 모든 Emitter에 브로드캐스트 전송
        for (Map.Entry<String, SseEmitter> entry : emitters.entrySet()) {
            try {
                entry.getValue().send(jsonData);
            } catch (IOException e) {
                logger.warn("Failed to send data to client. Removing emitter: {}", entry.getKey());
                entry.getValue().completeWithError(e);
                emitters.remove(entry.getKey());
            }
        }
    }

    /**
     *  - container가 꺼졌다 켜진 후, containerId가 바뀌었다고 판단되면 실시간으로 전송한다.
     *      -> 이상로그가 생길 시, 5번 메서드와 함께 데이터를 처리하며 실행된다.
     *
     * @param alert 실시간 전송할 데이터
     */
    public void publishContainerIdChanged(AlertContainerIdChanged alert) {
        String jsonData;

        try {
            jsonData = objectMapper.writeValueAsString(alert);
        } catch (IOException e) {
            // 변환에 실패하면 로깅만 하고 기본 메시지 설정
            logger.error("Failed to convert AlertContainerIdChanged to JSON. Sending default error message.", e);
            jsonData = "{\"error\": \"Failed to convert AlertContainerIdChanged to JSON\"}";
        }

        // 모든 Emitter에 브로드캐스트 전송
        for (Map.Entry<String, SseEmitter> entry : emitters.entrySet()) {
            try {
                entry.getValue().send(jsonData);
            } catch (IOException e) {
                logger.warn("Failed to send data to client. Removing emitter: {}", entry.getKey());
                entry.getValue().completeWithError(e);
                emitters.remove(entry.getKey());
            }
        }
    }

    /**
     *  - 해당 데이터에 대해 1분이상 데이터가 조회되지 않을 시 이에 대한 이상 로그를 실시간으로 전송한다.
     *      -> 이상로그가 생길 시, 5번 메서드와 함께 데이터를 처리하며 실행된다.
     *
     * @param alert 실시간 전송할 데이터
     */
    public void publishTimeout(AlertTimeout alert) {
        String jsonData;

        try {
            jsonData = objectMapper.writeValueAsString(alert);
        } catch (IOException e) {
            // 변환에 실패하면 로깅만 하고 기본 메시지 설정
            logger.error("Failed to convert AlertTimeout to JSON. Sending default error message.", e);
            jsonData = "{\"error\": \"Failed to convert AlertTimeout to JSON\"}";
        }

        // 모든 Emitter에 브로드캐스트 전송
        for (Map.Entry<String, SseEmitter> entry : emitters.entrySet()) {
            try {
                entry.getValue().send(jsonData);
            } catch (IOException e) {
                logger.warn("Failed to send data to client. Removing emitter: {}", entry.getKey());
                entry.getValue().completeWithError(e);
                emitters.remove(entry.getKey());
            }
        }
    }


}
