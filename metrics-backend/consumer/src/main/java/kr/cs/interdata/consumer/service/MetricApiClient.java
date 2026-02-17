package kr.cs.interdata.consumer.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;


@Service
public class MetricApiClient {

    // 로그 변수
    private final Logger logger = LoggerFactory.getLogger(MetricApiClient.class);

    private final WebClient webClient;

    @Autowired
    public MetricApiClient(WebClient webClient) {
        this.webClient = webClient;  // WebClient를 주입받음
    }

    /**
     * - 메트릭을 API 백엔드로 전송하는 메서드.
     *
     * @param metricData    메트릭 데이터
     */
    public void sendThresholdViolation(String metricData) {
        String url = "/api/metrics";

        // API 백엔드로 POST 요청을 보내고 응답을 처리
        webClient.post()
                .uri(url)
                .bodyValue(metricData)
                .retrieve()
                .bodyToMono(Void.class)   // 응답 본문 없음
                .doOnError(error -> {
                    // 에러 로그 처리
                    logger.warn("메트릭 전송 실패: {}", error.getMessage());
                })
                .subscribe();  // 비동기 방식으로 호출
                // 응답 사용하지 않음
    }

}
