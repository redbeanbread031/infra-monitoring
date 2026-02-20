package kr.cs.interdata.api_backend.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import kr.cs.interdata.api_backend.service.MetricService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
public class MetricController {

    private final MetricService metricService;

    public MetricController(MetricService metricService) {
        this.metricService = metricService;
    }


    @Operation( summary = "metric data 수신", description = "외부 consumer가 메트릭 데이터를 api-backend(POST)로 전달할 때 호출되는 API 엔드포인트입니다.",
            requestBody = @io.swagger.v3.oas.annotations.parameters.RequestBody(
                    description = "JSON 등 문자열 형태의 메트릭 데이터",
                    content = @Content(mediaType = "application/json",
                            examples = @ExampleObject(
                                    name = "Metric Data Example",
                                    value = "{\n" +
                                            "  \"type\": \"host\",\n" +
                                            "  \"hostId\": \"host ID\",\n" +
                                            "  \"name\": \"host name\",\n" +
                                            "  \"timeStamp\": \"yyyy-mm-dd'T'HH:MM:SS\",\n" +
                                            "  \"cpuUsagePercent\": 5.9,\n" +
                                            "  \"memoryUsedBytes\": 15519293440,\n" +
                                            "  \"diskReadBytesDelta\": 212992,\n" +
                                            "  \"diskWriteBytesDelta\": 605184,\n" +
                                            "  \"networkDelta\": {\n" +
                                            "    \"eth0\": {\n" +
                                            "      \"txBytesDelta\": 0,\n" +
                                            "      \"rxBytesDelta\": 0\n" +
                                            "    }\n" +
                                            "  },\n" +
                                            "  \"temperatures\": {\n" +
                                            "    \"x86_pkg_temp (thermal_zone0)\": 46.0,\n" +
                                            "    \"coretemp/Package id 0\": 46.0,\n" +
                                            "    \"coretemp/Core 0\": 31.0,\n" +
                                            "    \"coretemp/Core 1\": 30.0,\n" +
                                            "    \"coretemp/Core 2\": 31.0\n" +
                                            "  },\n" +
                                            "  \"containers\": {\n" +
                                            "    \"container-001\": {\n" +
                                            "      \"name\": \"app1\",\n" +
                                            "      \"cpuUsagePercent\": 39.89,\n" +
                                            "      \"memoryUsedBytes\": 47386624,\n" +
                                            "      \"diskReadBytesDelta\": 212992,\n" +
                                            "      \"diskWriteBytesDelta\": 605184,\n" +
                                            "      \"networkDelta\": {\n" +
                                            "        \"eth0\": {\n" +
                                            "          \"txBytesDelta\": 0,\n" +
                                            "          \"rxBytesDelta\": 0\n" +
                                            "        }\n" +
                                            "      }\n" +
                                            "    }\n" +
                                            "  }\n" +
                                            "}"
                            )
                    )
                    ),
            responses = {
                    @ApiResponse(responseCode = "200", description = "성공 (별도의 본문 없음, 반환은 하지만 응답은 보내지 않음을 의미)"
                    )
            }
    )
    @PostMapping("/metrics")
    public ResponseEntity<Void> sendMetrics(@RequestBody String metric) {
        metricService.sendMetric(metric);
        return ResponseEntity.ok().build();
    }


}
