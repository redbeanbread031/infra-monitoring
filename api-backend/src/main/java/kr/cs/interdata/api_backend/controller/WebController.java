package kr.cs.interdata.api_backend.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import kr.cs.interdata.api_backend.infra.websocket.ThresholdSsePublisher;
import kr.cs.interdata.api_backend.dto.history_dto.HistoryFilter;
import kr.cs.interdata.api_backend.dto.history_dto.HistoryForMachineId;
import kr.cs.interdata.api_backend.service.repository_service.MachineInventoryService;
import kr.cs.interdata.api_backend.service.threshold.ThresholdPolicyService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import kr.cs.interdata.api_backend.dto.*;
import kr.cs.interdata.api_backend.service.threshold.ThresholdQueryService;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;

/**
 * 임계값(Threshold), 인벤토리, 임계값 알림 등
 * 주요 웹 API 엔드포인트를 제공하는 컨트롤러입니다.
 */
@RestController
@RequestMapping("/api")
public class WebController {

    private final ThresholdQueryService thresholdQueryService;
    private final ThresholdPolicyService thresholdPolicyService;
    private final ThresholdSsePublisher thresholdSsePublisher;
    private final MachineInventoryService machineInventoryService;

    @Autowired
    public WebController(ThresholdQueryService thresholdQueryService,
                         ThresholdSsePublisher thresholdSsePublisher,
                         MachineInventoryService machineInventoryService,
                         ThresholdPolicyService thresholdPolicyService) {
        this.thresholdQueryService = thresholdQueryService;
        this.thresholdSsePublisher = thresholdSsePublisher;
        this.machineInventoryService = machineInventoryService;
        this.thresholdPolicyService = thresholdPolicyService;
    }


    @Operation( summary = "Over Threshold 조회", description = "각 메트릭의 상한(Over Threshold) 임계값을 조회합니다.",
            responses = {
                    @ApiResponse(responseCode = "200", description = "성공",
                            content = @Content(mediaType = "application/json",
                                    examples = @ExampleObject(value =
                                            """
                                            {
                                                "cpuPercent": "",
                                                "memoryUsage": "",
                                                "diskReadDelta": "",
                                                "diskWriteDelta": "",
                                                "networkRx": "",
                                                "networkTx": "",
                                                "temperature" : ""
                                            }
                                            """))
                    ),
                    @ApiResponse(responseCode = "500", description = "서버 오류",
                            content = @Content(mediaType = "application/json",
                                    examples = @ExampleObject(value = "{\"error\": \"Internal server error\"}")
                            )
                    )
            }
    )
    @GetMapping("/metrics/threshold-setting")
    public ResponseEntity<?> getThreshold() {
        return ResponseEntity.ok(thresholdPolicyService.getThreshold());
    }


    @Operation( summary = "Over Threshold 설정", description = "각 메트릭의 상한(Over Threshold) 임계값을 설정합니다.",
            requestBody = @io.swagger.v3.oas.annotations.parameters.RequestBody(
                    description = "임계값 설정 값들", required = true,
                    content = @Content(schema = @Schema(implementation = ThresholdSetting.class))
            ),
            responses = {
                    @ApiResponse(responseCode = "200", description = "성공",
                            content = @Content(mediaType = "application/json", examples = @ExampleObject(value = "{\"message\": \"ok\"}"))
                    ),
                    @ApiResponse(responseCode = "400", description = "잘못된 입력",
                            content = @Content(mediaType = "application/json",
                                    examples = @ExampleObject(
                                            name = "BadRequestExample",
                                            summary = "유효하지 않은 임계값 설정",
                                            value = """
                                                    {
                                                        "error": "A value above the threshold cannot be lower than a value below the threshold.",
                                                        "overThresholdValues": {
                                                            "cpuPercent": "",
                                                            "memoryUsage": "",
                                                            "diskReadDelta": "",
                                                            "diskWriteDelta": "",
                                                            "networkRx": "",
                                                            "networkTx": "",
                                                            "temperature": ""
                                                        }
                                                    }
                                                    """
                                    )
                            )
                    ),
                    @ApiResponse(responseCode = "500", description = "서버 오류",
                            content = @Content(mediaType = "application/json",
                                    examples = @ExampleObject(value = "{\"error\": \"Internal server error\"}")
                            )
                    )
            }
    )
    @PostMapping("/metrics/threshold-setting")
    public ResponseEntity<?> setThreshold(@RequestBody ThresholdSetting dto) {
        // 서비스로 설정 요청 위임 (에러 발생 시 error 응답)
        ThresholdErrorResponse errorResponse = thresholdPolicyService.setThreshold(dto);

        if (errorResponse != null) {
            return ResponseEntity
                    .badRequest()
                    .body(errorResponse);   // 잘못된 입력 경우 400 반환
        }

        // 설정 성공 시 응답 메시지
        return ResponseEntity.ok(Map.of("message", "ok"));
    }

    @Operation( summary = "Under Threshold 조회", description = "각 메트릭의 하한(Under Threshold) 임계값을 조회합니다.",
            responses = {
                    @ApiResponse(responseCode = "200", description = "성공",
                            content = @Content(mediaType = "application/json",
                                    examples = @ExampleObject(value =
                                            """
                                            {
                                                "cpuPercent": "",
                                                "memoryUsage": "",
                                                "diskReadDelta": "",
                                                "diskWriteDelta": "",
                                                "networkRx": "",
                                                "networkTx": "",
                                                "temperature" : ""
                                            }
                                            """))
                    ),
                    @ApiResponse(responseCode = "500", description = "서버 오류",
                            content = @Content(mediaType = "application/json",
                                    examples = @ExampleObject(value = "{\"error\": \"Internal server error\"}")
                            )
                    )
            }
    )
    @GetMapping("/metrics/under-threshold-setting")
    public ResponseEntity<?> getUnderThreshold() {
        return ResponseEntity.ok(thresholdPolicyService.getUnderThreshold());
    }


    @Operation( summary = "Under Threshold 설정", description = "각 메트릭의 하한(Under Threshold) 임계값을 설정합니다.",
            requestBody = @io.swagger.v3.oas.annotations.parameters.RequestBody(
                    description = "임계값 설정 값들",
                    required = true,
                    content = @Content(schema = @Schema(implementation = ThresholdSetting.class))
            ),
            responses = {
                    @ApiResponse(responseCode = "200", description = "성공",
                            content = @Content(mediaType = "application/json", examples = @ExampleObject(value = "{\"message\": \"ok\"}"))
                    ),
                    @ApiResponse(responseCode = "400", description = "잘못된 입력",
                            content = @Content(
                                    mediaType = "application/json",
                                    examples = @ExampleObject(
                                            name = "BadRequestExample",
                                            summary = "유효하지 않은 임계값 설정",
                                            value = """
                                                    {
                                                        "error": "A value below the threshold cannot be greater than a value above the threshold.",
                                                        "overThresholdValues": {
                                                            "cpuPercent": "",
                                                            "memoryUsage": "",
                                                            "diskReadDelta": "",
                                                            "diskWriteDelta": "",
                                                            "networkRx": "",
                                                            "networkTx": "",
                                                            "temperature": ""
                                                        }
                                                    }
                                                    """
                                    )
                            )
                    ),
                    @ApiResponse(responseCode = "500", description = "서버 오류",
                            content = @Content(mediaType = "application/json",
                                    examples = @ExampleObject(value = "{\"error\": \"Internal server error\"}")
                            )
                    )
            }
    )
    @PostMapping("/metrics/under-threshold-setting")
    public ResponseEntity<?> setUnderThreshold(@RequestBody ThresholdSetting dto) {
        ThresholdErrorResponse errorResponse = thresholdPolicyService.setUnderThreshold(dto);

        if (errorResponse != null) {
            return ResponseEntity
                    .badRequest()
                    .body(errorResponse);    // 잘못된 입력 경우 400 반환
        }
        // 설정 성공 시 응답 메시지
        return ResponseEntity.ok(Map.of("message", "ok"));
    }

    // operation description 수정 필요함.
    @Operation( summary = "특정 기계의 이상 기록 조회",
            description = "특정 기계(targetId)의 이상 기록 목록을 조회합니다.",
            requestBody = @io.swagger.v3.oas.annotations.parameters.RequestBody(
                    description = "조회 대상 기계 ID 정보",
                    required = true,
                    content = @Content(schema = @Schema(implementation = HistoryForMachineId.class))
            ),
            responses = {
                    @ApiResponse(responseCode = "200", description = "성공",
                            content = @Content(mediaType = "application/json",
                                    examples = @ExampleObject(value =
                                            """
                                            [
                                                {
                                                    "timestamp": "",
                                                    "targetId": "",
                                                    "metricName": "",
                                                    "threshold": "",
                                                    "value": ""
                                                },
                                                {
                                                    "...": "...more"
                                                }
                                            ]
                                            """))
                    ),
                    @ApiResponse(responseCode = "500", description = "서버 오류",
                            content = @Content(mediaType = "application/json",
                                    examples = @ExampleObject(value = "{\"error\": \"Internal server error\"}")
                            )
                    )
            }
    )
    @GetMapping("/metrics/history")
    public ResponseEntity<?> getThresholdHistory(
            @RequestParam(required = false) String date,
            @RequestParam(required = false) String machineType,
            @RequestParam(required = false) String hostName,
            @RequestParam(required = false) String machineName,
            @RequestParam(required = false) String messageType,
            @RequestParam(required = false) String metricName
    ) {
        HistoryFilter filter = HistoryFilter.builder()
                .date(date)
                .machineType(machineType)
                .hostName(hostName)
                .machineName(machineName)
                .messageType(messageType)
                .metricName(metricName)
                .build();

        return ResponseEntity.ok(thresholdQueryService.getThresholdHistory(filter));
    }


    @Operation( summary = "모든 머신의 이상 기록 조회",
            description = "모든 머신에서 발생한 경보를 최신 기준으로 최대 50개 반환합니다.",
            responses = {
                    @ApiResponse(responseCode = "200", description = "성공",
                            content = @Content(mediaType = "application/json",
                                    examples = @ExampleObject(value =
                                            """
                                            [
                                                {
                                                    "messageType" : "",
                                                    "machineType" : "",
                                                    "machineId" : "",
                                                    "machineName" : "",
                                                    "hostName" : "",
                                                    "metricName" : "",
                                                    "threshold" : "",
                                                    "value" : "",
                                                    "timestamp" : ""
                                                },
                                                {
                                                    "...": "... 49 number more"
                                                }
                                            ]
                                            """))
                    ),
                    @ApiResponse(responseCode = "500", description = "서버 오류",
                            content = @Content(mediaType = "application/json",
                                    examples = @ExampleObject(value = "{\"error\": \"Internal server error\"}")
                            )
                    )
            }
    )
    @GetMapping("/metrics/threshold-history-all")
    public ResponseEntity<?> getThresholdHistoryAll() {
        return ResponseEntity.ok(thresholdQueryService.getThresholdHistortForAll());
    }


    @Operation(summary = "전체 기계/컨테이너 인벤토리 리스트 조회",
            description = "전체 기계/컨테이너 인벤토리 리스트를 조회합니다.",
            responses = {
                    @ApiResponse(responseCode = "200", description = "성공",
                            content = @Content(mediaType = "application/json",
                                    examples = @ExampleObject(value =
                                            """
                                            {
                                                  "host": [
                                                        {
                                                          "id": "",
                                                          "name": ""
                                                        },
                                                        {
                                                            "...": "...more"
                                                        }
                                                  ],
                                                  "container": [
                                                        {
                                                            "host" : "",
                                                            "id": "",
                                                            "name": ""
                                                        },
                                                        {
                                                            "...": "...more"
                                                        }
                                                  ]
                                            }
                                            """))
                    ),
                    @ApiResponse(responseCode = "500", description = "서버 오류",
                            content = @Content(mediaType = "application/json",
                                    examples = @ExampleObject(value = "{\"error\": \"Internal server error\"}")
                            )
                    )
            }
    )
    @GetMapping("/inventory/list")
    public ResponseEntity<?> getInventoryList() {
        return ResponseEntity.ok(machineInventoryService.getHostContainerInventoryList());
    }


    @Operation( summary = "SSE(Server-Sent Events) 방식의 임계값(Threshold) 알림 전송",
            description = "클라이언트는 /api/metrics/threshold-alert로 SSE 연결, 아래와 같은 5가지의 이상 알림들 중 하나를 실시간으로 수신합니다. 자세한 내용은 /api-backend/dto/abnormal_log_dto/를 참고하세요.",
            responses = {
                    @ApiResponse(responseCode = "200", description = "성공",
                            content = @Content(mediaType = "application/json",
                                    examples = @ExampleObject(value =
                                            """
                                            [
                                                {"type":"아래의 다섯가지 타입 중 하나를 전송합니다."},
                                                {
                                                    "messageType" : "thresholdExceeded",
                                                    "machineId" : "",
                                                    "machineName" : "",
                                                    "metricName" : "",
                                                    "value" : "",
                                                    "threshold": "",
                                                    "timestamp":""
                                                },
                                                {
                                                    "messageType" : "thresholdDeceeded",
                                                    "machineId" : "",
                                                    "machineName" : "",
                                                    "metricName" : "",
                                                    "value" : "",
                                                    "threshold": "",
                                                    "timestamp":""
                                                },
                                                {
                                                    "messageType" : "zerovalue",
                                                    "machineId" : "",
                                                    "machineName" : "",
                                                    "timestamp" : ""
                                                },
                                                {
                                                    "messageType" : "containerIdChanged",
                                                    "machineId" : "",
                                                    "machineName" : "",
                                                    "timestamp" : ""
                                                },
                                                {
                                                    "messageType" : "timeout",
                                                    "machineId" : "",
                                                    "machineName" : "",
                                                    "timestamp" : ""
                                                }
                                            ]
                                            """))
                    ),
                    @ApiResponse(responseCode = "500", description = "서버 오류",
                            content = @Content(mediaType = "application/json",
                                    examples = @ExampleObject(value = "{\"error\": \"Internal server error\"}")
                            )
                    )
            }
    )
    @GetMapping("/metrics/threshold-alert")
    public SseEmitter alertThreshold() {
        return thresholdSsePublisher.alertThreshold();
    }

}
