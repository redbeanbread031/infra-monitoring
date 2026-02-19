package kr.cs.interdata.api_backend.service.repository_service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import kr.cs.interdata.api_backend.dto.HostContainerInventoryList;
import kr.cs.interdata.api_backend.entity.ContainerInventory;
import kr.cs.interdata.api_backend.entity.HostMachineInventory;
import kr.cs.interdata.api_backend.entity.TargetType;
import kr.cs.interdata.api_backend.repository.ContainerInventoryRepository;
import kr.cs.interdata.api_backend.repository.HostMachineInventoryRepository;
import kr.cs.interdata.api_backend.repository.TargetTypeRepository;
import kr.cs.interdata.api_backend.service.threshold.ThresholdQueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;


@Service
public class MachineInventoryService {

    @Autowired
    private final ObjectMapper objectMapper = new ObjectMapper();

    public final TargetTypeRepository targetTypeRepository;
    private final HostMachineInventoryRepository hostMachineInventoryRepository;
    private final ContainerInventoryRepository containerInventoryRepository;
    private final ThresholdQueryService thresholdQueryService;
    private final Logger logger = LoggerFactory.getLogger(MachineInventoryService.class);

    @Autowired
    public MachineInventoryService(TargetTypeRepository targetTypeRepository,
                                   HostMachineInventoryRepository hostMachineInventoryRepository,
                                   ContainerInventoryRepository containerInventoryRepository,
                                   ThresholdQueryService thresholdQueryService) {
        this.targetTypeRepository = targetTypeRepository;
        this.hostMachineInventoryRepository = hostMachineInventoryRepository;
        this.containerInventoryRepository = containerInventoryRepository;
        this.thresholdQueryService = thresholdQueryService;
    }



    /**
     *  - 파라미터로 준 metric data에서의 머신 정보들의 DB 존재 여부 판별 및 삽입 메서드
     *
     * @param metricData    metric data
     */
    @Async
    public void registerMachineIfAbsent(String metricData) {
        JsonNode root = parseJson(metricData);

        if (root == null) {
            logger.error("Null parameter detected - metric data: {}", root);
            return;
        }

        String hostId = root.path("hostId").asText();   // host id
        String hostName = root.path("name").asText();   // host name
        LocalDateTime timestamp = LocalDateTime.parse(root.path("timeStamp").asText());

        // hostInventory에 있는지 없는지 판별 없으면 삽입 또는 수정
        if (!hostMachineInventoryRepository.existsByHostIdAndHostName(hostId, hostName)) {
            Optional<HostMachineInventory> existingByName = hostMachineInventoryRepository.findByHostName(hostName);

            // 1. hostName으로 이미 등록된 row가 있으면 -> hostId만 업데이트
            if (existingByName.isPresent()) {
                HostMachineInventory existing = existingByName.get();
                existing.setHostId(hostId);
                hostMachineInventoryRepository.save(existing); // update
                logger.info("Updated hostId for existing hostName '{}': new hostId = {}", hostName, hostId);
            } else {
                // 2. 완전히 새로운 경우 -> insert
                TargetType type = targetTypeRepository.findByType("host")
                        .orElseGet(() -> targetTypeRepository.save(TargetType.builder().type("host").build()));

                HostMachineInventory newHost = new HostMachineInventory();
                newHost.setType(type);
                newHost.setHostId(hostId);
                newHost.setHostName(hostName);
                hostMachineInventoryRepository.save(newHost);

                logger.info("Inserted new host entry: hostId = {}, hostName = {}", hostId, hostName);
            }
        }


        JsonNode containersNode = root.path("containers");
        if (containersNode != null && containersNode.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = containersNode.fields();

            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                String containerId = entry.getKey();             // container id
                JsonNode containerNode = entry.getValue();       // 그 안의 메트릭 정보

                String containerName = containerNode.path("name").asText(); // ex. "app1"

                // containerInventory에 있는지 없는지 판별
                // 1. (hostName, containerId, containerName) 조합이 있는지 확인
                if (!containerInventoryRepository.existsByHostNameAndContainerIdAndContainerName(
                        hostName, containerId, containerName)) {

                    // 2. (hostName, containerName) 조합이 있는지 확인
                    Optional<ContainerInventory> existingInventory =
                            containerInventoryRepository.findByHostNameAndContainerName(hostName, containerName);

                    if (existingInventory.isPresent()) {
                        // 3. 있으면 ThresholdService에 경보 로그 주고 + containerId 덮어씌우고 저장

                        // send 경보 로그
                        thresholdQueryService.storeContainerIdChanged(
                                containerId,
                                containerName,
                                timestamp
                        );

                        // containerId 덮어씌우는 작업
                        ContainerInventory containerInventory = existingInventory.get();
                        containerInventory.setContainerId(containerId);
                        containerInventoryRepository.save(containerInventory);
                    } else {
                        // 4. 없으면 새로 생성
                        Optional<TargetType> optionalType = targetTypeRepository.findByType("container");
                        TargetType type;
                        if (optionalType.isPresent()) {
                            type = optionalType.get();
                        } else {
                            type = TargetType.builder().type("container").build();
                            targetTypeRepository.save(type);
                        }

                        ContainerInventory containerInventory = new ContainerInventory();
                        containerInventory.setType(type);
                        containerInventory.setHostName(hostName);
                        containerInventory.setContainerId(containerId);
                        containerInventory.setContainerName(containerName);
                        containerInventoryRepository.save(containerInventory);
                    }
                }

            }
        }
    }

    public HostContainerInventoryList getHostContainerInventoryList() {
        HostContainerInventoryList result = new HostContainerInventoryList();

        // 1. 모든 HostMachineInventory 조회
        List<HostMachineInventory> hostEntities = hostMachineInventoryRepository.findAll();
        List<HostContainerInventoryList.HostDTO> hostDTOs = hostEntities.stream()
                .map(host -> {
                    HostContainerInventoryList.HostDTO dto = new HostContainerInventoryList.HostDTO();
                    dto.setId(host.getHostId());
                    dto.setName(host.getHostName());
                    return dto;
                })
                .collect(Collectors.toList());

        // 2. 모든 ContainerInventory 조회
        List<ContainerInventory> containerEntities = containerInventoryRepository.findAll();
        List<HostContainerInventoryList.ContainerDTO> containerDTOs = containerEntities.stream()
                .map(container -> {
                    HostContainerInventoryList.ContainerDTO dto = new HostContainerInventoryList.ContainerDTO();
                    dto.setHost(container.getHostName());
                    dto.setId(container.getContainerId());
                    dto.setName(container.getContainerName());
                    return dto;
                })
                .collect(Collectors.toList());

        // 3. DTO 세팅
        result.setHost(hostDTOs);
        result.setContainer(containerDTOs);

        return result;
    }


    // 머신 모든 숫자 조회
    public int retrieveAllMachineNumber() {
        int result = 0;
        result = hostMachineInventoryRepository.countAll() + containerInventoryRepository.countAll();

        return result;
    }

    // "type"을 입력한 타입의 머신 총 숫자 조회
    public int retrieveMachineNumberByType(String type) {
        int result = 0;

        if (type.equals("host")) {
            result = hostMachineInventoryRepository.countAll();
        }
        else if (type.equals("container")) {
            result = containerInventoryRepository.countAll();
        }
        else {
            logger.error("{} is not a valid machine type", type);
        }

        return result;
    }

    // json 파싱
    private JsonNode parseJson(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception e) {
            throw new InvalidJsonException("JSON 파싱 실패", e);
        }
    }

    // 사용자 정의 예외
    public static class InvalidJsonException extends RuntimeException {
        public InvalidJsonException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
