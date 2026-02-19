package kr.cs.interdata.api_backend.service.repository_service;

import com.fasterxml.jackson.databind.ObjectMapper;
import kr.cs.interdata.api_backend.repository.ContainerInventoryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ContainerInventoryService {

    private final ContainerInventoryRepository containerInventoryRepository;
    private final Logger logger = LoggerFactory.getLogger(ContainerInventoryService.class);

    @Autowired
    public ContainerInventoryService(
                                   ContainerInventoryRepository containerInventoryRepository) {
        this.containerInventoryRepository = containerInventoryRepository;
    }

    /**
     *  - containerId와 containerName 조합을 기준으로 종속되어 있는 hostName을 반환한다.
     *
     * @param containerId       container ID
     * @param containerName     container name
     * @return  해당 id와 name조합이 종속되어 있는 hostName
     */
    public String addHostNameByContainerIdAndContainerName(String containerId, String containerName) {

        return String.valueOf(containerInventoryRepository.findHostNameByContainerIdAndContainerName(containerId, containerName));
    }

    /**
     * containerId를 이용해 컨테이너의 hostName을 조회하는 메서드.
     *
     * <p>
     * - 내부적으로 Repository에서 containerId로 hostName을 조회합니다.
     * - 조회 결과가 null일 경우 "unknown" 문자열을 반환하여, NullPointerException 등 예외를 방지합니다(null-safe).
     * - 실제 hostName 값이 존재하면 해당 값을 그대로 반환합니다.
     *
     * <b>주 사용 예:</b>
     * - 컨테이너 ID만으로 소속 hostName이 필요할 때 (예: 이상 로그 기록 시 자동 매핑)
     * - inventory DB에 데이터가 없거나 outdated된 경우에도 안전하게 "unknown" 반환.
     *
     * @param containerId  컨테이너의 고유 식별자(ID)
     * @return             해당 containerId의 hostName, 없으면 "unknown"
     */
    public String getHostNameByContainerId(String containerId) {
        String hostName = containerInventoryRepository.findHostNameByContainerId(containerId);
        return (hostName != null) ? hostName : "unknown"; // null-safe 처리
    }

    /**
     * containerId, containerName 정보를 이용해 hostName을 찾아 반환하는 메서드.
     *
     * <p>
     * 1. 우선 containerId와 containerName이 모두 있을 때, 두 값을 조합해 hostName을 조회<br>
     * 2. 1번에서 못 찾은 경우, containerName만으로 hostName을 한 번 더 조회<br>
     * 3. 두 방식 모두 실패하면 "unknown" 반환<br>
     *
     * <b>주요 용도:</b>
     * - 컨테이너의 hostName을 최대한 자동으로 매핑해 저장하고 싶을 때 사용
     * - inventory에 id/name 기반 데이터 누락 시에도 일부 보완 효과
     *
     * @param containerId    컨테이너의 고유 식별자 (containerId, null 가능)
     * @param containerName  컨테이너의 이름 (containerName, null 가능)
     * @return  hostName(성공 시), 찾지 못하면 "unknown" 문자열
     */
    public String fallbackHostName(String containerId, String containerName) {
        String hostName = null;
        if (containerId != null) {
            //containerID와 containerName 모두 있는 경우 두 값으로 조회
            hostName = containerInventoryRepository.findHostNameByContainerIdAndContainerName(containerId, containerName);
        }
        if (hostName == null && containerName != null) {
            //못 찾았고, containerName만이라도 있으면 이름만으로 한 번 더 조회
            hostName = containerInventoryRepository.findHostNameByContainerName(containerName);
        }
        //두 방법 모두 실패 시 unknown 반환
        return (hostName != null) ? hostName : "unknown";
    }




}
