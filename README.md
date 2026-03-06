# Infra Monitoring System  
**Kafka 기반 이벤트 파이프라인을 이용해**  
**온프레미스 환경의 200대 Host/Container 메트릭을**  
**2초 주기로 수집·분석하고 실시간 알림을 제공하는 모니터링 시스템**  

- **기간:** 2025.03 ~ 2025.08  
- **팀 구성:** 백엔드 2명, 프론트엔드 1명  
- **운영 규모:** 약 200대 Host/Container  
- **수집 주기:** 2초 단위 메트릭 수집  

---

## 1. 문제 정의

 온프레미스 서버실 환경에서 다수의 Host 및 Container 메트릭을 **초단위로 지속 수집해야 하는 모니터링 시스템**이 필요했습니다.  

 기존 HTTP 기반 모니터링 도구는 운영 환경 제약으로 사용할 수 없었고, 초단위 메트릭이 지속적으로 발생하는 구조에서 **API 요청 기반 처리 방식은 병목 위험이 있다고 판단했습니다.**  

 또한 다음과 같은 운영상의 문제가 존재했습니다.

- 장애 발생 시 문제 Host/Container 즉시 식별하기 어려움
- 임계치 초과를 실시간으로 감지하는 체계 부재
- 수집 계층과 처리 계층이 분리되지 않아 확장성 및 안정성 문제 존재

이 문제를 해결하기 위해 **Kafka 중심의 이벤트 기반 데이터 파이프라인**을 설계하고, 자체적인 실시간 메트릭 처리 및 알림 기능을 갖춘 모니터링 시스템을 구축했습니다.

---

## 2. 역할

프로젝트에서 **백엔드 설계 및 데이터 처리 구조 구현을 담당**했습니다.

- **Kafka 기반 메트릭 데이터 처리 파이프라인 구현**
- Kafka Consumer 구현 및 메트릭 데이터 모델 설계
- **임계치 분석 로직 및 실시간 알림 시스템 구현**
- **SSE(Server-Sent Events)** 기반 실시간 이벤트 스트림 구현
- Docker-compose 기반 **배포 구조 표준화**

---

## 3. 핵심 설계

<img width="4812" height="2993" alt="Image" src="https://github.com/user-attachments/assets/406bbcf3-dce3-481b-a66e-2fcff3eaf2f5" />

메트릭 수집 계층, 메시지 브로커(Kafka), 데이터 처리 계층, 실시간 스트림 계층으로 구성된 전체 시스템 아키텍처

### 3-1. Kafka 기반 계층 분리 구조 설계

 초단위 메트릭 데이터가 지속적으로 발생하는 환경에서  
**수집 계층과 처리 계층을 분리하지 않을 경우**  
**데이터 처리 병목 및 장애 영향 범위가 확대될 수 있다고 판단했습니다.**

 당시 **사내 인프라에 기존 Kafka 클러스터가 운영 중**이었고,
이를 활용해 수집 계층과 처리 계층을 분리하는
이벤트 기반 데이터 파이프라인 구조를 적용하는 방안이 제안되었습니다.

 이에 해당 Kafka 클러스터를 메시지 브로커로 활용하여  
메트릭 수집과 데이터 처리 흐름을 비동기 이벤트 기반으로 구성했습니다.

```
[ Collector ]
      │
      ▼
[ Kafka ]
      │
      ▼
[ API Backend ] ──▶ [ MySQL ]
      │
      ▼
[ SSE Stream ] ──▶ [ Client ]
```
이 구조를 통해 다음을 달성했습니다.

- 수집/처리 계층 완전 분리
- 비동기 이벤트 기반 처리 구조 도입
- 병목 및 장애 영향 범위 최소화
- 초단위 데이터 처리 안정성 확보

---

### 3-2. 임계치 정책 및 실시간 알림 구조
  
 운영 환경에서 장애 발생 시 **문제 지점을 즉시 식별할 수 있도록 임계치 기반 이벤트 감지 구조**를 구현했습니다.  

구현 내용
- 서비스 계층에 **임계치 판단 로직 캡슐화**
- 사용자 지정 임계치 정책 지원
- 메트릭 **급변 감지 로직 구현**
- SSE 기반 **실시간 알림 이벤트 전송**

이를 통해 **Host/Container 상태 변화를 실시간으로 확인할 수 있는 모니터링 체계**를 구축했습니다.

---

### 3-3. 실시간 메트릭 스트림 병목 문제 해결

 프론트엔드와 연동하여 실시간 그래프 시각화를 적용하는 과정에서 **메트릭 데이터 전송 병목 문제**가 발생했습니다.

초기 구조에서는
  
- Host 메트릭
- Container 메트릭

을 **각각 별도의 이벤트로 전송**하고 있었고, 이로 인해 **websocket 이벤트 수가 급증하면서 클라이언트 그래프 렌더링 병목이 발생**했습니다.

이를 해결하기 위해

- **Host 메트릭 + 해당 Host의 Container 메트릭을 하나의 JSON 데이터로 병합**
- 이벤트 수를 줄여 **실시간 스트림 처리 부담 감소**
- 사용자 UI에서 **실시간 그래프 렌더링 가능한 데이터 범위 재설계**

결과적으로

- **전송 이벤트 수 감소**
- UI 그래프 렌더링 병목 완화
- 사용자 경험을 유지하면서 실시간 모니터링 안정성 확보

---

### 3-4. 배포 환경 표준화

기존
- 모듈별 `.env` 관리
- Docker에 서버를 올림에도 수동으로 명령 실행
- 전체 배포 **15~20분 소요**

개선
- **docker-compose 도입**
- 단일 `.env` 기반 환경 주입
- 서비스 의존성 명시 및 기동 순서 정의

결과
- 전체 배포 시간 약 **15~20분 → 5분 내외**
- 환경 설정 누락으로 인한 실행 오류 감소
- **실운영자인 비개발자도 단일 명령어로 시스템 기동 가능**

---

## 4. 운영 결과

- 약 **200대 Host/Container 환경에서 2초 주기 메트릭 안정 처리**
- Kafka 기반 비동기 구조로 **수집/처리 계층 분리**
- 임계치 초과 이벤트 **실시간 감지 및 SSE 전송**
- 실시간 모니터링으로 메트릭 급변 감지 체계 구축
- 배포 시간 **3배 이상 단축**

---

## 5. 배운 점

- 이벤트 기반 아키텍처에서의 **책임 분리 설계 중요성**
- 메시징 시스템 중심 구조의 **확장성 및 장애 격리 특성 이해**
- 실시간 데이터 처리 환경에서의 **스트림 병목 문제 해결 경험**
- 운영 환경을 고려한 **배포 표준화 및 시스템 재현성 확보의 중요성**

---
**[ 🔗 링크 ]**
* **팀 프로젝트 레포지토리**  
`https://github.com/knu-capstone-design-2/server-monitoring`

---

**[ 💡 참고 사항 ]**
* **[ 아래 내용은 이 프로그램의 상세 소개 및 실행 방법입니다. 참고만 해주시면 감사하겠습니다. ]**

---
  
## 목차

- [server-monitoring 소개](#server-monitoring-소개)
- [docker-compose 각 파일 설명 및 사용 방법](#docker-compose-각-파일-설명-및-사용-방법)
- [실행 전 준비사항](#실행-전-준비사항)
- [실행 방법](#실행-방법)
- [환경설정](#환경설정)
- [server-monitoring 주요 흐름도](#server-monitoring-주요-흐름도)
- [server-monitoring 주요 특징](#server-monitoring-주요-특징)
- [구성 예시](#구성-예시)
- [metrics-backend 모듈 설명](#metrics-backend-모듈-설명)
  - [모듈 구성](#모듈-구성)
- [api-backend 모듈 설명](#api-backend-모듈-설명)
  - [api-backend 주요 기능 및 구조](#api-backend-주요-기능-및-구조)
- [문의 및 기여](#문의-및-기여)
  
## server-monitoring 소개

`server-monitoring`은 실제 서버실의 서버 컴퓨터에 설치하여,  
**호스트 머신과 해당 서버에 속한 모든 컨테이너의 메트릭(자원 사용량 등)을 실시간으로 수집하고 모니터링할 수 있도록 설계된 시스템**입니다.

Docker 환경에서 collector를 실행하면 서버 컴퓨터의 호스트 및 컨테이너 자원 데이터를 자동으로 수집하여 Kafka 클러스터로 전송합니다.  
중개 컴퓨터에서는 Docker에서 실행된 consumer가 Kafka에서 이 데이터를 받아 backend로 전송합니다. 
메인 컴퓨터에서는 Docker에서 실행된 backend가 consumer에서 받아온 데이터를 사용해 실시간 메트릭 데이터를 전송하거나 각 머신별 임계치 초과 여부를 계산하고,  
임계치 초과 시 실시간 알림을 전송하여 운영자가 신속하게 서버 상태를 모니터링하고 대응할 수 있도록 지원합니다.

---

## docker-compose 각 파일 설명 및 **사용 방법**

- docker-compose.collector.yml  
  **메트릭을 수집하려는 컴퓨터들의 도커에 설치해 실행시킵니다.**
  - metrics-backend/data-collector (의존성 존재:metrics-backend/producer)

- docker-compose.consumer.yml  
  **kafka cluster에서 메트릭을 받아오는 컴퓨터의 도커에 설치해 실행시킵니다.**
  - metrics-backend/consumer

- docker-compose.backend.yml  
  **consumer로 받아온 메트릭을 처리하는 컴퓨터의 도커에 설치해 실행시킵니다.**
  - api-backend
  - MySQL 데이터베이스

---

## 실행 전 준비사항

- **Docker 설치**  
  이 프로젝트는 Docker 환경에서 동작하므로, 먼저 Docker가 설치되어 있어야 합니다.  
  👉 [Docker 설치 가이드](https://docs.docker.com/get-docker/)



---


## 실행 방법


#### 1. 환경설정
- 1-1. 폴더 최상위 루트에 .env파일을 만들어 환경설정을 해준다.
  - 환경설정은 아래의 **환경설정** 부분을 참고하세요!

---
#### 2. 해당 컴퓨터에 시스템을 올리기 전 이미지 파일 생성 단계
-**2-1. ~ 2-4.의 단계는 해당 프로젝트 폴더의 최상위 루트에서 실행시킵니다.**

- 2-1. 이미지 : isslab/im-api-backend 생성
```bash
docker build -t isslab/im-api-backend:latest -f api-backend/Dockerfile .
```

- 2-2. 이미지 : isslab/im-metrics-consumer 생성
```bash
docker build -t isslab/im-metrics-consumer:latest -f metrics-backend/consumer/Dockerfile .
```

- 2-3. 이미지 : isslab/im-data-collector 생성
```bash
docker build -t isslab/im-data-collector:latest -f metrics-backend/data-collector/Dockerfile .

```

---
#### 3. 각 해당 컴퓨터에서 각 docker-compose 실행


- 3-1. 백엔드 + DB 실행
```bash
docker-compose -f docker-compose.backend.yml up -d
```

- 3-2. consumer 실행
```bash
docker-compose -f docker-compose.consumer.yml up -d
```

- 3-3. collector 측 실행 (각 장비 or 서버컴 등에서)
```bash
docker-compose -f docker-compose.collector.yml up -d
```


- **순서대로 실행함을 !강력히! 권장합니다.**
- **테스트를 위해 하나의 컴퓨터에 `docker-compose.collector.yml`, `docker-compose.consumer.yml`, `docker-compose.backend.yml`를 함께 실행시키는 것도 가능합니다.**


---

## 환경설정
- 💡 최상위 경로에 .env 파일이 없다면 **반드시** 새로 생성합니다.
```bash
TZ=Asia/Seoul   # 변경 가능
DATABASE_ROOT_PASSWORD=<Root-Password>(임의 설정)
DATABASE_USERNAME=<Username>(임의 설정)
DATABASE_PASSWORD=<Password>(임의 설정)
CORS_ALLOWED_ORIGINS=<주소1>,<주소2>,... [CORS 허용 Origin 목록(콤마로 구분)]
BOOTSTRAP_SERVER=[kafka 클러스터 ip주소:외부포트번호]
KAFKA_TOPIC_NAME=[kafka topic name]
KAFKA_CONSUMER_GROUP_ID=[kafka consumer group id]
API_BASE_URL=http://api-backend:8004    # 필수
```

---

## server-monitoring 주요 특징

- **서버실의 각 서버 컴퓨터에 collector를 Docker로 설치**  
  호스트 및 모든 컨테이너의 메트릭(자원 사용량 등)을 자동 수집

- **Kafka 클러스터 연동**  
  수집된 데이터는 Kafka 클러스터를 통해 메인 서버로 전송

- **임계치(Threshold) 모니터링 및 알림**  
  메인 서버의 backend가 Kafka에서 데이터를 받아  
  각 서버/컨테이너별 임계치 초과 여부를 계산  
  임계치 초과 시 실시간 알림 제공

- **운영 편의성**  
  운영자는 전체 서버실의 자원 현황과 이상 상황을 한눈에 모니터링 가능

---

## 구성 예시

- **각 서버 컴퓨터**
  - collector (`docker-compose.collector.yml` 참고) 실행
  - host와 container의 메트릭을 수집해 Kafka로 메트릭 데이터 전송

- **중개 컴퓨터(kafka cluster에 연결)**
  - consumer (`docker-compose.consumer.yml` 참고) 실행
  - kafka에서 데이터 수신 및 backend로 데이터 단방향 전송

- **메인 컴퓨터**
  - backend (`docker-compose.backend.yml` 참고) 실행
  - 데이터 수신 및 메트릭 데이터 모니터링, 임계치 모니터링/알림

---

이 시스템을 통해 실제 서버실의 다양한 서버와 컨테이너의 자원 사용 현황을  
중앙에서 실시간으로 모니터링하고, 임계치 초과 등 이상 상황에 즉시 대응할 수 있습니다.

---

# metrics-backend 모듈 설명

`metrics-backend`는 컨테이너 및 호스트 머신에서 메트릭 데이터를 수집하고, Kafka를 통해 전송 및 처리하는 백엔드 시스템입니다. 이 프로젝트는 메트릭 수집 → Kafka 전송 → 데이터 수집 및 전송의 흐름을 중심으로 구성되며, Docker 환경에서 실행됩니다.

## 모듈 구성

- **data-collector**  
  컨테이너 머신 및 호스트 머신의 자원 사용 데이터를 수집합니다.
  > - [data-collector README](./metrics-backend/data-collector/README.md)

- **producer**  
  수집된 데이터를 Kafka로 전송하는 Kafka 프로듀서 역할을 합니다.
  > - [producer README](./metrics-backend/producer/README.md)

- **consumer**  
  Kafka로부터 메트릭 데이터를 수신하며, 내부적으로 WebClient를 활용해 데이터를 backend에 전송하는 기능을 수행합니다.
  > - [consumer README](./metrics-backend/consumer/README.md)


---

# api-backend 모듈 설명

`api-backend`는 클라이언트(프론트엔드)와 다른 백엔드 서버(metrics-backend)로부터 API 요청을 받아 처리하며, 데이터베이스와 관련된 모든 작업을 담당하는 Java Spring 기반 서버입니다.  
➡ [자세한 README 보기](./api-backend/README.md)

이 시스템은 데이터 저장·조회, 임계치 관리, 실시간 데이터 알림 등 다양한 API 기능을 제공하며, 클라이언트와 백엔드 간의 데이터 흐름을 중계하는 핵심 브릿지 역할을 수행합니다.

주요 기능은 다음과 같습니다:

- DB 초기 데이터 입력, API를 통한 데이터 저장 등 데이터베이스 관련 모든 작업 처리
- 머신(Host, Container) 정보 및 임계치(Threshold) 초과 데이터 관리
- SSE(Server-Sent Events) 방식으로 임계치 초과 데이터를 클라이언트에 실시간 전달
- 날짜별 임계치 초과 데이터 조회, 임계치 설정/조회 등 다양한 API 제공
- 클라이언트와 백엔드 서버 간 데이터 흐름을 중계하는 브릿지 역할



## api-backend 주요 기능 및 구조

- **데이터 관리**  
  DB 초기화, 데이터 저장, 조회, 수정 등 데이터베이스 관련 모든 API 처리

- **임계치(Threshold) 관리**  
  머신별 임계치 설정/조회, 임계치 초과 데이터 실시간 알림(SSE) 제공

- **실시간 알림**  
  임계치 초과 발생 시 SSE를 통해 클라이언트에 알림 전송

- **클라이언트-백엔드 브릿지**  
  프론트엔드와 metrics-backend 사이의 데이터 흐름을 관리하는 핵심 API 게이트웨이 역할 수행


---

## 문의 및 기여

- 이 프로젝트에 대한 문의, 개선 제안, 버그 제보는 이슈 또는 PR로 남겨주세요.

