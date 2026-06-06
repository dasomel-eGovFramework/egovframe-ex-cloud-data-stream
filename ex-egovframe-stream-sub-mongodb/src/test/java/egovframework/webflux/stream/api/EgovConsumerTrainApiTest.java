package egovframework.webflux.stream.api;

import java.util.List;
import java.util.function.Consumer;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import egovframework.webflux.repository.RealtimePositionRepository;
import egovframework.webflux.stream.api.dto.RealtimePositionDTO;
import egovframework.webflux.stream.api.entity.RealtimePosition;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class EgovConsumerTrainApiTest {

    @Mock
    private RealtimePositionRepository realtimePositionRepository;

    private final EgovConsumerTrainApi config = new EgovConsumerTrainApi();

    private RealtimePositionDTO buildDto(String subwayId, String subwayNm) {
        RealtimePositionDTO dto = new RealtimePositionDTO();
        dto.setSubwayId(subwayId);
        dto.setSubwayNm(subwayNm);
        dto.setTrainNo("0060");
        return dto;
    }

    @Test
    @DisplayName("trainApi Consumer - 단건 메시지를 수신하여 MongoDB에 저장한다")
    void trainApi_savesReceivedPosition() {
        given(realtimePositionRepository.save(any(RealtimePosition.class)))
            .willReturn(Mono.just(new RealtimePosition()));

        Consumer<List<RealtimePositionDTO>> consumer = config.trainApi(realtimePositionRepository);

        RealtimePositionDTO dto = buildDto("1001", "1호선");

        // Consumer는 void 반환이므로 직접 호출하여 mock 상호작용을 검증한다
        consumer.accept(List.of(dto));

        verify(realtimePositionRepository, times(1)).save(any(RealtimePosition.class));
    }

    @Test
    @DisplayName("trainApi Consumer - 복수 메시지를 수신하면 각각 저장한다")
    void trainApi_savesMultiplePositions() {
        given(realtimePositionRepository.save(any(RealtimePosition.class)))
            .willReturn(Mono.just(new RealtimePosition()));

        Consumer<List<RealtimePositionDTO>> consumer = config.trainApi(realtimePositionRepository);

        List<RealtimePositionDTO> payload = List.of(
            buildDto("1001", "1호선"),
            buildDto("1002", "2호선"),
            buildDto("1003", "3호선")
        );

        consumer.accept(payload);

        verify(realtimePositionRepository, times(3)).save(any(RealtimePosition.class));
    }

    @Test
    @DisplayName("trainApi Consumer - 빈 리스트를 수신하면 저장을 호출하지 않는다")
    void trainApi_emptyPayload_doesNotSave() {
        Consumer<List<RealtimePositionDTO>> consumer = config.trainApi(realtimePositionRepository);

        consumer.accept(List.of());

        verify(realtimePositionRepository, times(0)).save(any(RealtimePosition.class));
    }

    @Test
    @DisplayName("save() - RealtimePositionRepository가 저장 후 Mono를 반환한다")
    void repository_save_returnsMono() {
        RealtimePosition position = new RealtimePosition();
        position.setSubwayId("1001");
        position.setSubwayNm("1호선");

        given(realtimePositionRepository.save(position)).willReturn(Mono.just(position));

        StepVerifier.create(realtimePositionRepository.save(position))
            .expectNextMatches(p -> "1001".equals(p.getSubwayId()))
            .verifyComplete();
    }
}
