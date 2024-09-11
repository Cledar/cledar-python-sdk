from typing import Sequence
import pytest
from faker import Faker
import pydantic

from kafka_service.input_parser import (
    IncorrectMessageValue,
    InputParser,
)
from kafka_service.models.pipeline import (
    AudioStreamInfo,
    ContentDistributor,
    ContentMetadata,
    ContentNetwork,
    ContentOrigin,
    PipelineMessage,
    PipelineMessageMetadata,
    PipelineMessageReference,
    PipelineStageReference,
    SubtitleStreamInfo,
    VideoStreamInfo,
)
from kafka_service.schemas import KafkaMessage


@pydantic.dataclasses.dataclass
class S3Metadata:
    url: str
    type: str | None
    container: str | None


class InputMessagePayload(pydantic.BaseModel, PipelineMessage):
    id: str
    chunks: Sequence[S3Metadata]


@pydantic.dataclasses.dataclass
class InputMessageReference(PipelineMessageReference):
    id: str
    chunks: Sequence[S3Metadata]


fake = Faker()

# pylint: disable=line-too-long
input_json_value = '{"id":"78c7392659984c2d89057bd628275855","created_at":"2024-08-30T11:09:56Z","created_by":"chunk-transformer","metadata":{"origin":{"id":"dbd5f857572e49bfb8dd678dd14a4b85","name":"TVP_1","url":"https://example.com","mode":"broadcast","target":"television","network":{"id":null,"name":"korbank"},"distributor":{"id":null,"name":"TVP_1"},"metadata":{"id":null,"medium":"video","type":"channel"}},"streams":[{"id":"2cad60eac39f4cb4a676eb001350c3c3","type":"video","index":0,"codec_name":"h264","format":"yuv420p","width":1920,"height":1080,"display_aspect_ratio":"16:9","pixel_aspect_ratio":"1:1","pixel_format":"yuv420p","framerate":"25:1","delay":0},{"id":"9a8c7216c1e24f9eb2152c52ac3feb50","type":"audio","index":1,"codec_name":"mp2","language":"pol","format":"s16p","channels":2,"channels_layout":"stereo","sample_rate":48000,"bit_rate":192000,"frame_size":1152,"delay":0},{"id":"50bc1f29f4b84b28a92aa263567dd9ca","type":"audio","index":2,"codec_name":"ac3","language":"qaa","format":"fltp","channels":6,"channels_layout":"5.1(side)","sample_rate":48000,"bit_rate":384000,"frame_size":0,"delay":0},{"id":"eb2f0753258b4d0fbf77af82ac7cff59","type":"subtitle","index":4,"codec_name":"dvbsub","language":"pol","delay":0},{"id":"dc57b3f8fdb04420bc820fe1d45985b6","type":"audio","index":6,"codec_name":"mp2","language":"aux","format":"s16p","channels":2,"channels_layout":"stereo","sample_rate":48000,"bit_rate":128000,"frame_size":1152,"delay":0}],"pipeline_stages":[{"id":"c577dd6d2be743d5a155ef75d6d1ec79","created_at":"2024-08-30T11:09:55Z","created_by":"stream-chunker","topic_name":"reference-chunks","inputs":[]},{"id":"78c7392659984c2d89057bd628275855","created_at":"2024-08-30T11:09:56Z","created_by":"chunk-transformer","topic_name":"reference-transformed-chunks","inputs":[{"id":"f11a8990bfff4288b9b284cb9908c788","created_at":"2024-08-30T11:09:55Z","created_by":"stream-chunker","s3_key":"0285ee22c95f43f9a98792d210aa6813.mpegts","s3_bucket":"reference-chunks","chunk_duration":5,"chunk_overlap_sec":0,"chunk_size":5179024,"ignored_streams":[3,5,7]}]}]},"chunks":[{"url":"s3://reference-transformed-chunks/video/0285ee22c95f43f9a98792d210aa6813_stream_0.mp4","type":"video","container":"mpegts"},{"url":"s3://reference-transformed-chunks/audio/0285ee22c95f43f9a98792d210aa6813_stream_1.mp4","type":"audio","container":"mpegts"},{"url":"s3://reference-transformed-chunks/audio/0285ee22c95f43f9a98792d210aa6813_stream_2.mp4","type":"audio","container":"mpegts"},{"url":"s3://reference-transformed-chunks/audio/0285ee22c95f43f9a98792d210aa6813_stream_6.mp4","type":"audio","container":"mpegts"}]}'

# pylint: disable=unexpected-keyword-arg
expected = InputMessagePayload(  # type: ignore
    id="78c7392659984c2d89057bd628275855",
    created_at="2024-08-30T11:09:56Z",
    created_by="chunk-transformer",
    chunks=[
        S3Metadata(
            url="s3://reference-transformed-chunks/video/0285ee22c95f43f9a98792d210aa6813_stream_0.mp4",
            type="video",
            container="mpegts",
        ),
        S3Metadata(
            url="s3://reference-transformed-chunks/audio/0285ee22c95f43f9a98792d210aa6813_stream_1.mp4",
            type="audio",
            container="mpegts",
        ),
        S3Metadata(
            url="s3://reference-transformed-chunks/audio/0285ee22c95f43f9a98792d210aa6813_stream_2.mp4",
            type="audio",
            container="mpegts",
        ),
        S3Metadata(
            url="s3://reference-transformed-chunks/audio/0285ee22c95f43f9a98792d210aa6813_stream_6.mp4",
            type="audio",
            container="mpegts",
        ),
    ],
    metadata=PipelineMessageMetadata(
        origin=ContentOrigin(
            id="dbd5f857572e49bfb8dd678dd14a4b85",
            name="TVP_1",
            url="https://example.com",
            mode="broadcast",
            target="television",
            network=ContentNetwork(id=None, name="korbank"),
            distributor=ContentDistributor(id=None, name="TVP_1"),
            metadata=ContentMetadata(id=None, medium="video", type="channel"),
        ),
        streams=[
            VideoStreamInfo(
                id="2cad60eac39f4cb4a676eb001350c3c3",
                type="video",
                index=0,
                codec_name="h264",
                format="yuv420p",
                width=1920,
                height=1080,
                display_aspect_ratio="16:9",
                pixel_aspect_ratio="1:1",
                pixel_format="yuv420p",
                framerate="25:1",
                delay=0,
            ),
            AudioStreamInfo(
                id="9a8c7216c1e24f9eb2152c52ac3feb50",
                type="audio",
                index=1,
                codec_name="mp2",
                language="pol",
                format="s16p",
                channels=2,
                channels_layout="stereo",
                sample_rate=48000,
                bit_rate=192000,
                frame_size=1152,
                delay=0,
            ),
            AudioStreamInfo(
                id="50bc1f29f4b84b28a92aa263567dd9ca",
                type="audio",
                index=2,
                codec_name="ac3",
                language="qaa",
                format="fltp",
                channels=6,
                channels_layout="5.1(side)",
                sample_rate=48000,
                bit_rate=384000,
                frame_size=0,
                delay=0,
            ),
            SubtitleStreamInfo(
                id="eb2f0753258b4d0fbf77af82ac7cff59",
                type="subtitle",
                index=4,
                codec_name="dvbsub",
                language="pol",
                delay=0,
            ),
            AudioStreamInfo(
                id="dc57b3f8fdb04420bc820fe1d45985b6",
                type="audio",
                index=6,
                codec_name="mp2",
                language="aux",
                format="s16p",
                channels=2,
                channels_layout="stereo",
                sample_rate=48000,
                bit_rate=128000,
                frame_size=1152,
                delay=0,
            ),
        ],
        pipeline_stages=[
            PipelineStageReference(
                id="c577dd6d2be743d5a155ef75d6d1ec79",
                created_at="2024-08-30T11:09:55Z",
                created_by="stream-chunker",
                topic_name="reference-chunks",
                inputs=[],
            ),
            PipelineStageReference(
                id="78c7392659984c2d89057bd628275855",
                created_at="2024-08-30T11:09:56Z",
                created_by="chunk-transformer",
                topic_name="reference-transformed-chunks",
                inputs=[
                    {
                        "id": "f11a8990bfff4288b9b284cb9908c788",
                        "created_at": "2024-08-30T11:09:55Z",
                        "created_by": "stream-chunker",
                        "s3_key": "0285ee22c95f43f9a98792d210aa6813.mpegts",
                        "s3_bucket": "reference-chunks",
                        "chunk_duration": 5,
                        "chunk_overlap_sec": 0,
                        "chunk_size": 5179024,
                        "ignored_streams": [3, 5, 7],
                    }
                ],
            ),
        ],
    ),
)


def test_parse_1():
    parser = InputParser(InputMessagePayload)
    obj = parser.parse_json(input_json_value)

    assert obj.chunks == expected.chunks
    assert obj.id == expected.id
    assert obj.created_at == expected.created_at
    assert obj.created_by == expected.created_by
    assert obj.metadata.origin == expected.metadata.origin
    assert obj.metadata.pipeline_stages == expected.metadata.pipeline_stages
    assert obj.metadata.streams == expected.metadata.streams
    assert obj == expected
    assert obj.model_dump_json() == input_json_value


def test_parse_mesage_raise():
    parser = InputParser(InputMessagePayload)

    with pytest.raises(IncorrectMessageValue):
        parser.parse_message(
            KafkaMessage(
                value=None,
                key=fake.text(),
                topic=fake.text(),
            )
        )
