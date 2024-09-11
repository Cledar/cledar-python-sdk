# pylint: disable=too-many-instance-attributes
from typing import Dict, Sequence

import pydantic


@pydantic.dataclasses.dataclass
class ContentNetwork:
    id: str | None
    name: str


@pydantic.dataclasses.dataclass
class ContentDistributor:
    id: str | None
    name: str


@pydantic.dataclasses.dataclass
class ContentMetadata:
    id: str | None
    medium: str
    type: str


@pydantic.dataclasses.dataclass
class ContentStream:
    id: str
    type: str | None
    index: int | None
    codec_name: str | None


@pydantic.dataclasses.dataclass
class AudioStreamInfo(ContentStream):
    language: str | None
    format: str | None
    channels: int | None
    channels_layout: str | None
    sample_rate: int | None
    bit_rate: int | None
    frame_size: int | None
    delay: int | None


@pydantic.dataclasses.dataclass
class VideoStreamInfo(ContentStream):
    format: str | None
    width: int | None
    height: int | None
    display_aspect_ratio: str | None
    pixel_aspect_ratio: str | None
    pixel_format: str | None
    framerate: str | None
    delay: int | None


@pydantic.dataclasses.dataclass
class SubtitleStreamInfo(ContentStream):
    language: str | None
    delay: int | None


@pydantic.dataclasses.dataclass
class ContentOrigin:
    id: str
    name: str
    url: str
    mode: str
    target: str

    network: ContentNetwork
    distributor: ContentDistributor
    metadata: ContentMetadata


@pydantic.dataclasses.dataclass
class PipelineMessageReference:
    id: str
    created_at: str
    created_by: str


@pydantic.dataclasses.dataclass
class PipelineStageReference(PipelineMessageReference):
    topic_name: str
    inputs: Sequence[Dict | PipelineMessageReference]


@pydantic.dataclasses.dataclass
class PipelineMessageMetadata:
    origin: ContentOrigin
    streams: list[VideoStreamInfo | AudioStreamInfo | SubtitleStreamInfo]
    pipeline_stages: list[PipelineStageReference]


@pydantic.dataclasses.dataclass
class PipelineMessage(PipelineMessageReference):
    metadata: PipelineMessageMetadata
