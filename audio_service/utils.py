import tempfile
from tempfile import _TemporaryFileWrapper


def load_audio_as_file(file_format: str, media: bytes) -> _TemporaryFileWrapper:
    if file_format not in ["mp3", "wav", "flac"]:
        file_format = "wav"

    with tempfile.NamedTemporaryFile(suffix=f".{file_format}", delete=False) as tmp:
        tmp.write(media)
        tmp.flush()
        tmp.seek(0)

        return tmp
