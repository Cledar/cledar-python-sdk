import tempfile
from tempfile import _TemporaryFileWrapper


def load_audio_as_file(
    file_format: str,
    media: bytes,
) -> _TemporaryFileWrapper[bytes]:
    """
    Write raw audio bytes into a NamedTemporaryFile and return the open handle.

    :param file_format: expected "mp3", "wav" or "flac" (defaults to "wav")
    :param media: raw audio payload
    :return: a temporary file wrapper holding the audio
    """
    fmt = file_format.lower()
    if fmt not in ("mp3", "wav", "flac"):
        fmt = "wav"

    tmp: _TemporaryFileWrapper[bytes] = tempfile.NamedTemporaryFile(
        suffix=f".{fmt}",
        delete=False,
    )
    tmp.write(media)
    tmp.flush()
    tmp.seek(0)
    return tmp
