# Audio processing (chunk merge, conversion)
from typing import List
from io import BytesIO
from pydub import AudioSegment  # pip install pydub
import wave
import tempfile

def merge_chunks(chunks: List[bytes]) -> bytes:
    if not chunks:
        return b""
    return b"".join(chunks)


def converted_audio_compatible(audio_bytes: bytes) -> bytes:
    buf = BytesIO(audio_bytes)
    try:
        audio = AudioSegment.from_file(buf) 
    except Exception as e:
        raise RuntimeError(f"Failed to parse audio bytes: {e}")

    audio = audio.set_frame_rate(16000).set_channels(1).set_sample_width(2)  # 16-bit

    out_buf = BytesIO()
    audio.export(out_buf, format="wav")
    return out_buf.getvalue()
