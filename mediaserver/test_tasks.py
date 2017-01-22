from pathlib import Path
import json
from itertools import chain

from ffmpy import FFmpeg, FFprobe

from tasks import GetFFprobeOutput, DecodeFFprobeOutput, FileInfo, VideoStream, AudioStream, SubtitleStream

def ffmpy_cmd_out(cmd):
    class WrappedFFmpyCmd:
        def __init__(self, *args, **kwargs):
            self.cmd = cmd(*args, **kwargs)

        def run(self):
            return self.cmd.cmd, None
    return WrappedFFmpyCmd

FFprobeCmdOut = ffmpy_cmd_out(FFprobe)
FFmpegCmdOut = ffmpy_cmd_out(FFmpeg)

def test_GetFFprobeOutput():
    assert "ffprobe -v error -print_format json -show_streams -i input.mkv" == GetFFprobeOutput(FFprobeCmdOut).execute(Path("input.mkv"))

def test_DecodeFFprobeOutput():
    filename = "input.mkv"
    tags = {
        "title": "title"
    }
    video_streams = [
        { "index": 0, "codec_type": "video", "codec_name": "h264", "width": 1920, "height": 1080 }
    ]
    audio_streams = [
        { "index": 1, "codec_type": "audio", "codec_name": "dts", "tags": { "language": "eng" } },
        { "index": 2, "codec_type": "audio", "codec_name": "vorbis", "tags": {} },
        { "index": 3, "codec_type": "audio", "codec_name": "opus" }
    ]
    subtitle_streams = [
        { "index": 4, "codec_type": "subtitle", "tags": { "language": "eng" } },
        { "index": 5, "codec_type": "subtitle", "tags": { "language": "fre" } },
        { "index": 6, "codec_type": "subtitle", "tags": { } },
        { "index": 7, "codec_type": "subtitle" }
    ]
    misc_streams = [
        { "codec_type": "misc" },
        {}
    ]
    fi = DecodeFFprobeOutput().execute(json.dumps({
        "format": {
            "filename": filename,
            "tags": tags
        },
        "streams": list(chain(video_streams, audio_streams, subtitle_streams, misc_streams)) 
    }))
    assert fi.file_format.filename == filename
    assert fi.file_format.tags == tags
    assert fi.streams.videos == [
        VideoStream(index=0, codec="h264", width=1920, height=1080)
    ]
    assert fi.streams.audios == [
        AudioStream(index=1, codec="dts", lang="eng"),
        AudioStream(index=2, codec="vorbis", lang=None),
        AudioStream(index=3, codec="opus", lang=None)
    ]
    assert fi.streams.subtitles == [
        SubtitleStream(index=4, lang="eng"),
        SubtitleStream(index=5, lang="fre"),
        SubtitleStream(index=6, lang=None),
        SubtitleStream(index=7, lang=None)
    ]
