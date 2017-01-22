from pathlib import Path
import json
from itertools import chain

from ffmpy import FFmpeg, FFprobe

from tasks import *

def ffmpy_cmd_out(cmd): 
    calls = []
    class WrappedFFmpyCmd:
        def __init__(self, *args, **kwargs):
            self.cmd = cmd(*args, **kwargs)

        def run(self):
            calls.append(self.cmd.cmd)
            return None, None
    return WrappedFFmpyCmd, calls

def test_GetFFprobeOutput():
    wrapper, calls = ffmpy_cmd_out(FFprobe)
    GetFFprobeOutput(wrapper).execute(Path('input.mkv'))
    assert calls == ['ffprobe -v error -print_format json -show_streams -i input.mkv']

def test_DecodeFFprobeOutput():
    filename = 'input.mkv'
    tags = {
        'title': 'title'
    }
    video_streams = [
        { 'index': 0, 'codec_type': 'video', 'codec_name': 'h264', 'width': 1920, 'height': 1080 }
    ]
    audio_streams = [
        { 'index': 1, 'codec_type': 'audio', 'codec_name': 'dts', 'tags': { 'language': 'eng' } },
        { 'index': 2, 'codec_type': 'audio', 'codec_name': 'vorbis', 'tags': {} },
        { 'index': 3, 'codec_type': 'audio', 'codec_name': 'opus' }
    ]
    subtitle_streams = [
        { 'index': 4, 'codec_type': 'subtitle', 'tags': { 'language': 'eng' } },
        { 'index': 5, 'codec_type': 'subtitle', 'tags': { 'language': 'fre' } },
        { 'index': 6, 'codec_type': 'subtitle', 'tags': { } },
        { 'index': 7, 'codec_type': 'subtitle' }
    ]
    misc_streams = [
        { 'codec_type': 'misc' },
        {}
    ]
    fi = DecodeFFprobeOutput().execute(json.dumps({
        'format': {
            'filename': filename,
            'tags': tags
        },
        'streams': list(chain(video_streams, audio_streams, subtitle_streams, misc_streams)) 
    }))
    assert fi.file_format.filename == filename
    assert fi.file_format.tags == tags
    assert fi.streams.videos == [
        VideoStream(index=0, codec='h264', width=1920, height=1080)
    ]
    assert fi.streams.audios == [
        AudioStream(index=1, codec='dts', lang='eng'),
        AudioStream(index=2, codec='vorbis', lang=None),
        AudioStream(index=3, codec='opus', lang=None)
    ]
    assert fi.streams.subtitles == [
        SubtitleStream(index=4, lang='eng'),
        SubtitleStream(index=5, lang='fre'),
        SubtitleStream(index=6, lang=None),
        SubtitleStream(index=7, lang=None)
    ]

def test_EncodeSubtitleStream():
    wrapper, calls = ffmpy_cmd_out(FFmpeg)
    assert Path('subtitle-eng.vtt') == EncodeSubtitleStream(wrapper).execute(Path('input.mkv'), SubtitleStream(index=0, lang='eng'))
    assert calls == ['ffmpeg -i input.mkv -map 0:0 subtitle-eng.vtt']

def test_EncodeAudioStream():
    wrapper, calls = ffmpy_cmd_out(FFmpeg)
    assert Path('audio-eng.webm') == EncodeAudioStream(wrapper).execute(Path('input.mkv'), AudioStream(index=0, codec='dts', lang='eng'))
    assert calls == ['ffmpeg -i input.mkv -map 0:0 -c:a libvorbis -b:a 160k -f webm -dash 1 audio-eng.webm']

def test_EncodeVideoStream():
    wrapper, calls = ffmpy_cmd_out(FFmpeg)
    assert Path('video-1080p.webm') == EncodeVideoStream(1080, wrapper).execute(Path('input.mkv'), VideoStream(index=0, codec='h264', width=1920, height=1080))
    assert calls == [
        'ffmpeg -i input.mkv -map 0:0 -c:v libvpx-vp9 -s 1920x1080 -b:v 6M -tile-columns 6 -frame-parallel 1 -keyint_min 150 -g 150 -qmin 0 -qmax 50 -f webm -dash 1 -pass 1 -speed 4 /dev/null',
        'ffmpeg -i input.mkv -map 0:0 -c:v libvpx-vp9 -s 1920x1080 -b:v 6M -tile-columns 6 -frame-parallel 1 -keyint_min 150 -g 150 -qmin 0 -qmax 50 -f webm -dash 1 -pass 2 -speed 1 -auto-alt-ref 1 -lag-in-frames 25 video-1080p.webm'
    ]
