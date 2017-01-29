from os import path
import json
from itertools import chain
from unittest.mock import patch

from tasks import *

def test_get_ffprobe_output():
    file_path = 'input.mkv'
    with patch('ffmpy.FFprobe.run', autospec=True) as mock:
        get_ffprobe_output(file_path)
        assert mock.call_count == 1
        call = mock.call_args[0][0].cmd
        assert file_path in call

def test_decode_ffprobe_output():
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
    fi = decode_ffprobe_output({
        'format': {
            'filename': filename,
            'tags': tags
        },
        'streams': list(chain(video_streams, audio_streams, subtitle_streams, misc_streams)) 
    })
    assert fi.file_format.filename == filename
    assert fi.file_format.tags == tags
    assert fi.streams.videos == [
        VideoStream(index=0, codec='h264', width=1920, height=1080)
    ]
    assert fi.streams.audios == [
        AudioStream(index=1, codec='dts', lang='eng'),
        AudioStream(index=2, codec='vorbis', lang='nolang'),
        AudioStream(index=3, codec='opus', lang='nolang')
    ]
    assert fi.streams.subtitles == [
        SubtitleStream(index=4, lang='eng'),
        SubtitleStream(index=5, lang='fre'),
        SubtitleStream(index=6, lang='nolang'),
        SubtitleStream(index=7, lang='nolang')
    ]

def test_filter_streams():
    filename = 'input.mkv'
    tags = {
        'title': 'title'
    }

    widest_video_stream = VideoStream(index=0, codec='h264', width=1920, height=1080)
    another_video_stream = VideoStream(index=1, codec='vp9', width=1280, height=720)

    eng_audio_stream = AudioStream(index=2, codec='dts', lang='eng')
    fre_audio_stream = AudioStream(index=3, codec='vorbis', lang='fre')
    nolang_audio_stream = AudioStream(index=4, codec='opus', lang='nolang')

    eng_subtitle_stream = SubtitleStream(index=2, lang='eng')
    fre_subtitle_stream = SubtitleStream(index=3, lang='fre')
    nolang_subtitle_stream = SubtitleStream(index=4, lang='nolang')

    file_info = FileInfo(
        file_format=FileFormat(filename=filename, tags=tags),
        streams=Streams(
            videos=[widest_video_stream, another_video_stream],
            audios=[eng_audio_stream, fre_audio_stream, nolang_audio_stream] * 2,
            subtitles=[eng_subtitle_stream, fre_subtitle_stream, nolang_subtitle_stream] * 2
        )
    )

    s = filter_streams(file_info)
    assert s.videos == [widest_video_stream]
    assert s.audios == {
        'eng': [eng_audio_stream],
        'fre': [fre_audio_stream],
        'nolang': [nolang_audio_stream]
    }
    assert s.subtitles == {
        'eng': [eng_subtitle_stream],
        'fre': [fre_subtitle_stream],
        'nolang': [nolang_subtitle_stream]
    }

def test_encode_video_stream():
    file_path = 'input.mkv'
    stream_index = 1
    stream_width = 1920
    stream_height = 1080
    definition = 720
    bitrate = '3M'
    output_file_path = 'output.webm'
    with patch('ffmpy.FFmpeg.run', autospec=True) as mock:
        encode_video_stream(file_path, stream_index, stream_width, stream_height,  definition, bitrate, output_file_path)

        assert mock.call_count == 2

        first_pass_call = mock.call_args_list[0][0][0].cmd
        second_pass_call = mock.call_args_list[1][0][0].cmd

        assert '-i %s' % file_path in first_pass_call
        assert '-map 0:%s' % stream_index in first_pass_call
        assert '-s %sx%s' % (stream_width * definition // stream_height, definition) in first_pass_call
        assert '-b:v %s' % bitrate in first_pass_call
        assert '-passlogfile %s.stats' % output_file_path in first_pass_call
        assert '-pass 1' in first_pass_call
        assert '/dev/null' in first_pass_call

        assert '-i %s' % file_path in second_pass_call
        assert '-map 0:%s' % stream_index in second_pass_call
        assert '-s %sx%s' % (stream_width * definition // stream_height, definition) in second_pass_call
        assert '-b:v %s' % bitrate in second_pass_call
        assert '-pass 2' in second_pass_call
        assert output_file_path in second_pass_call

def test_encode_audio_stream():
    file_path = 'input.mkv'
    stream_index = 1
    output_file_path = 'output.webm'
    with patch('ffmpy.FFmpeg.run', autospec=True) as mock:
        encode_audio_stream(file_path, stream_index, output_file_path)
        assert mock.call_count == 1

        call = mock.call_args[0][0].cmd

        assert '-i %s' % file_path in call
        assert '-map 0:%s' % stream_index in call
        assert output_file_path in call

def test_encode_subtitle_stream():
    file_path = 'input.mkv'
    stream_index = 1
    output_file_path = 'output.vtt'
    with patch('ffmpy.FFmpeg.run', autospec=True) as mock:
        encode_subtitle_stream(file_path, stream_index, output_file_path)
        assert mock.call_count == 1

        call = mock.call_args[0][0].cmd

        assert '-i %s' % file_path in call
        assert '-map 0:%s' % stream_index in call
        assert output_file_path in call

def test_encode_manifest():
    base_path = '/random/base/path/'
    adaptation_sets = [
        ('adapt_0', [path.join(base_path, 'file_0'), path.join(base_path, 'file_1')]),
        ('adapt_1', [path.join(base_path, 'file_2')])
    ]
    output_file_path = 'manifest.mpd'
    with patch('ffmpy.FFmpeg.run', autospec=True) as mock:
        encode_manifest(base_path, adaptation_sets, output_file_path)
        assert mock.call_count == 1

        call = mock.call_args[0][0].cmd

        for i in range(0, 3):
            assert '-f webm_dash_manifest -i file_%s' % i in call
            assert '-map %s' % i in call

        assert '-f webm_dash_manifest -adaptation_sets "id=adapt_0,streams=0,1 id=adapt_1,streams=2"' in call
        assert output_file_path in call
