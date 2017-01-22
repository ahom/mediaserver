from pathlib import Path
import json
from collections import namedtuple
from itertools import chain

from taskflow import task
from taskflow.patterns import linear_flow as lf

from ffmpy import FFprobe, FFmpeg

VideoStream = namedtuple('VideoStream', ['index', 'codec', 'width', 'height'])
AudioStream = namedtuple('AudioStream', ['index', 'codec', 'lang'])
SubtitleStream = namedtuple('SubtitleStream', ['index', 'lang'])
Streams = namedtuple('Streams', ['videos', 'audios', 'subtitles'])
FileFormat = namedtuple('FileFormat', ['filename', 'tags'])
FileInfo = namedtuple('FileInfo', ['file_format', 'streams'])

class GetFFprobeOutput(task.Task):
    default_provides = 'ffprobe_output'

    def __init__(self, ffprobe_func=FFprobe):
        super(GetFFprobeOutput, self).__init__()
        self.ffprobe_func = ffprobe_func

    def execute(self, file_path):
        ff = self.ffprobe_func(
            inputs={ file_path.as_posix(): None },
            global_options=[
                '-v', 'error',
                '-print_format', 'json',
                '-show_streams'
            ]
        )

        out, err = ff.run()

        return out

class DecodeFFprobeOutput(task.Task):
    default_provides = 'file_info'

    def execute(self, ffprobe_output):
        j = json.loads(ffprobe_output)
        streams = [ o for o in j['streams'] if 'codec_type' in o ]

        return FileInfo(
            file_format=FileFormat(filename=j['format']['filename'],tags=j['format']['tags']),
            streams=Streams(
                videos=[
                    VideoStream(
                        index=o['index'],
                        codec=o['codec_name'],
                        width=o['width'],
                        height=o['height']
                    )
                    for o in streams if o['codec_type'] == 'video'
                ],
                audios=[
                    AudioStream(
                        index=o['index'],
                        codec=o['codec_name'],
                        lang=o.get('tags', {}).get('language', None)
                    )
                    for o in streams if o['codec_type'] == 'audio'
                ],
                subtitles=[
                    SubtitleStream(
                        index=o['index'],
                        lang=o.get('tags', {}).get('language', None)
                    )
                    for o in streams if o['codec_type'] == 'subtitle'
                ]
            )
        )

def GetFileInfo():
    f = lf.Flow()
    f.add(GetFFprobeOutput())
    f.add(DecodeFFprobeOutput())
    return f

class FetchTMDbIds(task.Task):
    default_provides = 'tmdb_ids'

    # TODO: fetch TMDbIds from file_info and torrent_name
    def execute(self, file_info, torrent_name):
        return []

class EncodeVideoStream(task.Task):
    DEFINITION_TO_BITRATE = {
        1080: '6M',
        720:  '3M',
        480:  '1.5M'
    }

    def __init__(self, definition, ffmpeg_func=FFmpeg):
        super(EncodeVideoStream, self).__init__()
        self.definition = definition
        self.ffmpeg_func = ffmpeg_func

    def _first_pass(self, file_path, common_options):
        ff = self.ffmpeg_func(
            inputs={ file_path.as_posix(): None },
            outputs={ 
                'dummy_first_pass.webm': chain(common_options, [
                    '-pass', '1',
                    '-speed', '4'
                ])
            }
        )

        ff.run()

    def _second_pass(self, file_path, common_options):
        output_file_path = file_path.parent / 'video-%s.webm' % self.definition

        ff = self.ffmpeg_func(
            inputs={ file_path.as_posix(): None },
            outputs={ 
                output_file_path.as_posix(): chain(common_options, [
                    '-pass', '2',
                    '-speed', '1',
                    '-auto-alt-ref', '1',
                    '-lag-in-frames', '25'
                ])
            }
        )

        ff.run()

        return output_file_path

    def execute(self, file_path, video_stream):
        common_options = [
            '-map', '0:%s' % video_stream.index,
            '-c:v', 'libvpx-vp9',
            '-s', '%sx%s' % (self.definition * video_stream.width // video_stream.height, self.definition),
            '-b:v', DEFINITION_TO_BITRATE[self.definition],
            '-tile-columns', '6',
            '-frame-parallel', '1',
            '-keyint_min', '150',
            '-g', '150',
            '-qmin', '0',
            '-qmax', '50',
            '-f', 'webm',
            '-dash', '1'
        ]
        self._first_pass(file_path, common_options)
        return self._second_pass(file_path, common_options)

class EncodeAudioStream(task.Task):
    def __init__(self, ffmpeg_func=FFmpeg):
        super(EncodeAudioStream, self).__init__()
        self.ffmpeg_func = ffmpeg_func

    def execute(self, file_path, audio_stream):
        output_file_path = file_path.parent / 'audio-%s.webm' % audio_stream.lang

        ff = self.ffmpeg_func(
            inputs={ file_path.as_posix(): None },
            outputs={ 
                output_file_path.as_posix(): [
                    '-map', '0:%s' % audio_stream.index,
                    '-c:a', 'libvorbis',
                    '-b:a', '160k',
                    '-f', 'webm',
                    '-dash', '1'
                ]
            }
        )

        ff.run()

        return output_file_path

class EncodeSubtitleStream(task.Task):
    def __init__(self, ffmpeg_func=FFmpeg):
        super(EncodeSubtitleStream, self).__init__()
        self.ffmpeg_func = ffmpeg_func

    def execute(self, file_path, subtitle_stream):
        output_file_path = file_path.parent / 'subtitle-%s.vtt' % subtitle_stream.lang

        ff = self.ffmpeg_func(
            inputs={ file_path.as_posix(): None },
            outputs={ 
                output_file_path.as_posix(): [
                    '-map', '0:%s' % subtitle_stream.index
                ]
            }
        )

        ff.run()

        return output_file_path

# Process torrent (Launched by web ui)
#  - Fetch torrent file
#  - Put torrent file in transmission watched folder
### TRANSMISSION
#  - Process torrent files (Launched by transmission's hook)
#    - Filter files (keep only video files)
#    - Process file (parallel)
#      - Create work folder
#      - Do work
#          - Move file to work folder
#          - Get file info (FFprobe)
#          - Fetch TMDb ID(s)
#          - Encode streams
#          - Encode manifest
#          - Upload to ACD
#          - Update library db
#      - Delete work folder

