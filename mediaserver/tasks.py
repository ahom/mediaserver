from pathlib import Path
import json
from collections import namedtuple
from itertools import chain
import uuid
import os

import luigi

from ffmpy import FFprobe, FFmpeg

VideoStream = namedtuple('VideoStream', ['index', 'codec', 'width', 'height'])
AudioStream = namedtuple('AudioStream', ['index', 'codec', 'lang'])
SubtitleStream = namedtuple('SubtitleStream', ['index', 'lang'])
Streams = namedtuple('Streams', ['videos', 'audios', 'subtitles'])
FileFormat = namedtuple('FileFormat', ['filename', 'tags'])
FileInfo = namedtuple('FileInfo', ['file_format', 'streams'])

class GetFFprobeOutputTask(luigi.Task):
    file_path = luigi.Parameter()

    def run(self):
        ff = self.ffprobe_func(
            inputs={ file_work_path.as_posix(): None },
            global_options=[
                '-v', 'error',
                '-print_format', 'json',
                '-show_streams'
            ]
        )

        out, err = ff.run()

        with self.output().open('w') as output_file:
            output_file.write(out)

    def output(self):
        return luigi.LocalTarget('%s.ffprobe' % self.file_path.as_posix())


class DecodeFFprobeOutputTask(luigi.Task):
    file_path = luigi.Parameter()

    def requires(self):
        return GetFFprobeOutputTask(self.file_path)

    def run(self):
        with self.input().open('r') as ffprobe_input_file:
            j = json.load(ffprobe_input_file)
            streams = [ o for o in j['streams'] if 'codec_type' in o ]

            with self.output().open('w') as output_file:
                pickle.dumps(FileInfo(
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
                ))

    def output(self):
        return luigi.LocalTarget('%s.file_info' % self.file_path.as_posix())

class FilterStreamsTask(luigi.Task):
    file_path = luigi.Parameter()

    def requires(self):
        return DecodeFFprobeOutputTask(self.file_path)

    def run(self):
        with self.input().open('r') as file_info_file:
            file_info = pickle.loads(file_info_file)

            # Get widest video stream
            max_width = max(v.width for v in file_info.streams.videos)
            video_streams = [v for v in file_info.streams.videos if v.width == max_width]

            # Get eng/fre audio streams
            audio_streams = {
                lang: [a for a in file_path.streams.audios if a.lang == lang] for lang in ['eng', 'fre']
            }

            # Get eng/fre subtitle streams
            subtitle_streams = {
                lang: [a for a in file_path.streams.subtitles] for lang in ['eng', 'fre']
            }

            tasks = [] 
            if video_streams:
                video_streams = video_streams[0]

            for a in audio_streams.values():
                if a:
                    a = a[0]

            for s in subtitle_streams.values():
                if s:
                    s = s[0]

            with self.output().open('w') as output_file:
                pickle.dumps(Streams(
                    videos=video_streams,
                    audios=audio_streams,
                    subtitles=subtitle_streams
                ))

    def output(self):
        return luigi.LocalTarget('%s.streams' % self.file_path.as_posix())


class EncodeManifestTask(luigi.Task):
    file_path = luigi.Parameter()

    def requires(self):
        return FilterStreamsTask(self.file_path)

    def run(self):
        with self.input().open('r') as file_info_file:
            file_info = pickle.loads(file_info_file)

            # Getting video stream

            video_stream = [s for s in file_info.streams.video if s.width == max(]
            for v in file_info.streams.videos:




    def output(self):
        return luigi.LocalTarget((file_path.parent / 'manifest.mpd').as_posix())

class EncodeVideoStreamTask(luigi.Task):
    file_path = luigi.Parameter()
    stream_index = luigi.Parameter()
    definition = luigi.Parameter()
    bitrate = luigi.Parameter()

    def requires(self):
        return DecodeFFprobeOutputTask(self.file_path)

    def _first_pass(self, file_work_path, common_options):
        ff = self.ffmpeg_func(
            inputs={ file_work_path.as_posix(): None },
            outputs={ 
                '/dev/null': chain(common_options, [
                    '-pass', '1',
                    '-speed', '4',
                    '-passlogfile', ''
                ])
            }
        )

        ff.run()

    def _second_pass(self, file_work_path, common_options):
        output_file_path = file_work_path.parent / ('video-%sp.webm' % self.definition)

        ff = self.ffmpeg_func(
            inputs={ file_work_path.as_posix(): None },
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

    def run(self, file_work_path, video_stream):
        common_options = [
            '-map', '0:%s' % video_stream.index,
            '-c:v', 'libvpx-vp9',
            '-s', '%sx%s' % (self.definition * video_stream.width // video_stream.height, self.definition),
            '-b:v', self.DEFINITION_TO_BITRATE[self.definition],
            '-tile-columns', '6',
            '-frame-parallel', '1',
            '-keyint_min', '150',
            '-g', '150',
            '-qmin', '0',
            '-qmax', '50',
            '-f', 'webm',
            '-dash', '1'
        ]
        self._first_pass(file_work_path, common_options)
        return self._second_pass(file_work_path, common_options)

class EncodeAudioStream(task.Task):
    default_provides = 'audio_encode_file_path'

    def __init__(self, ffmpeg_func=FFmpeg, **kwargs):
        super(EncodeAudioStream, self).__init__(**kwargs)
        self.ffmpeg_func = ffmpeg_func

    def execute(self, file_work_path, audio_stream):
        output_file_path = file_work_path.parent / ('audio-%s.webm' % audio_stream.lang)

        ff = self.ffmpeg_func(
            inputs={ file_work_path.as_posix(): None },
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
    default_provides = 'subtitle_encode_file_path'

    def __init__(self, ffmpeg_func=FFmpeg, **kwargs):
        super(EncodeSubtitleStream, self).__init__(**kwargs)
        self.ffmpeg_func = ffmpeg_func

    def execute(self, file_work_path, subtitle_stream):
        output_file_path = file_work_path.parent / ('subtitle-%s.vtt' % subtitle_stream.lang)

        ff = self.ffmpeg_func(
            inputs={ file_work_path.as_posix(): None },
            outputs={ 
                output_file_path.as_posix(): [
                    '-map', '0:%s' % subtitle_stream.index
                ]
            }
        )

        ff.run()

        return output_file_path

def CreateWorkDir(task.Task):
    default_provides = 'work_dir'

    def __init__(self, parent_path, **kwargs):
        super(CreateWorkDir, self).__init__(**kwargs)
        self.parent_path = parent_path

    def execute(self):
        work_dir = parent_path / uuid.uuid4().hex
        os.makedirs(work_dir)
        return work_dir

def ProcessFile(file_path):
    p = Path('~/temp_dirs')
    f = lf.Flow()
    f.add(CreateWorkDir(p))
    f.add(ExplodePath(rebind={ 'path': 'file_path' }, provides=('file_path_dir', 'file_path_name')))
    f.add(JoinPaths(rebind={ 'path1': 'work_dir', 'path2': 'file_path_name' }, provides=('file_work_path'))
    f.add(CopyFile(rebind={ 'source': 'file_path', 'dest': 'file_work_path' }))
    f.add(GetFFprobeOutput())
    f.add(DecodeFFprobeOutput())


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

