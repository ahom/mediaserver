from pathlib import Path
import json
from collections import namedtuple
from itertools import chain
import uuid
import os
import shutil

import luigi

from ffmpy import FFprobe, FFmpeg

VideoStream = namedtuple('VideoStream', ['index', 'codec', 'width', 'height'])
AudioStream = namedtuple('AudioStream', ['index', 'codec', 'lang'])
SubtitleStream = namedtuple('SubtitleStream', ['index', 'lang'])
Streams = namedtuple('Streams', ['videos', 'audios', 'subtitles'])
FileFormat = namedtuple('FileFormat', ['filename', 'tags'])
FileInfo = namedtuple('FileInfo', ['file_format', 'streams'])

def get_ffprobe_output(file_path):
    return FFprobe(
        inputs={ file_path.as_posix(): None },
        global_options=[
            '-v', 'error',
            '-print_format', 'json',
            '-show_streams'
        ]
    ).run()

class GetFFprobeOutputTask(luigi.Task):
    file_path = luigi.Parameter()

    def run(self):
        out, _ = get_ffprobe_output(self.file_path)
        with self.output().open('w') as output_file:
            output_file.write(out)

    def output(self):
        return luigi.LocalTarget('%s.ffprobe' % self.file_path.as_posix())

def decode_ffprobe_output(j):
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
                    lang=o.get('tags', {}).get('language', 'nolang')
                )
                for o in streams if o['codec_type'] == 'audio'
            ],
            subtitles=[
                SubtitleStream(
                    index=o['index'],
                    lang=o.get('tags', {}).get('language', 'nolang')
                )
                for o in streams if o['codec_type'] == 'subtitle'
            ]
        )
    )

class DecodeFFprobeOutputTask(luigi.Task):
    file_path = luigi.Parameter()

    def requires(self):
        return GetFFprobeOutputTask(self.file_path)

    def run(self):
        with self.input().open('r') as ffprobe_input_file:
            j = json.load(ffprobe_input_file)
            with self.output().open('w') as output_file:
                pickle.dumps(decode_ffprobe_output(j))

    def output(self):
        return luigi.LocalTarget('%s.file_info' % self.file_path.as_posix())

def filter_streams(file_info):
    # Get widest video stream
    max_width = max(v.width for v in file_info.streams.videos)
    video_streams = [v for v in file_info.streams.videos if v.width == max_width]

    # Get eng/fre audio streams
    audio_streams = {
        lang: [a for a in file_info.streams.audios if a.lang == lang] for lang in ['eng', 'fre', 'nolang']
    }

    # Get eng/fre subtitle streams
    subtitle_streams = {
        lang: [a for a in file_info.streams.subtitles if a.lang == lang] for lang in ['eng', 'fre', 'nolang']
    }

    # Only get first stream
    video_streams = video_streams[:1]

    for k in audio_streams.keys():
        audio_streams[k] = audio_streams[k][:1]

    for k in subtitle_streams.keys():
        subtitle_streams[k] = subtitle_streams[k][:1]

    return Streams(
        videos=video_streams,
        audios=audio_streams,
        subtitles=subtitle_streams
    )

class FilterStreamsTask(luigi.Task):
    file_path = luigi.Parameter()

    def requires(self):
        return DecodeFFprobeOutputTask(self.file_path)

    def run(self):
        with self.input().open('r') as file_info_file:
            file_info = pickle.loads(file_info_file)
            with self.output().open('w') as output_file:
                pickle.dumps(filter_streams(file_info))

    def output(self):
        return luigi.LocalTarget('%s.streams' % self.file_path.as_posix())

def encode_video_stream(file_path, video_stream, definition, bitrate, output_file_path):
    common_options = [
        '-map', '0:%s' % video_stream.index,
        '-c:v', 'libvpx-vp9',
        '-s', '%sx%s' % (definition * video_stream.width // video_stream.height, definition),
        '-b:v', bitrate,
        '-tile-columns', '6',
        '-frame-parallel', '1',
        '-keyint_min', '150',
        '-g', '150',
        '-qmin', '0',
        '-qmax', '50',
        '-f', 'webm',
        '-dash', '1'
    ]
    # first pass
    FFmpeg(
        inputs={ file_path.as_posix(): None },
        outputs={ 
            '/dev/null': chain(common_options, [
                '-pass', '1',
                '-speed', '4',
                '-passlogfile', '%s.stats' % output_file_path.as_posix()
            ])
        }
    ).run()
    # second pass
    return FFmpeg(
        inputs={ file_path.as_posix(): None },
        outputs={ 
            output_file_path.as_posix(): chain(common_options, [
                '-pass', '2',
                '-speed', '1',
                '-auto-alt-ref', '1',
                '-lag-in-frames', '25'
            ])
        }
    ).run()

class EncodeVideoStreamTask(luigi.Task):
    file_path = luigi.Parameter()
    video_stream = luigi.Parameter()
    definition = luigi.Parameter()
    bitrate = luigi.Parameter()

    def run(self):
        temp_file_path = Path('%s.tmp' % self.output().path)
        encode_video_stream(self.file_path, self.video_stream, self.definition, self.bitrate, temp_file_path)
        shutil.move(temp_file_path.as_posix(), self.output().path)

    def output(self):
        return luigi.LocalTarget(self.file_path.parent / 'video_%s_%sp.webm' % (self.video_stream.index, self.definition))

def encode_audio_stream(file_path, audio_stream, output_file_path):
    return FFmpeg(
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
    ).run()

class EncodeAudioStreamTask(luigi.Task):
    file_path = luigi.Parameter()
    audio_stream = luigi.Parameter()

    def run(self):
        temp_file_path = Path('%s.tmp' % self.output().path) 
        encode_audio_stream(self.file_path, self.audio_stream, temp_file_path)
        shutil.move(temp_file_path.as_posix(), self.output().path)

    def output(self):
        return luigi.LocalTarget(self.file_path.parent / 'audio_%s.webm' % self.audio_stream.index)

def encode_subtitle_stream(file_path, subtitle_stream, output_file_path):
    return FFmpeg(
        inputs={ file_path.as_posix(): None },
        outputs={ 
            output_file_path.as_posix(): [
                '-map', '0:%s' % subtitle_stream.index
            ]
        }
    ).run()

class EncodeSubtitleStreamTask(luigi.Task):
    file_path = luigi.Parameter()
    subtitle_stream = luigi.Parameter()

    def run(self):
        temp_file_path = Path('%s.tmp' % self.output().path) 
        encode_subtitle_stream(self.file_path, self.subtitle_stream, temp_file_path)
        shutil.move(temp_file_path.as_posix(), self.output().path)

    def output(self):
        return luigi.LocalTarget(self.file_path.parent / 'subtitle_%s.vtt' % self.subtitle_stream.index)
    default_provides = 'subtitle_encode_file_path'

def encode_manifest(base_path, adaptation_sets, output_file_path):
    current_count = 0
    adaptation_sets_helper = []
    for name, files in adaptation_sets:
        adaptation_sets_helper.append((name, files, current_count))
        current_count += len(files)

    FFmpeg(
        inputs={ 
            file_name.relative_to(base_path).as_posix(): ['-f', 'webm_dash_manifest'] 
                for file_name in chain.from_iterable(
                    files for _, files in adaptation_sets
                )              
        },
        outputs={ 
            output_file_path.as_posix(): chain(
                ['-c', 'copy'],
                chain.from_iterable(
                    [['-map', str(x)] for x in range(current_count)]
                ),
                [
                    '-f', 'webm_dash_manifest',
                    '-adaptation_sets', ' '.join(
                        'id=%s,streams=%s' % (
                            name,
                            ','.join(str(x) for x in range(start_id, start_id + len(files)))
                        ) for name, files, start_id in adaptation_sets_helper
                    )
                ]
            )
        }
    ).run()

class EncodeManifestTask(luigi.Task):
    file_path = luigi.Parameter()
    adaptation_sets = luigi.Parameter()

    def run(self):
        temp_file_path = Path('%s.tmp' % self.output().path) 
        encode_manifest(self.file_path.parent, self.adaptation_sets, temp_file_path)
        shutil.move(temp_file_path.as_posix(), self.output().path)

    def output(self):
        return luigi.LocalTarget((self.file_path.parent / 'manifest.mpd').as_posix())

class ProcessFileTask(luigi.Task):
    file_path = luigi.Parameter()

    def requires(self):
        return FilterStreamsTask(self.file_path)

    def run(self):
        with self.input().open('r') as streams_file:
            streams = pickle.loads(streams_file)

            # Encode streams
            video_tasks = [
                [
                    EncodeVideoStreamTask(
                        file_path=self.file_path,
                        video_stream=v,
                        definition=d,
                        bitrate=b
                    ) for d, b in [(1080, '6M'), (720, '3M'), (480, '1.5M')] if d <= v.height
                ] for v in streams.videos
            ]
            audio_tasks = [
                EncodeAudioStreamTask(
                    file_path=file_path, 
                    stream_index=a.index,
                    lang=a.lang
                ) for a in streams.audios.values()
            ]
            subtitle_tasks = [
                EncodeSubtitleStreamTask(
                    file_path=file_path, 
                    stream_index=s.index,
                    lang=s.lang
                ) for s in streams.subtitles.values()
            ]

            yield chain(chain.from_iterable(video_tasks), audio_tasks, subtitle_tasks)

            # Encode manifest
            manifest_task = EncodeManifestTask(
                base_path=self.file_path.parent,
                adaptation_sets=list(chain(
                    [(
                        'video_%s' % v.index, 
                        [t.output().path for t in video_tasks[i]]
                    ) for i, v in  enumerate(streams.videos)],
                    [(
                        'audio_%s_%s' % (a.index, a.lang), 
                        [a.output().path]
                    ) for a in audio_tasks],
                    [(
                        'subtitle_%s_%s' % (s.index, s.lang), 
                        [s.output().path]
                    ) for s in subtitle_tasks]
                ))
            )

            yield manifest_task

            with self.output().open('w') as out:
                out.writelines(task.output().path for task in chain(
                    chain.from_iterable(video_tasks),
                     audio_tasks,
                     subtitle_tasks,
                     [manifest_task]
                ))

    def output(self):
        return luigi.LocalTarget(self.file_path.parent / 'file_list.txt')

class CopyFileTask(luigi.Task):
    src = luigi.Parameter()
    dst = luigi.Parameter()

    def run(self):
        os.makedirs(self.dst.parent.as_posix())
        tmp_file_path = '%s.tmp' % self.output().path
        shutil.copyfile(self.src.as_posix(), tmp_file_path)
        shutil.move(tmp_file_path, self.output().path)

    def output(self):
        return luigi.LocalTarget(self.dst)


class HandleFileTask(luigi.Task):
    file_path = luigi.Parameter()
    base_work_dir = luigi.Parameter()
    base_final_dir = luigi.Parameter()

    def requires(self):
        return CopyFileTask(
            src=self.file_path,
            dst=self.base_work_dir / self.file_path.name / self.file_path.name
        )

    def run(self):
        process_file_task = ProcessFileTask(self.input().path)
        yield process_file_task

        # Try to get TMDb from file infos

        # Deduce folder to put in

        # Copy files to acd

        # Update DB to know that the file has been processed

        # Remove work_dir
        shutil.rmtree(base_path)
