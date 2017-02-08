from os import path, makedirs
import json
from collections import namedtuple
from itertools import chain
import uuid
import subprocess
import shutil
import pickle

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
        inputs={ file_path: None },
        global_options=[
            '-v', 'error',
            '-print_format', 'json',
            '-show_streams', '-show_format'
        ]
    ).run(stdout=subprocess.PIPE)

class GetFFprobeOutputTask(luigi.Task):
    file_path = luigi.Parameter()

    def run(self):
        out, _ = get_ffprobe_output(self.file_path)
        with self.output().open('wb') as output_file:
            output_file.write(out)

    def output(self):
        return luigi.LocalTarget('%s.ffprobe' % self.file_path, format=luigi.format.Nop)

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
        with self.input().open('rb') as ffprobe_input_file:
            j = json.load(ffprobe_input_file)
            with self.output().open('wb') as output_file:
                pickle.dump(decode_ffprobe_output(j), output_file)

    def output(self):
        return luigi.LocalTarget('%s.file_info' % self.file_path, format=luigi.format.Nop)

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
        with self.input().open('rb') as file_info_file:
            file_info = pickle.load(file_info_file)
            with self.output().open('wb') as output_file:
                pickle.dump(filter_streams(file_info), output_file)

    def output(self):
        return luigi.LocalTarget('%s.streams' % self.file_path, format=luigi.format.Nop)

def encode_video_stream(file_path, stream_index, stream_width, stream_height, definition, bitrate, output_file_path):
    common_options = [
        '-y',
        '-map', '0:%s' % stream_index,
        '-c:v', 'libvpx',
        '-s', '%sx%s' % (definition * stream_width // stream_height, definition),
        '-b:v', bitrate,
        '-keyint_min', '150',
        '-g', '150',
        '-qmin', '0',
        '-qmax', '60',
        '-slices', '8',
        '-threads', '2',
        '-f', 'webm',
        '-dash', '1',
        '-passlogfile', '%s.stats' % output_file_path
    ]
    # first pass
    FFmpeg(
        inputs={ file_path: None },
        outputs={ 
            '/dev/null': chain(common_options, [
                '-pass', '1'
            ])
        }
    ).run()
    # second pass
    return FFmpeg(
        inputs={ file_path: None },
        outputs={ 
            output_file_path: chain(common_options, [
                '-pass', '2',
                '-auto-alt-ref', '1',
                '-lag-in-frames', '25'
            ])
        }
    ).run()

class EncodeVideoStreamTask(luigi.Task):
    file_path = luigi.Parameter()
    stream_index = luigi.IntParameter()
    stream_width = luigi.IntParameter()
    stream_height = luigi.IntParameter()
    definition = luigi.IntParameter()
    bitrate = luigi.Parameter()

    def run(self):
        temp_file_path = '%s.tmp' % self.output().path
        encode_video_stream(self.file_path, self.stream_index, self.stream_width, self.stream_height, self.definition, self.bitrate, temp_file_path)
        shutil.move(temp_file_path, self.output().path)

    def output(self):
        return luigi.LocalTarget(path.join(path.dirname(self.file_path), 'video_%s_%sp.webm' % (self.stream_index, self.definition)))

def encode_audio_stream(file_path, stream_index, output_file_path):
    return FFmpeg(
        inputs={ file_path: None },
        outputs={ 
            output_file_path: [
                '-map', '0:%s' % stream_index,
                '-c:a', 'libvorbis',
                '-b:a', '160k',
                '-ac', '2',
                '-f', 'webm',
                '-dash', '1'
            ]
        }
    ).run()

class EncodeAudioStreamTask(luigi.Task):
    file_path = luigi.Parameter()
    stream_index = luigi.IntParameter()

    def run(self):
        temp_file_path = '%s.tmp' % self.output().path 
        encode_audio_stream(self.file_path, self.stream_index, temp_file_path)
        shutil.move(temp_file_path, self.output().path)

    def output(self):
        return luigi.LocalTarget(path.join(path.dirname(self.file_path), 'audio_%s.webm' % self.stream_index))

def encode_subtitle_stream(file_path, stream_index, output_file_path):
    return FFmpeg(
        inputs={ file_path: None },
        outputs={ 
            output_file_path: [
                '-map', '0:%s' % stream_index,
                '-c:s', 'webvtt',
                '-f', 'webvtt'
            ]
        }
    ).run()

class EncodeSubtitleStreamTask(luigi.Task):
    file_path = luigi.Parameter()
    stream_index = luigi.IntParameter()

    def run(self):
        temp_file_path = '%s.tmp' % self.output().path 
        encode_subtitle_stream(self.file_path, self.stream_index, temp_file_path)
        shutil.move(temp_file_path, self.output().path)

    def output(self):
        return luigi.LocalTarget(path.join(path.dirname(self.file_path), 'subtitle_%s.vtt' % self.stream_index))

def encode_manifest(base_path, adaptation_sets, output_file_path):
    current_count = 0
    adaptation_sets_helper = []
    for name, files in adaptation_sets:
        adaptation_sets_helper.append((name, files, current_count))
        current_count += len(files)

    FFmpeg(
        inputs={ 
            file_name: ['-f', 'webm_dash_manifest'] 
                for file_name in chain.from_iterable(
                    files for _, files in adaptation_sets
                )              
        },
        outputs={ 
            output_file_path: chain(
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
    base_path = luigi.Parameter()
    adaptation_sets = luigi.ListParameter()

    def run(self):
        temp_base_path = '%s.tmp' % self.output().path 
        encode_manifest(self.base_path, self.adaptation_sets, temp_base_path)
        shutil.move(temp_base_path, self.output().path)

    def output(self):
        return luigi.LocalTarget(path.join(self.base_path, 'manifest.mpd'))

class ProcessFileTask(luigi.Task):
    file_path = luigi.Parameter()

    def requires(self):
        return FilterStreamsTask(self.file_path)

    def run(self):
        with self.input().open('rb') as streams_file:
            streams = pickle.load(streams_file)

            # Encode streams
            video_tasks = [
                [
                    EncodeVideoStreamTask(
                        file_path=self.file_path,
                        stream_index=v.index,
                        stream_width=v.width,
                        stream_height=v.height,
                        definition=d,
                        bitrate=b
                    ) for d, b in [(1080, '10M'), (720, '5M'), (480, '2.5M')] if d <= v.height
                ] for v in streams.videos
            ]
            audio_tasks = list(chain.from_iterable([
                [
                    EncodeAudioStreamTask(
                        file_path=self.file_path, 
                        stream_index=a.index
                    ) for a in a_list
                ] for a_list in streams.audios.values()
            ]))
            subtitle_tasks = list(chain.from_iterable([
                [
                    EncodeSubtitleStreamTask(
                        file_path=self.file_path, 
                        stream_index=s.index
                    ) for s in s_list
                ] for s_list in streams.subtitles.values()
            ]))
            subtitle_tasks = []

            yield chain(chain.from_iterable(video_tasks), audio_tasks)

            # Encode manifest
            manifest_task = EncodeManifestTask(
                base_path=path.dirname(self.file_path),
                adaptation_sets=list(chain(
                    [(
                        'video_%s' % v.index, 
                        [t.output().path for t in video_tasks[i]]
                    ) for i, v in  enumerate(streams.videos)],
                    [(
                        'audio_%s_%s' % (a.index, a.lang), 
                        [audio_tasks[i].output().path]
                    ) for i, a in enumerate(chain.from_iterable(streams.audios.values()))]
                    #[(
                    #    'subtitle_%s_%s' % (s.index, s.lang), 
                    #    [subtitle_tasks[i].output().path]
                    #) for i, s in enumerate(chain.from_iterable(streams.subtitles.values()))]
                ))
            )

            yield manifest_task

            with self.output().open('w') as out:
                out.write("\n".join(task.output().path for task in chain(
                    chain.from_iterable(video_tasks),
                    audio_tasks,
                    subtitle_tasks,
                    [manifest_task]
                )))

    def output(self):
        return luigi.LocalTarget(path.join(path.dirname(self.file_path), 'file_list.txt'))

class CopyFileTask(luigi.Task):
    src = luigi.Parameter()
    dst = luigi.Parameter()

    def run(self):
        dir_path = path.dirname(self.dst)
        if not path.exists(dir_path):
            makedirs(dir_path)
        tmp_file_path = '%s.tmp' % self.output().path
        shutil.copyfile(self.src, tmp_file_path)
        shutil.move(tmp_file_path, self.output().path)

    def output(self):
        return luigi.LocalTarget(self.dst)


class HandleFileTask(luigi.Task):
    file_path = luigi.Parameter()
    base_work_dir = luigi.Parameter()
    base_staging_dir = luigi.Parameter()

    def requires(self):
        filename = path.basename(self.file_path)
        return CopyFileTask(
            src=self.file_path,
            dst=path.join(self.base_work_dir, filename, filename)
        )

    def run(self):
        process_file_task = ProcessFileTask(self.input().path)
        yield process_file_task

        staging_dir = path.join(self.base_staging_dir, path.basename(self.file_path))

        with process_file_task.output().open('r') as list_file:
            yield [CopyFileTask(
                src=file_path.strip(),
                dst=path.join(staging_dir, path.basename(file_path.strip()))
            ) for file_path in list_file]

        yield CopyFileTask(
            src=self.input().path,
            dst=path.join(staging_dir, 'input.mkv')
        )

        shutil.rmtree(path.join(self.base_work_dir, path.basename(self.file_path)))
