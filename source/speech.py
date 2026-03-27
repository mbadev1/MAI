import os
import sys
from contextlib import contextmanager
import numpy as np
from pyannote.core import Annotation, SlidingWindowFeature, SlidingWindow, Segment
import whisper_timestamped as whisper


##helper function to concatenate transcriptions and diarizations
def concat(chunks, collar=0.05):
    """
    Concatenate predictions and audio
    given a list of `(diarization, waveform)` pairs
    and merge contiguous single-speaker regions
    with pauses shorter than `collar` seconds.
    """
    first_annotation = chunks[0][0]
    first_waveform = chunks[0][1]
    annotation = Annotation(uri=first_annotation.uri)
    data = []
    for ann, wav in chunks:
        annotation.update(ann)
        data.append(wav.data)
    annotation = annotation.support(collar)
    window = SlidingWindow(
        first_waveform.sliding_window.duration,
        first_waveform.sliding_window.step,
        first_waveform.sliding_window.start,
    )
    data = np.concatenate(data, axis=0)
    return annotation, SlidingWindowFeature(data, window)

@contextmanager
def suppress_stdout():
    # Auxiliary function to suppress Whisper logs (it is quite verbose)
    with open(os.devnull, "w") as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:
            yield
        finally:
            sys.stdout = old_stdout


class WhisperTranscriber:
    def __init__(self, model="small", device=None, language="fi", compression_ratio_threshold=1.0, no_speech_threshold=0.4):
        self.model = whisper.load_model(model, device=device)
        self.language = language
        self.compression_ratio_threshold = compression_ratio_threshold
        self.no_speech_threshold = no_speech_threshold
        self._buffer = ""

    def transcribe(self, waveform):
        """Transcribe audio using Whisper"""
        # Pad/trim audio to fit 30 seconds as required by Whisper
        audio = waveform.data.astype("float32").reshape(-1)
        audio = whisper.pad_or_trim(audio)

        # Transcribe the given audio while suppressing logs
        with suppress_stdout():
            transcription = whisper.transcribe(
                model=self.model,
                audio=audio,
                language=self.language, 
                task = "translate",
                # Extra options to reduce hallucinations from the Whisper model
                vad=True,
                detect_disfluencies=True,
                condition_on_previous_text=False,
                # We use past transcriptions to condition the model
                initial_prompt=self._buffer,
                verbose=True, # to avoid progress bar
                compression_ratio_threshold = self.compression_ratio_threshold,
                no_speech_threshold = self.no_speech_threshold,
                ##decode_options=options
            )
            
        return transcription
    
    def identify_speakers(self, transcription, diarization, time_shift):
        """Iterate over transcription segments to assign speakers"""
        speaker_captions = []
        for segment in transcription["segments"]:

            # Crop diarization to the segment timestamps
            start = time_shift + segment["words"][0]["start"]
            end = time_shift + segment["words"][-1]["end"]
            dia = diarization.crop(Segment(start, end))

            # Assign a speaker to the segment based on diarization
            speakers = dia.labels()
            num_speakers = len(speakers)
            if num_speakers == 0:
                # No speakers were detected
                caption = (-1, segment["text"])
            elif num_speakers == 1:
                # Only one speaker is active in this segment
                spk_id = int(speakers[0].split("speaker")[1])
                caption = (spk_id, segment["text"])
            else:
                # Multiple speakers, select the one that speaks the most
                max_speaker = int(np.argmax([
                    dia.label_duration(spk) for spk in speakers
                ]))
                caption = (max_speaker, segment["text"])
            speaker_captions.append(caption)

        return speaker_captions

    def __call__(self, diarization, waveform):
        # Step 1: Transcribe
        transcription = self.transcribe(waveform)
        # Update transcription buffer
        # self._buffer += transcription["text"]
        if len(set(transcription["text"])) > 3:
            self._buffer = transcription["text"].split('.')[-1]
        # The audio may not be the beginning of the conversation
        time_shift = waveform.sliding_window.start
        # Step 2: Assign speakers
        speaker_transcriptions = self.identify_speakers(transcription, diarization, time_shift)
        return speaker_transcriptions

##helper function to make different speakers appear as messages
def message_transcription(transcription):
    result = []
    for speaker, text in transcription:
        if speaker == -1:
            # No speakerfound for this text, use default terminal color
            result.append(text)
        else:
            result.append("Speaker"+str(speaker)+": "+text)
    return "\n".join(result)
