import os
import sys
import logging
import traceback
import hashlib
import pandas as pd
import csv
from dotenv import load_dotenv
from pprint import pprint

import rx
import rx.operators as ops
import diart.operators as dops
from diart import OnlineSpeakerDiarization, PipelineConfig
from diart.sources import WebSocketAudioSource
from huggingface_hub import login

from source import clair
from source import speech
from source.clair import RepetitionDetectedError


def setup_logger(name, log_file, level=logging.INFO):
    """Function to set up a logger with the given name and log file."""
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    logger.addHandler(stream_handler)
    logger.propagate = False  # Prevent propagation to root
    return logger

# Initialize logger
logger = setup_logger('server', 'logs/server.log')

def main():
    try:
        # Load environment variables from .env file
        load_dotenv()

        # Transcription server configuration
        HOST = "0.0.0.0"
        PORT = int(os.environ.get("PORT"))
        logger.info(f"Environment variables loaded. Server will listen on {HOST}:{PORT}")

        # Authenticate with Hugging Face
        huggingface_token = os.environ.get("HF_TOKEN")
        if huggingface_token:
            login(token=huggingface_token)
            logger.info("Authenticated with Hugging Face.")
        else:
            logger.warning("HF_TOKEN not set. Some features may not work.")

        """
        Whisper configuration
        - WHISPER_SIZE is the model name used for transcription (eg 'base', 'small','large')
        - WHISPER_COMPRESS_RATIO_THRESHOLD is the threshold for the compression ratio
        - WHISPER_NO_SPEECH_THRESHOLD is the threshold for the no speech probability
        """
        WHISPER_SIZE = os.environ.get("WHISPER_SIZE")
        WHISPER_COMPRESS_RATIO_THRESHOLD = float(os.environ.get("WHISPER_COMPRESS_RATIO_THRESHOLD"))
        WHISPER_NO_SPEECH_THRESHOLD = float(os.environ.get("WHISPER_NO_SPEECH_THRESHOLD"))

        """
        Diarization pipeline configuration
        - DURATION is the duration of the audio chunk to be processed at once
        - STEP is the amount by which the audio window "slides" forward to create the next chunk, controlling the overlap between consecutive audio chunks. 
            With step=0.5 and duration=2, chunk1 will be [0s-2s], chunk2 will be [0.5s-2.5s], ...
        - SAMPLE_RATE is the sample rate of the audio source (not needed for the pipeline)
        - TAU, RHO, DELTA are the parameters for the diarization algorithm
        """
        PIPELINE_LANGUAGE = os.environ.get("PIPELINE_LANGUAGE")
        PIPELINE_MAX_SPEAKERS = int(os.environ.get("PIPELINE_MAX_SPEAKERS"))
        PIPELINE_DURATION = float(os.environ.get("PIPELINE_DURATION"))
        PIPELINE_STEP = float(os.environ.get("PIPELINE_STEP"))
        MIC_SAMPLE_RATE = int(os.environ.get("MIC_SAMPLE_RATE"))
        PIPELINE_TAU = float(os.environ.get("PIPELINE_TAU"))
        PIPELINE_RHO = float(os.environ.get("PIPELINE_RHO"))
        PIPELINE_DELTA = float(os.environ.get("PIPELINE_DELTA"))

        # Clair API configuration
        CLAIR_URL = os.environ.get("CLAIR_URL")
        CLAIR_TOKEN = os.environ.get("CLAIR_TOKEN")

        # Activate Clair configuration
        try:
            req = clair.activate_configuration(
                mode='ssrl', 
                language='EN', 
                keywords=['force', 'energy conservation', 'kinectic', 'potential'], # not used (as TSIM is not used in the SSRL mode)
                host=CLAIR_URL,
                token=CLAIR_TOKEN
            )
        except Exception as e:
            logger.error(f"Failed to activate Clair configuration: {e}", exc_info=True)

        # Pipeline configuration
        speech_config = PipelineConfig(
            duration=PIPELINE_DURATION,
            step=PIPELINE_STEP,  # When lower is more accurate but slower
            sample_rate=MIC_SAMPLE_RATE,
            latency="min",       # When higher is more accurate but slower
            tau_active=PIPELINE_TAU,   # suggested by diart paper 
            rho_update=PIPELINE_RHO,    # suggested by diart paper
            delta_new=PIPELINE_DELTA,   # suggested by diart paper
            device="cuda",
            max_speakers=PIPELINE_MAX_SPEAKERS,
        )

        # Initialize ASR and diarization
        asr = speech.WhisperTranscriber(model=WHISPER_SIZE, device="cuda", 
                                        language=PIPELINE_LANGUAGE,
                                        compression_ratio_threshold=WHISPER_COMPRESS_RATIO_THRESHOLD, 
                                        no_speech_threshold=WHISPER_NO_SPEECH_THRESHOLD)
        dia = OnlineSpeakerDiarization(speech_config)

        # Split the stream into chunks of seconds for transcription
        # batch_size is the number of audio chunks grouped together into a single batch before being sent to the diarization step.
        batch_size = int(PIPELINE_DURATION // PIPELINE_STEP)

        # Initialize dialogue and group ID
        dialogue = []
        # group_id = hashlib.md5(pd.Timestamp.now().strftime('%Y%m%d%H%M%S').encode()).hexdigest()[:6]
        group_id = CLAIR_TOKEN # considering that the token is unique for each group (eg in Oulu's fall'24 data collection)
        last_processed_turn = {'last_turn': None}

        # Set up audio source
        source = WebSocketAudioSource(MIC_SAMPLE_RATE, HOST, PORT)

        # Define the processing pipeline
        source.stream.pipe(
            # Format audio stream to sliding windows
            dops.rearrange_audio_stream(
                speech_config.duration, speech_config.step, speech_config.sample_rate
            ),
            # Buffer audio chunks
            ops.buffer_with_count(count=batch_size),
            # Diarization
            ops.map(dia),
            # Concatenate audio chunks
            ops.map(speech.concat),
            # ASR processing
            ops.map(lambda ann_wav: ('', '') if ann_wav[0].get_timeline().duration() == 0 else asr(*ann_wav)),
            # Transcription cleanup
            ops.map(lambda speaker_caption: '' if speaker_caption == ('', '') else speech.message_transcription(speaker_caption)),
            # Buffering turns
            ops.map(lambda text: clair.buffering_turn(text, dialogue, group_id, 
                                                      turn_threshold=5,
                                                      silence_threshold=3, 
                                                      last_processed_turn=last_processed_turn,
                                                      verbose=True)),
            # Flatten turns
            ops.flat_map(lambda turns: rx.from_iterable(turns)),
            # Filter out empty turns
            ops.filter(lambda turn: 'text' not in turn or turn['text'].strip() != ''),
            # Send to API
            ops.map(lambda turn: clair.send_to_api_and_get_response(**turn, dialogue=dialogue, 
                                                                    host=CLAIR_URL, 
                                                                    token=CLAIR_TOKEN,
                                                                    verbose=True))
        ).subscribe(
            on_next=lambda output: (
                logger.info(f"Received API response: {output}"),
                source.send(output)
            ) if output else None,
            on_error=lambda e: handle_error(e, source)
        )
        logger.info("Audio processing pipeline established.")

        # Start listening
        logger.info(f"Listening... Group ID: {group_id}")
        source.read()

    except Exception as e:
        logger.critical(f"Critical error in main: {e}", exc_info=True)
        sys.exit(1)

    finally:
        logger.info("Stopped listening.")
        # Export transcribed dialogue to a csv file
        if dialogue:
            try:
                for msg in dialogue:
                    msg['group'] = group_id
                df = pd.DataFrame(dialogue)[['group', 'username', 'timestamp', 'text']]
                if not os.path.exists(f'logs/{group_id}.csv'):
                    df.to_csv(f'logs/{group_id}.csv', index=False, sep="|", quoting=csv.QUOTE_NONNUMERIC)
                else:
                    df.to_csv(f'logs/{group_id}.csv', index=False, sep="|", quoting=csv.QUOTE_NONNUMERIC,
                              mode='a', header=False)
                logger.info(f"Dialogue exported to logs/{group_id}.csv")
            except Exception as export_error:
                logger.error(f"Failed to export dialogue: {export_error}", exc_info=True)


def handle_error(e, source):
    if isinstance(e, RepetitionDetectedError):
        logger.error(f"Repetition detected: {e}")
        source.close()
        sys.exit(1)
    else:
        logger.error(f"Error in stream processing: {e}", exc_info=True)


if __name__ == "__main__":
    main()