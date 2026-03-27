"""
This script is the client_mic application for the MAI-CLAIR project.
It connects to the server, streams the microphone audio to the server,
and plays the audio response from the server.

Usage: python client_mic.py
"""
import os
import json
import random
import logging
import sys
import rx.operators as ops
from websocket import WebSocket, create_connection, WebSocketException
from diart.sources import MicrophoneAudioSource
from diart.utils import encode_audio
import dotenv
import threading 
import pyttsx3
import sounddevice as sd
import soundfile as sf
import time
#import simpleaudio as sa




def setup_logger(name, log_file, level=logging.INFO):
    """
    Function to set up a logger with the given name and log file.
    """
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    logger.addHandler(stream_handler)
    logger.propagate = False
    return logger

# Initialize logger
logger = setup_logger('client_mic', 'logs/client_mic.log')

class AudioManager:
    """
    This class loads and plays audio files for the client_mic application.
    It uses TTS to say text when no audio files are available.
    """
    def __init__(self, output_device, audio_folder_path):
        logger.info("Initializing AudioManager.")
        self.output_device = output_device
        self.audio_folder_path = audio_folder_path

        # Indicate the names of the audio files for each talk move
        self.talk_moves = {
            'cognitive': ["cog1", "cog2", "cog3" ],
            'metacognitive': ["meta1", "meta2", "meta3", "meta4" ],
            'behavioral': ["behav1", "behav2", "behav3", "behav4" ],
            'socio_emotional': ["emo1", "emo2", "emo3", "emo4" ],
            'shared_perspective': ["shared1", "shared2", "shared3" ],
        }

        # Read audio files
        self.audio = {}
        for talk_move in self.talk_moves:
            self.audio[talk_move] = {}
            for variation in self.talk_moves[talk_move]:
                self.audio[talk_move][variation] = sf.read(f"{self.audio_folder_path}/{variation}.wav") # FYI sf.read returns a tuple (audio, fs)

        # Initialize Text-to-Speech (tts) engine
        self.tts = pyttsx3.init()
        self.tts.setProperty('rate', 130)    # Speed percent (can go over 100)
        self.tts.setProperty('volume', 1)    # Volume 0-1
        self.tts.setProperty('voice', 'HKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\Speech\\Voices\\Tokens\\TTS_MS_EN-US_ZIRA_11.0')
        logger.info("Text-to-Speech engine initialized (used when audio files are not available).")

    def play_audio(self, talk_move):
        logger.info(f"Attempting to play audio for talk_move: {talk_move}")
        try:
            variation = random.choice(self.talk_moves[talk_move])
            sd.play(self.audio[talk_move][variation][0], self.audio[talk_move][variation][1], 
                    device=self.output_device)
            sd.wait()
            logger.info(f"Played audio: {variation}")
        except Exception as e:
            logger.error(f"Audio file not found for the talk_move '{talk_move}': {e}", exc_info=True)

    def say(self, text):
        logger.info(f"TTS saying: {text}")
        self.tts.say(text)

    def runAndWait(self):
        logger.debug("Running TTS engine.")
        self.tts.runAndWait()

def select_audio_devices():
    """
    This function selects the input and output audio devices via user input.
    """
    print("Available input devices:")
    input_devices = sd.query_devices()
    connected_input_devices = [device for device in input_devices if device['max_input_channels'] > 0]
    for i, device in enumerate(connected_input_devices):
        print(f"{i}: {device['name']}")
    input_device_index = int(input("Select input device by index: "))
    input_device = connected_input_devices[input_device_index]['name']
    print("Available output devices:")
    connected_output_devices = [device for device in input_devices if device['max_output_channels'] > 0]
    for i, device in enumerate(connected_output_devices):
        print(f"{i}: {device['name']}")
    output_device_index = int(input("Select output device by index: "))
    output_device = connected_output_devices[output_device_index]['name']
    return input_device, output_device

def listen_server(ws, audio_manager, should_continue):
    """
    This function listens to the server and plays the audio response.
    """
    logger.info("Listening to server...")
    try:
        while should_continue.is_set():
            # Receive message from websocket server
            output = ws.recv()
            logger.debug(f"Received raw output: {output}")
            # Transform output to dictionary
            output = json.loads(output)
            logger.info(f"Received output: {output}")

            # Check if there is a response
            if output.get('response'):
                logger.info("Output contains 'response'. Attempting to play audio.")
                # Play the output response as audio
                try:
                    audio_manager.play_audio(output['selected_move'])  
                except Exception as e:
                    logger.warning(f"Audio file not found for response '{output['response']}': {e}")
                    # Optionally use TTS as fallback
                    # audio_manager.say(output['response'])
                    # audio_manager.runAndWait()
            elif 'test test' in output.get('transcription', '').lower().replace(",", "").replace(".", ""):
                logger.info("Transcription contains 'test test'. Playing fallback audio.")
                try:
                    audio_manager.play_audio('issue_conceptual_understanding')  
                except Exception as e:
                    logger.warning(f"Failed to play fallback audio: {e}")
    except Exception as e:
        logger.error(f"Error while receiving message: {e}", exc_info=True)

def connect_and_stream(host, port, input_device, audio_manager, pipeline_step, mic_sample_rate):
    """
    This function connects to the server and streams the microphone audio to the server.
    """
    logger.info(f"Connecting to server at ws://{host}:{port}")
    ws = WebSocket()
    should_continue = threading.Event()
    should_continue.set()
    audio_source = None
    listener_thread = None

    try:
        ws.connect(f"ws://{host}:{port}")
        logger.info(f"Connected to server at ws://{host}:{port}")

        listener_thread = threading.Thread(target=listen_server, args=(ws, audio_manager, should_continue), daemon=True)
        listener_thread.start()

        audio_source = MicrophoneAudioSource(mic_sample_rate, block_size=int(pipeline_step * mic_sample_rate), 
                                             device=input_device)
        audio_source.stream.pipe(
            ops.map(encode_audio)
        ).subscribe_(ws.send)
        logger.info("Microphone audio source is now streaming to the server...")
        audio_source.read()

    except WebSocketException as e:
        logger.error(f"WebSocket error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return False
    finally:
        logger.info("Closing connection and cleaning up...")
        should_continue.clear()
        if ws.connected:
            ws.close()
        if listener_thread:
            listener_thread.join()
        if audio_source:
            audio_source.close()
    
    return True


if __name__ == "__main__":

    # Load environment variables
    dotenv.load_dotenv()
    HOST = os.environ.get("HOST")
    PORT = int(input("Enter the port number: "))
    PIPELINE_STEP = float(os.environ.get("PIPELINE_STEP"))
    MIC_SAMPLE_RATE = int(os.environ.get("MIC_SAMPLE_RATE"))
    AUDIO_FOLDER_PATH = os.environ.get("AUDIO_FOLDER_PATH").replace("\\", "/").rstrip('/')
    CLAIR_TOKEN = os.environ.get("CLAIR_TOKEN")

    # Select audio devices via user input
    input_device, output_device = select_audio_devices()
    
    # Initialize AudioManager
    audio_manager = AudioManager(output_device, AUDIO_FOLDER_PATH)

    # Start the client_mic application
    while True:
        try:
            logger.info(f"Starting client_mic application - Group id: {CLAIR_TOKEN}.")
            connect_and_stream(HOST, PORT, input_device, audio_manager, PIPELINE_STEP, MIC_SAMPLE_RATE)
            logger.info("Connection failed. Waiting 10 seconds before trying to reconnect...")
            time.sleep(10)
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received. Exiting the program.")
            break
    logger.info("Client_mic shutdown complete.")