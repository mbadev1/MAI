"""
Unified ASIO client – streams multiple ASIO channel groups to their respective
servers using a single shared ASIO device session.

Replaces the need to run separate instances of asio_client_mic.py (one per group),
which fails because ASIO drivers are single-client and reject a second open.

* One SharedAsioInput opens the ASIO device once, capturing all channels needed
  by every group.  Its callback demuxes audio blocks into per-group queues.
* Each group thread reads from its own queue, encodes, and streams to its server
  via websocket – exactly like the original per-group script.
* Audio playback (responses) is serialised with a global lock so that sd.play()
  never races on the shared output device.

Config:
    unified_groups.json  – array of group definitions (server, channels,
                           per-group audio_folder_path).
    .env                 – shared parameters (PIPELINE_STEP, STREAM_SAMPLE_RATE,
                           LOG_INPUT_LEVELS, AUDIO_FOLDER_PATH as fallback).
"""

import os
import json
import random
import logging
import sys
import ctypes
import socket
import queue
import rx.operators as ops
from websocket import WebSocket, WebSocketException
from diart.utils import encode_audio
import dotenv
import threading
import pyttsx3
import soundfile as sf
import time
import numpy as np
from scipy.signal import resample_poly
from rx.subject import Subject

# Set environment variable before importing sounddevice.
os.environ["SD_ENABLE_ASIO"] = "1"

import sounddevice as sd


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logger(name, log_file, level=logging.INFO):
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s')
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


logger = setup_logger('unified_client', 'logs/unified_client.log')

# Global lock – serialises sd.play() so multiple group threads never fight
# over the ASIO output device simultaneously.
PLAYBACK_LOCK = threading.Lock()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def is_asio_driver_load_error(error):
    return 'failed to load asio driver' in str(error).lower()


def initialize_com_for_current_thread():
    coinit_apartment_threaded = 0x2
    ole32 = ctypes.windll.ole32
    hr = ole32.CoInitializeEx(None, coinit_apartment_threaded)
    if hr not in (0, 1):
        logger.warning(f"COM initialization returned HRESULT {hr}")
    return hr


def uninitialize_com_for_current_thread():
    ctypes.windll.ole32.CoUninitialize()


def can_reach_server(host, port, timeout_seconds=3):
    try:
        with socket.create_connection((host, int(port)), timeout=timeout_seconds):
            return True, None
    except OSError as e:
        return False, e


def drain_queue(q):
    """Remove all pending items from a queue."""
    while True:
        try:
            q.get_nowait()
        except queue.Empty:
            break


# ---------------------------------------------------------------------------
# Shared ASIO input – one InputStream for ALL groups
# ---------------------------------------------------------------------------

class SharedAsioInput:
    """
    Opens a single ASIO InputStream that captures every channel required by
    any group, then distributes audio blocks into per-group queues.
    """

    MAX_QUEUE_SIZE = 500  # drop oldest block when a consumer falls behind

    def __init__(self, device, all_channels_1based, sample_rate, block_size):
        self.all_channels = sorted(set(all_channels_1based))
        self.sample_rate = sample_rate
        self.block_size = block_size
        self.device = device

        # Map 1-based physical channel → column index in callback indata
        self._channel_to_col = {
            ch: i for i, ch in enumerate(self.all_channels)
        }

        self._subscribers = {}  # name → (col_indices_list, Queue)
        self._lock = threading.Lock()

        selectors = [ch - 1 for ch in self.all_channels]
        self._stream = sd.InputStream(
            channels=len(selectors),
            samplerate=sample_rate,
            latency=0,
            blocksize=block_size,
            callback=self._callback,
            device=device,
            extra_settings=sd.AsioSettings(channel_selectors=selectors),
        )

    def add_group(self, name, channels_1based):
        """Register a group and return the Queue it should read from."""
        cols = [self._channel_to_col[ch] for ch in channels_1based]
        q = queue.Queue(maxsize=self.MAX_QUEUE_SIZE)
        with self._lock:
            self._subscribers[name] = (cols, q)
        return q

    # -- audio callback (runs in real-time thread – keep fast) -------------

    def _callback(self, indata, frames, time_info, status):
        if status:
            logger.warning(f"SharedAsioInput status: {status}")
        with self._lock:
            for _name, (cols, q) in self._subscribers.items():
                chunk = indata[:, cols].copy()
                try:
                    q.put_nowait(chunk)
                except queue.Full:
                    # Drop oldest block so the consumer always sees fresh audio
                    try:
                        q.get_nowait()
                    except queue.Empty:
                        pass
                    try:
                        q.put_nowait(chunk)
                    except queue.Full:
                        pass

    def start(self):
        self._stream.start()

    def close(self):
        self._stream.stop()
        self._stream.close()


# ---------------------------------------------------------------------------
# Per-group audio source (reads from SharedAsioInput queue)
# ---------------------------------------------------------------------------

class GroupAudioSource:
    """
    Pulls audio blocks from a SharedAsioInput queue, down-mixes to mono,
    resamples to the target rate, and publishes on an rx Subject – the same
    interface that the websocket streaming pipeline expects.
    """

    def __init__(self, shared_queue, input_channels, capture_sr, output_sr,
                 block_duration, group_name, log_input_levels=False):
        self.input_channels = input_channels
        self.capture_sr = int(capture_sr)
        self.output_sr = int(output_sr)
        self.output_block_size = int(block_duration * self.output_sr)
        self.group_name = group_name
        self.log_input_levels = log_input_levels
        self._queue = shared_queue
        self._running = True
        self.stream = Subject()
        self._blocks_seen = 0
        self._level_interval = max(1, int(round(2.0 / block_duration)))

    def read(self):
        """Blocking loop – pull, process, push to rx Subject."""
        while self._running:
            try:
                data = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue

            # Down-mix to mono
            if data.ndim == 1 or data.shape[1] == 1:
                mono = data[:, 0] if data.ndim > 1 else data
            else:
                mono = data.mean(axis=1)

            # Resample if the ASIO capture rate differs from stream rate
            if self.capture_sr != self.output_sr:
                mono = resample_poly(
                    mono, self.output_sr, self.capture_sr)

            # Pad / trim to exact expected block length
            if mono.shape[0] > self.output_block_size:
                mono = mono[:self.output_block_size]
            elif mono.shape[0] < self.output_block_size:
                mono = np.pad(
                    mono,
                    (0, self.output_block_size - mono.shape[0]),
                    mode='constant',
                )

            if self.log_input_levels:
                self._blocks_seen += 1
                if self._blocks_seen % self._level_interval == 0:
                    rms = float(np.sqrt(
                        np.mean(np.square(mono, dtype=np.float64))))
                    peak = float(np.max(np.abs(mono)))
                    logger.info(
                        f"[{self.group_name}] Input level "
                        f"(channels={self.input_channels}) "
                        f"rms={rms:.6f} peak={peak:.6f}"
                    )

            try:
                self.stream.on_next(mono[np.newaxis, :])
            except Exception as e:
                self.stream.on_error(e)
                break

        self.stream.on_completed()

    def close(self):
        self._running = False


# ---------------------------------------------------------------------------
# Audio playback manager (unchanged logic, added PLAYBACK_LOCK)
# ---------------------------------------------------------------------------

class AudioManager:
    def __init__(self, output_device, audio_folder_path, output_channels=None):
        logger.info("Initializing AudioManager.")
        self.output_device = output_device
        self.audio_folder_path = audio_folder_path
        if output_channels is None:
            self.output_channels = None
        elif isinstance(output_channels, int):
            self.output_channels = [output_channels]
        else:
            self.output_channels = list(output_channels)
        self.talk_moves = {
            'cognitive': ["cog1", "cog2", "cog3"],
            'metacognitive': ["meta1", "meta2", "meta3", "meta4"],
            'behavioral': ["behav1", "behav2", "behav3", "behav4"],
            'socio_emotional': ["emo1", "emo2", "emo3"],
            'shared_perspective': ["shared1", "shared2", "shared3"],
        }
        self.audio = {}
        for talk_move in self.talk_moves:
            self.audio[talk_move] = {}
            for variation in self.talk_moves[talk_move]:
                self.audio[talk_move][variation] = sf.read(
                    f"{self.audio_folder_path}/{variation}.mp3")
        self.tts = None

    def _ensure_tts(self):
        if self.tts is not None:
            return
        self.tts = pyttsx3.init()
        self.tts.setProperty('rate', 130)
        self.tts.setProperty('volume', 1)
        self.tts.setProperty('voice',
            'HKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\Speech\\Voices\\'
            'Tokens\\TTS_MS_EN-US_ZIRA_11.0')
        logger.info("Text-to-Speech engine initialized.")

    def play_audio(self, talk_move):
        logger.info(f"Attempting to play audio for talk_move: {talk_move}")
        try:
            variation = random.choice(self.talk_moves[talk_move])
            data = self.audio[talk_move][variation][0]
            sr = self.audio[talk_move][variation][1]
            if data.dtype == np.int16:
                data = data.astype(np.float32) / 32768.0
            if len(data.shape) > 1:
                data = data.mean(axis=1)

            if (self.output_channels
                    and len(self.output_channels) > 1
                    and data.ndim == 1):
                data = np.tile(
                    data[:, np.newaxis], (1, len(self.output_channels)))

            play_kwargs = {
                'device': self.output_device,
                'blocksize': 4096,
            }
            if self.output_channels:
                play_kwargs['mapping'] = self.output_channels

            with PLAYBACK_LOCK:
                sd.play(data, sr, **play_kwargs)
                sd.wait()
            logger.info(f"Played audio: {variation}")
        except Exception as e:
            logger.error(
                f"Audio file not found for the talk_move '{talk_move}': {e}",
                exc_info=True)

    def say(self, text):
        self._ensure_tts()
        self.tts.say(text)

    def runAndWait(self):
        self._ensure_tts()
        self.tts.runAndWait()


# ---------------------------------------------------------------------------
# Server listener (unchanged from original)
# ---------------------------------------------------------------------------

def listen_server(ws, audio_manager, group_name, should_continue):
    logger.info(f"[{group_name}] Listening to server...")
    try:
        while should_continue.is_set():
            output = ws.recv()
            output = json.loads(output)
            logger.info(f"[{group_name}] Received output: {output}")
            if output.get('response'):
                logger.info(
                    f"[{group_name}] Playing audio for: "
                    f"{output.get('selected_move')}")
                try:
                    audio_manager.play_audio(output['selected_move'])
                except Exception as e:
                    logger.warning(f"[{group_name}] Audio error: {e}")
            elif ('test test' in output.get('transcription', '')
                    .lower().replace(",", "").replace(".", "")):
                try:
                    audio_manager.play_audio(
                        'issue_conceptual_understanding')
                except Exception as e:
                    logger.warning(
                        f"[{group_name}] Fallback audio error: {e}")
    except Exception as e:
        logger.error(
            f"[{group_name}] Error while receiving message: {e}",
            exc_info=True)


# ---------------------------------------------------------------------------
# Per-group connection loop
# ---------------------------------------------------------------------------

def connect_and_stream(
    host, port, shared_queue, input_channels, capture_sr,
    audio_manager, pipeline_step, target_sr, group_name,
    log_input_levels=False,
):
    """
    Manage the websocket connection for one group.  Audio arrives via
    *shared_queue* (fed by SharedAsioInput); a fresh GroupAudioSource is
    created on each reconnection attempt so the rx pipeline is clean.
    """
    initialize_com_for_current_thread()
    try:
        while True:
            logger.info(
                f"[{group_name}] Connecting to server at "
                f"ws://{host}:{port}")
            reachable, connect_error = can_reach_server(host, port)
            if not reachable:
                logger.error(
                    f"[{group_name}] Server ws://{host}:{port} is "
                    f"unreachable ({connect_error}). Retrying in 10 s.")
                drain_queue(shared_queue)
                time.sleep(10)
                continue

            ws = WebSocket()
            should_continue = threading.Event()
            should_continue.set()
            audio_source = None
            listener_thread = None

            try:
                # Discard stale audio accumulated while disconnected
                drain_queue(shared_queue)

                audio_source = GroupAudioSource(
                    shared_queue=shared_queue,
                    input_channels=input_channels,
                    capture_sr=capture_sr,
                    output_sr=target_sr,
                    block_duration=pipeline_step,
                    group_name=group_name,
                    log_input_levels=log_input_levels,
                )

                ws.connect(f"ws://{host}:{port}")
                logger.info(
                    f"[{group_name}] Connected to ws://{host}:{port}")

                listener_thread = threading.Thread(
                    target=listen_server,
                    args=(ws, audio_manager, group_name, should_continue),
                    daemon=True,
                )
                listener_thread.start()

                audio_source.stream.pipe(
                    ops.map(encode_audio)
                ).subscribe_(ws.send)
                logger.info(
                    f"[{group_name}] Streaming microphone audio...")
                audio_source.read()

            except WebSocketException as e:
                logger.error(f"[{group_name}] WebSocket error: {e}")
            except Exception as e:
                logger.error(
                    f"[{group_name}] Unexpected error: {e}",
                    exc_info=True)
            finally:
                logger.info(f"[{group_name}] Closing connection...")
                should_continue.clear()
                if ws.connected:
                    ws.close()
                if listener_thread:
                    listener_thread.join(timeout=2)
                if audio_source:
                    audio_source.close()

            logger.info(
                f"[{group_name}] Reconnecting in 10 seconds...")
            time.sleep(10)
    finally:
        uninitialize_com_for_current_thread()


# ---------------------------------------------------------------------------
# Config loading & ASIO discovery
# ---------------------------------------------------------------------------

def load_groups_config():
    config_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'unified_groups.json',
    )
    if not os.path.exists(config_path):
        logger.error(f"unified_groups.json not found at {config_path}")
        sys.exit(1)
    with open(config_path, 'r') as f:
        config = json.load(f)
    return config['groups']


def find_asio_api_index(host_apis):
    exact = [
        i for i, api in enumerate(host_apis)
        if str(api.get('name', '')).strip().upper() == 'ASIO'
    ]
    if exact:
        return exact[0]
    contains = [
        i for i, api in enumerate(host_apis)
        if 'ASIO' in str(api.get('name', '')).upper()
    ]
    return contains[0] if contains else None


def negotiate_capture_rate(device, all_channels_1based, target_rate):
    """
    Try the target sample rate first, then common ASIO rates.
    Returns the first rate the driver accepts.
    """
    candidate_rates = [int(target_rate), 48000, 44100]
    selectors = [ch - 1 for ch in all_channels_1based]
    settings = sd.AsioSettings(channel_selectors=selectors)
    tried = []
    for rate in candidate_rates:
        if rate in tried:
            continue
        tried.append(rate)
        try:
            sd.check_input_settings(
                device=device,
                channels=len(all_channels_1based),
                samplerate=rate,
                extra_settings=settings,
            )
            if rate == target_rate:
                logger.info(
                    f"Shared ASIO capture rate: {rate} Hz")
            else:
                logger.warning(
                    f"ASIO device does not support {target_rate} Hz. "
                    f"Capturing at {rate} Hz and resampling.")
            return rate
        except Exception as e:
            logger.warning(f"ASIO check failed at {rate} Hz: {e}")
            if is_asio_driver_load_error(e):
                raise
    raise RuntimeError(
        f"Failed to open shared ASIO stream at any rate (tried {tried}).")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":

    dotenv.load_dotenv()
    PIPELINE_STEP = float(os.environ.get("PIPELINE_STEP", "1"))
    STREAM_SAMPLE_RATE = int(os.environ.get("STREAM_SAMPLE_RATE", "16000"))
    LOG_INPUT_LEVELS = (
        os.environ.get("LOG_INPUT_LEVELS", "1").strip().lower()
        in {"1", "true", "yes", "on"}
    )
    DEFAULT_AUDIO_FOLDER = (
        os.environ.get("AUDIO_FOLDER_PATH", "")
        .replace("\\", "/").rstrip('/')
    )

    logger.info(
        f"STREAM_SAMPLE_RATE={STREAM_SAMPLE_RATE} Hz, "
        f"PIPELINE_STEP={PIPELINE_STEP} s")

    # ---- Discover ASIO devices ----------------------------------------

    asio_api = None
    all_devices = []
    asio_index_to_global = {}

    for attempt in range(1, 4):
        host_apis = sd.query_hostapis()
        asio_api = find_asio_api_index(host_apis)
        if asio_api is None:
            api_names = [str(api.get('name', '')) for api in host_apis]
            logger.error(
                f"No ASIO host API found. Detected: {api_names}")
            sys.exit(1)

        all_devices = sd.query_devices()
        asio_index_to_global = {}
        asio_idx = 0
        for global_idx, d in enumerate(all_devices):
            if d['hostapi'] == asio_api:
                asio_index_to_global[asio_idx] = global_idx
                asio_idx += 1

        if asio_index_to_global:
            break

        logger.warning(
            f"No ASIO devices on attempt {attempt}/3. Retrying...")
        time.sleep(2)

    if not asio_index_to_global:
        logger.error("No ASIO devices found after retries.")
        sys.exit(1)

    print("\nAvailable ASIO audio devices "
          "(use these indices in unified_groups.json):")
    for a_idx, g_idx in asio_index_to_global.items():
        d = all_devices[g_idx]
        print(f"  [{a_idx}] {d['name']} "
              f"(in={d['max_input_channels']}, "
              f"out={d['max_output_channels']})")
    print()

    # ---- Load & validate groups ---------------------------------------

    groups = load_groups_config()
    logger.info(f"Loaded {len(groups)} group(s) from unified_groups.json")

    for group in groups:
        in_idx = group['input_device']
        out_idx = group['output_device']
        if in_idx not in asio_index_to_global:
            logger.error(
                f"[{group['name']}] input_device {in_idx} invalid. "
                f"Valid: {list(asio_index_to_global.keys())}")
            sys.exit(1)
        if out_idx not in asio_index_to_global:
            logger.error(
                f"[{group['name']}] output_device {out_idx} invalid. "
                f"Valid: {list(asio_index_to_global.keys())}")
            sys.exit(1)

        group['_global_input'] = asio_index_to_global[in_idx]
        group['_global_output'] = asio_index_to_global[out_idx]

        for key in ('input_channels', 'output_channels'):
            ch = group.get(key, [1])
            if isinstance(ch, int):
                ch = [ch]
            if not ch or not all(
                    isinstance(c, int) and c >= 1 for c in ch):
                logger.error(
                    f"[{group['name']}] {key} must be 1-based ints")
                sys.exit(1)
            group[key] = ch

        max_in = all_devices[group['_global_input']]['max_input_channels']
        max_out = all_devices[group['_global_output']]['max_output_channels']
        if max(group['input_channels']) > max_in:
            logger.error(
                f"[{group['name']}] input_channels exceed "
                f"device max ({max_in})")
            sys.exit(1)
        if max(group['output_channels']) > max_out:
            logger.error(
                f"[{group['name']}] output_channels exceed "
                f"device max ({max_out})")
            sys.exit(1)

        # Per-group audio folder (falls back to .env AUDIO_FOLDER_PATH)
        if not group.get('audio_folder_path'):
            group['audio_folder_path'] = DEFAULT_AUDIO_FOLDER

        logger.info(
            f"[{group['name']}] "
            f"input={in_idx}(global:{group['_global_input']}), "
            f"output={out_idx}(global:{group['_global_output']}), "
            f"in_ch={group['input_channels']}, "
            f"out_ch={group['output_channels']}, "
            f"audio={group['audio_folder_path']}")

    # ---- Build one SharedAsioInput per physical input device ----------
    # (All current groups share device 0, so there will be one stream.)

    device_groups = {}
    for group in groups:
        device_groups.setdefault(
            group['_global_input'], []).append(group)

    shared_inputs = {}   # global_device_idx → SharedAsioInput
    group_queues = {}    # group_name → Queue
    capture_rates = {}   # global_device_idx → negotiated sample rate

    for global_dev, dev_groups in device_groups.items():
        all_channels = sorted(set(
            ch for g in dev_groups for ch in g['input_channels']
        ))

        capture_rate = negotiate_capture_rate(
            global_dev, all_channels, STREAM_SAMPLE_RATE)
        capture_rates[global_dev] = capture_rate

        block_size = int(PIPELINE_STEP * capture_rate)
        shared = SharedAsioInput(
            global_dev, all_channels, capture_rate, block_size)
        shared_inputs[global_dev] = shared

        for g in dev_groups:
            q = shared.add_group(g['name'], g['input_channels'])
            group_queues[g['name']] = q
            logger.info(
                f"[{g['name']}] subscribed to shared input "
                f"(device={global_dev}, channels={g['input_channels']})")

    # Start all shared input streams
    for global_dev, shared in shared_inputs.items():
        shared.start()
        logger.info(
            f"Shared ASIO input started on device {global_dev} "
            f"(rate={capture_rates[global_dev]} Hz, "
            f"channels={shared.all_channels})")

    # ---- Launch one thread per group ----------------------------------

    threads = []
    for group in groups:
        global_dev = group['_global_input']
        capture_rate = capture_rates[global_dev]

        audio_folder = group['audio_folder_path']
        if not audio_folder or not os.path.isdir(audio_folder):
            logger.error(
                f"[{group['name']}] audio folder not found: "
                f"{audio_folder!r}")
            sys.exit(1)

        audio_mgr = AudioManager(
            group['_global_output'],
            audio_folder,
            output_channels=group['output_channels'],
        )

        t = threading.Thread(
            target=connect_and_stream,
            args=(
                group['server_ip'],
                group['port'],
                group_queues[group['name']],
                group['input_channels'],
                capture_rate,
                audio_mgr,
                PIPELINE_STEP,
                STREAM_SAMPLE_RATE,
                group['name'],
                LOG_INPUT_LEVELS,
            ),
            daemon=True,
        )
        t.start()
        threads.append(t)
        logger.info(f"Started thread for {group['name']}")
        time.sleep(0.25)

    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt. Shutting down...")

    for shared in shared_inputs.values():
        shared.close()
    logger.info("Unified client shutdown complete.")
