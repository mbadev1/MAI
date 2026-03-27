# Clair-MAI

Documentation under construction...

## Transcription and diarization with a remote microphone


### Installation steps


#### Docker setup

```
docker build -t clair-mai:0.2 -f clair_mai.Dockerfile .
docker-compose up -d
docker logs -f clair_mai_5000
docker restart clair_mai_5000
```

#### Local setup

```
conda create --name <name_your_env> python=3.10
conda activate <name_your_env>
conda install portaudio=19.6.0 pysoundfile=0.12.1 ffmpeg=4.3 -c conda-forge
pip install python-dotenv==1.0.1 termcolor==2.4.0 diart==0.7 whisper_timestamped==1.12.20 onnxruntime==1.17.1 pydub==0.25.1 simpleaudio==1.0.4 pyttsx3==2.90
```

Additional GPU setup (if applicable to your machine) to enable `device="cuda"`. Otherwise, use `device="cpu"` instead in your decoder code.

```
pip install torch==1.13.1+cu117 torchaudio==0.13.1+cu117 --index-url https://download.pytorch.org/whl/cu117
```

### Usage

1. Check `.env` for the preferred settings.
2. Run `server.py` on the main server.
3. Run `client_mic.py` on the client machine with a microphone.
4. Check the output displayed on the decoder.
