# Use an official NVIDIA CUDA base image with Python 3.10
FROM nvidia/cuda:11.7.1-cudnn8-runtime-ubuntu20.04

# Set environment variables to prevent interactive prompts during installation
ENV DEBIAN_FRONTEND=noninteractive

# Install essential system packages
RUN apt-get update && apt-get install -y \
    wget \
    build-essential \
    libffi-dev \
    libsndfile1 \
    libasound2-dev \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Install Miniconda
ENV CONDA_DIR=/opt/conda
RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh && \
    bash ~/miniconda.sh -b -p $CONDA_DIR && \
    rm ~/miniconda.sh

# Update PATH environment variable
ENV PATH=$CONDA_DIR/bin:$PATH

# Create a new Conda environment named 'voice' with Python 3.9
RUN conda create --name voice python=3.9 -y

# Activate the 'voice' environment and install dependencies
SHELL ["conda", "run", "-n", "voice", "/bin/bash", "-c"]

# Install Conda dependencies
RUN conda install -y \
    portaudio=19.6.0 \
    pysoundfile=0.12.1 \
    ffmpeg=4.3 \
    -c conda-forge

# Upgrade pip to a specific version
RUN pip install --upgrade pip==23.0.1

# Install pip dependencies with specific versions
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install PyTorch with CUDA support
RUN pip install torch==1.13.1+cu117 torchaudio==0.13.1+cu117 --index-url https://download.pytorch.org/whl/cu117

# Set working directory to /app
WORKDIR /app

# Create logs directory
RUN mkdir -p logs

# Copy the server script and source modules
COPY server.py .
COPY source/ source/

# Copy the .env file (ensure it doesn't contain sensitive information if using version control)
COPY .env .

# Expose the necessary port (default will be overridden in docker-compose)
EXPOSE 5000

# Define the default command to run the server script within the Conda environment
CMD ["conda", "run", "--no-capture-output", "-n", "voice", "python", "server.py"]