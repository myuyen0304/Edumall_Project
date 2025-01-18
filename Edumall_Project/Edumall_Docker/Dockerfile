FROM python:3

WORKDIR /usr/src/app

COPY requirements.txt ./

RUN pip install --trusted-host pypi.python.org -r requirements.txt

# RUN pip3 install --no-cache-dir -r requirements.txt

RUN apt-get update && apt-get install -y wget unzip && \
    wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt install -y ./google-chrome-stable_current_amd64.deb && \
    rm google-chrome-stable_current_amd64.deb && \
    apt-get clean
    
COPY . .

# During debugging, this entry point will be overridden. For more information, please refer to https://aka.ms/vscode-docker-python-debug
CMD ["python", "edumall_docker.py"]
