FROM python:3.9-slim-buster

ARG CONTEXT_NAME=training

WORKDIR /opt/ml

ADD model/${CONTEXT_NAME}_job/preprocessing /opt/ml
ADD utils /opt/ml/utils

RUN pip3 install --upgrade -r /opt/ml/requirements.txt

ENV PYTHONUNBUFFERED=TRUE
ENV PYTHONDONTWRITEBYTECODE=TRUE

ENTRYPOINT ["python3","/opt/ml/preprocessing.py"]

# Build docker file from model folder to get full context so that it can copy utils folder as well

# cd model
# docker build -f preprocessing/Dockerfile . -t msil-preprocessing
# docker tag msil-preprocessing:latest 731580992380.dkr.ecr.ap-south-1.amazonaws.com/msil-preprocessing:latest
# docker run -v ~/.aws:/root/.aws 731580992380.dkr.ecr.ap-south-1.amazonaws.com/msil-preprocessing:latest   --train_metatable_name    trainmetatable   --region ap-south-1
# docker push 731580992380.dkr.ecr.ap-south-1.amazonaws.com/msil-preprocessing:latest