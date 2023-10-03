FROM python:3.9-slim-buster

ARG CONTEXT_NAME=inferencing

WORKDIR /opt/ml

ADD model/${CONTEXT_NAME}_job/${CONTEXT_NAME} /opt/ml
ADD utils /opt/ml/utils

RUN pip3 install --upgrade -r /opt/ml/requirements.txt

ENV PYTHONUNBUFFERED=TRUE
ENV PYTHONDONTWRITEBYTECODE=TRUE

ENTRYPOINT ["python3","/opt/ml/inferencing.py"]

# Build docker file from root folder to get full context so that it can copy utils folder as well


# docker build -f model/inferencing_job/inferencing/Dockerfile . -t 731580992380.dkr.ecr.ap-south-1.amazonaws.com/msil-inference:latest
# docker run -v ~/.aws:/root/.aws 731580992380.dkr.ecr.ap-south-1.amazonaws.com/msil-inference:latest  --s3_inferencing_data_input_path    --s3_inferencing_prefix_output_path  --pk_id  --prev_batch_job_id  --inference_metatable_name  -mapping_id  --region ap-south-1
# docker push 731580992380.dkr.ecr.ap-south-1.amazonaws.com/msil-inference:latest
# docker run -v ~/.aws:/root/.aws 731580992380.dkr.ecr.ap-south-1.amazonaws.com/msil-inference:latest  --s3_inferencing_data_input_path  s3://msil-poc-apsouth1-shared/preprocessing/year=2023/month=11/day=01/stepjobid=MSIL-POC-Inferencing-State-Machine-d4c71ee2-6795-4e1c-b27e-011a27b66b2e/smid=MSIL-POC-Inferencing-State-Machine-d4c71ee2-6795-4e1c-b27e-011a27b66b2e/pk=Part1/mapping=rajasthan/    --s3_inferencing_prefix_output_path s3://msil-poc-apsouth1-shared/inferencedummy/   --pk_id Part1   --prev_batch_job_id 5679fb3c-1adf-4323-b82b-da8a09e4025a  --inference_metatable_name inferencemetatable  --mapping_id  rajasthan  --region ap-south-1
