# ModelOps - Template for DAPM (Dynamic and Parallel MLOps) framework

This repository implements ModelOps with DAPM (Dynamic and Parallel MLOps) framework

## Prerequisite to run batch framework

1. ETL Athena DB (dq_table) and S3 bucket.
2. Sample data path in [training_job/preprocessing](model/training_job/preprocessing/preprocessing.py)

## Workflow

1. Update environment json file. ex: [env/dev.tfvars.json](env/dev.tfvars.json). All folder paths must be relative from root of repository.

2. Configure seed job in jenkins. Follow [instructions](jenkins/Readme.md) in jenkins folder.

3. Deploy infra from [infra/terraform](infra/terraform/) using Jenkins `terraform-infra-deploy pipeline`.

    To try locally for dev environment

    ```shell
    terraform plan -var-file=../../env/dev.tfvars.json
    terraform apply -var-file=../../env/dev.tfvars.json
    ```

4. Sync model code with ECR and Glue job by running `aws-model-code-sync pipeline`.

5. Trigger training step-function by running `ModelOps-model-training pipeline`.

6. Approve model from sagemaker studio to execute inference. Alternatively inference will also run on scheduled basis.

## Branching Strategy

1. `main` brach represents development environment for trunk based development.
2. promote code to `production` branch from `main` branch with `PR` to promote code to production environment.
