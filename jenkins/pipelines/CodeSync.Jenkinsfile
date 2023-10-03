def jenkins_role = "Jenkins-AssumeRole-ProServe-CrossAccount-Role"
def environment_mapping = [
    "main" : "dev",
    "uat" : "uat",
    "prod": "production"
]
def target_env = environment_mapping[BRANCH_NAME]
def tf_config_file = "env/${target_env}.tfvars.json"
def regex = /(production|main|uat)/

def aws_account_number = null
def region = null
def config = null
def training_params = null
def inferencing_params = null


pipeline {

    agent {
        label 'ace_analytics'
    }

    options {
        ansiColor('xterm')
        disableConcurrentBuilds()
    }

    environment {
        GIT_COMMIT_HASH = sh(script: 'git rev-parse --short HEAD', returnStdout: true).trim()
        PYTHONPATH="${env.WORKSPACE}:$PYTHONPATH"
        ENV_TF_VARS = "$tf_config_file"
        PYTHONDONTWRITEBYTECODE=1
    }

    stages {

        stage ('Initialization') {
            when {
                expression { BRANCH_NAME ==~ regex }
            }
            steps {
                script {
                    sh 'ls -ltr'
                    sh 'python3 -m pip install boto3'
                    try {
                        config = readJSON file: tf_config_file
                        aws_account_number = config['account_number'].toString()
                        region = config['region']
                    } catch (Exception e) {
                        error("Cannot read config file.\nError:\n${e}")
                    }
                }
            }
        }
        stage ('SonarQube analysis') {
            steps {
                script {
                  def scannerHome = tool 'sonarqube'
                  withSonarQubeEnv("sonarv2") {
                     sh "${scannerHome}/bin/sonar-scanner"
                  }
                }
            }
        }

        stage ('Sync Utils & Common') {
            when {
                expression { BRANCH_NAME ==~ regex }
                anyOf {
                    changeset "model/**"
                    changeset "analytics-etl/**"
                    changeset "utils/**"
                }
            }
            steps {
                withAWS(roleAccount: aws_account_number, role: jenkins_role, region: region){

                    script {
                        s3_names = readJSON text: sh(script: 'jenkins/scripts/export_s3_names.py --env-file $ENV_TF_VARS', returnStdout: true)
                        env.TRAINING_S3_INTERNAL = s3_names['training_internal_bucket']
                        env.INFERENCING_S3_INTERNAL = s3_names['inferencing_internal_bucket']
                        env.ANALYTICS_S3_INTERNAL = s3_names['analytics_etl_bucket']
                    }

                    sh '''
                        printenv

                        echo "*********** Syncing Common Files ***********"

                        echo "*********** Mapping Json ***********"
                        aws s3 cp model/common/mapping.json s3://${TRAINING_S3_INTERNAL}/mapping_json/mapping_json.json

                        echo "*********** Syncing Utils With Training Bucket ***********"
                        aws s3 sync utils/ s3://${TRAINING_S3_INTERNAL}/src/python/utils/

                        echo "*********** Syncing Utils With Inferencing Bucket ***********"
                        aws s3 sync utils/ s3://${INFERENCING_S3_INTERNAL}/src/python/utils/

                        echo "*********** Syncing Utils With Analytics Bucket ***********"
                        aws s3 sync utils/ s3://${ANALYTICS_S3_INTERNAL}/src/python/utils/

                        echo "*********** Syncing Utils .. Done! ***********"
                    '''
                }
            }
        }

        stage ('Sync Analytics ETL code') {
            when {
                expression { BRANCH_NAME ==~ regex }
                changeset "analytics-etl/**"
            }
            steps {
                withAWS(roleAccount: aws_account_number, role: jenkins_role, region: region) {
                    sh "aws s3 sync analytics-etl s3://${ANALYTICS_S3_INTERNAL}/src/python/analytics-etl"
                }
            }
        }

        stage ('Sync Monitoring code') {
            when {
                expression { BRANCH_NAME ==~ regex }
                changeset "**/monitoring/**"
            }
            steps {
                withAWS(roleAccount: aws_account_number, role: jenkins_role, region: region) {
                    sh "python3 jenkins/scripts/sync_code.py --context monitoring --env-file $ENV_TF_VARS"
                }
            }
        }

        stage ('Sync Training code') {
            when {
                expression { BRANCH_NAME ==~ regex }
                anyOf {
                changeset "model/training_job/**"
                changeset "utils/**"
                }
            }
            steps {
                withAWS(roleAccount: aws_account_number, role: jenkins_role, region: region) {
                    sh "python3 jenkins/scripts/sync_code.py --context training --env-file $ENV_TF_VARS"
                }
            }
        }

        stage ('Sync Inference code') {
            when {
                expression { BRANCH_NAME ==~ regex }
                anyOf {
                changeset "model/inferencing_job/**"
                changeset "utils/**"
                }
            }
            steps {
                withAWS(roleAccount: aws_account_number, role: jenkins_role, region: region) {
                    sh "python3 jenkins/scripts/sync_code.py --context inferencing --env-file $ENV_TF_VARS"
                }
            }
        }
    }
    post {
        // Clean after build
        always {
            cleanWs disableDeferredWipeout: true, deleteDirs: true
        }
    }
}
