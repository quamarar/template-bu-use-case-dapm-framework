def jenkins_role = "Jenkins-AssumeRole-ProServe-CrossAccount-Role"
def environment_mapping = [
    "main" : "dev",
    "uat" : "uat",
    "prod": "production"
]
def target_env = environment_mapping[BRANCH_NAME]
def tf_config_file = "env/${target_env}.tfvars.json"


def aws_account_number = null
def region = null

pipeline {
    agent {
        label 'ace_analytics'
    }

    options {
        ansiColor('xterm')
        disableConcurrentBuilds()
    }

    parameters {
        choice(
            choices: ['training', 'inferencing'],
            description: 'Step function Context',
            name: 'context'
        )
    }

    triggers {
        parameterizedCron('''
            # leave spaces where you want them around the parameters. They'll be trimmed.
            # we let the build run with the default name
            0 0 27 * * %context=inferencing
            
        ''')
    }

    environment {
        ENV_TF_VARS = "$tf_config_file"
        PYTHONPATH="${env.WORKSPACE}:$PYTHONPATH"
    }

    stages {

        stage ('Initialization') {
            steps {
                script {
                    try {
                        config = readJSON file: tf_config_file
                        aws_account_number = config['account_number'].toString()
                        region = config['region']
                    } catch (Exception e) {
                        error("Cannot read config file.\nError:\n${e}")
                    }
                }
                sh 'python3 -m pip install boto3'
            }
        }

        stage('Invoking step function'){
            steps{
                sh 'printenv'
                withAWS(roleAccount: aws_account_number, role: jenkins_role, region:region) {
                    sh "python3 jenkins/scripts/step_function_executioner.py --context ${params.context} --env-file $ENV_TF_VARS"
                }
            }
        }
    }
}
