def jenkins_role = "Cross-Account-role'"
def environment_mapping = [
    "master" : "dev",
    "uat" : "uat",
    "prod": "production"
]
def target_env = environment_mapping[BRANCH_NAME]
def tf_config_file = "env/${target_env}.tfvars.json"
def regex = /(production|master|uat)/

pipeline {
    agent any

    parameters {
        choice(
            choices: ['plan', 'apply', 'show', 'preview-destroy', 'destroy'],
            description: 'Terraform action to apply',
            name: 'action'
        )
    }

    environment {
        ENV_TF_VARS = "$tf_config_file"
        PYTHONDONTWRITEBYTECODE=1
    }

    stages {

        stage('Initialise terraform directory') {
            steps{

                sh 'ls -ltr'

                script {
                    try {
                        def config = readJSON file: tf_config_file
                    } catch (Exception e) {
                        error("Cannot read config file.\nError:\n${e}")
                    }
                }

                dir('infra') {
                    withAWS(roleAccount:'731580992380', role:'Cross-Account-role')  
                       {
                        sh 'terraform init --upgrade -reconfigure -no-color -backend-config="key=msil-mvp-tfstate/dapm-terraform.tfstate"'
                        sh 'terraform validate'
                      
                }
            }
        }
    }

        stage('Terraform Plan') {
            when {
                expression { params.action == 'plan' || params.action == 'apply' }
            }
            steps{
                sh 'printenv'
                dir('infra') {
                     withAWS(roleAccount:'731580992380', role:'Cross-Account-role') 
                     {
                    sh 'terraform plan -input=false -lock=false -out=tfplan --var-file="../$ENV_TF_VARS"'
                     }
                }
            }
        }

        stage('Approval') {
            when {
                allOf {
                    expression { params.action == 'apply' }
                    expression { BRANCH_NAME ==~ regex }
                }
            }
            steps {
                dir('infra') {
                    withAWS(roleAccount:'731580992380', role:'Cross-Account-role')
                    {
                    sh 'terraform show -no-color tfplan > tfplan.txt'

                    script {
                        def plan = readFile 'tfplan.txt'
                        input message: "Apply the plan?",
                        parameters: [text(name: 'Plan', description: 'Please review the plan', defaultValue: plan)]
                    }
                }
                }
            }
        }

        stage('Terraform Apply - All') {
            when {
                allOf{
                    expression { params.action == 'apply' }
                    expression { BRANCH_NAME ==~ regex }
                }
            }

            steps {
                dir('infra') {
                    withAWS(roleAccount:'731580992380', role:'Cross-Account-role') 
                    {
                    sh '''
                        terraform show -json tfplan  > tfplan.json

                        glue_wf_exists=glue_values=`cat tfplan.json | jq '[.resource_changes[] | select( .module_address != null) | select(.module_address | contains("glue_wf"))] | [.[].module_address] | unique | length'`

                        if [ $((glue_wf_exists)) -ne 0 ]
                        then
                            echo "Found Glue workflow components. Initiating target deploy"
                            terraform apply --var-file="../$ENV_TF_VARS" -target="module.analytics_etl.module.glue_wfs" -parallelism=1 -auto-approve

                            echo "Applying overall plan"
                            terraform apply --var-file="../$ENV_TF_VARS" -auto-approve
                        else
                            terraform apply -no-color -lock=false -input=false tfplan
                        fi
                    '''
                }
             }
            }
        }

        stage('Preview-Destroy') {
            when {
                expression { params.action == 'preview-destroy' || params.action == 'destroy' }
            }
            steps {
                dir('infra') {
                    sh 'terraform plan -destroy -out=tfplan -lock=false --var-file="../$ENV_TF_VARS"'
                    sh 'terraform show  tfplan > tfplan.txt'
                }
            }
        }

        stage('Destroy') {
            when {
                allOf {
                    expression { params.action == 'destroy' }
                    expression { BRANCH_NAME ==~ regex }
                }
            }
            steps {
                dir('infra') {
                    script {
                        def plan = readFile 'tfplan.txt'
                        input message: "Delete the stack?",
                        parameters: [text(name: 'Plan', description: 'Please review the plan', defaultValue: plan)]
                    }

                    sh 'terraform destroy -no-color -auto-approve -lock=false  --var-file="../$ENV_TF_VARS"'

                }
            }
        }
    }
    post {
        always {
            cleanWs disableDeferredWipeout: true, deleteDirs: true
        }
    }
}
