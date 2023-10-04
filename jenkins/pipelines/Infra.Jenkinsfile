
pipeline {
    agent any

    parameters {
        choice(
            choices: ['plan', 'apply', 'show', 'preview-destroy', 'destroy'],
            description: 'Terraform action to apply',
            name: 'action'
        )
    }


    stages {

        stage('Initialise terraform directory') {
            steps{
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
                    sh 'terraform plan -input=false -lock=false -out=tfplan --var-file=../env/dev.tfvars.json'
                     }
                }
            }
        }

        stage('Approval') {
            when {        
                    expression { params.action == 'apply' }
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
 
                    expression { params.action == 'apply' }

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
                            terraform apply --var-file="../env/dev.tfvars.json" -target="module.analytics_etl.module.glue_wfs" -parallelism=1 -auto-approve

                            echo "Applying overall plan"
                            terraform apply --var-file="../env/dev.tfvars.json" -auto-approve
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
                    withAWS(roleAccount:'731580992380', role:'Cross-Account-role') {
                    sh 'terraform plan -destroy -out=tfplan -lock=false --var-file="../$ENV_TF_VARS"'
                    sh 'terraform show  tfplan > tfplan.txt'
                }
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
                    withAWS(roleAccount:'731580992380', role:'Cross-Account-role') {
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
    }
    post {
        always {
            cleanWs disableDeferredWipeout: true, deleteDirs: true
        }
    }
}
