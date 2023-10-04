
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
                    sh 'terraform apply -no-color -input=false tfplan'

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
                    sh 'terraform plan -destroy -out=tfplan -lock=false --var-file="../env/dev.tfvars.json"'
                    sh 'terraform show  tfplan > tfplan.txt'
                }
                }
            }
        }

        stage('Destroy') {
            when {
                    expression { params.action == 'destroy' }
                }
            
            steps {
                dir('infra') {
                    withAWS(roleAccount:'731580992380', role:'Cross-Account-role') {
                    script {
                        def plan = readFile 'tfplan.txt'
                        input message: "Delete the stack?",
                        parameters: [text(name: 'Plan', description: 'Please review the plan', defaultValue: plan)]
                    }

                    sh 'terraform destroy -no-color -auto-approve -lock=false  --var-file="../env/dev.tfvars.json"'
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
