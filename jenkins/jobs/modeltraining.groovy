multibranchPipelineJob('MSIL_Analytics_ACE/template-bu-use-case-dapm-framework/modelops-model-step-function-orchestrator') {

    branchSources {
        branchSource {
            source {
                github {
                     id('modelops-model-step-function-orchestrator')
                     repositoryUrl ('template-bu-use-case-dapm-framework')
                     repository('template-bu-use-case-dapm-framework')
                     configuredByUrl(false)
                     repoOwner('MSIL-Analytics-ACE')
                      traits {
                           headWildcardFilter {
                            includes('master production uat')  
                            excludes('release')
                           }
                            gitHubBranchDiscovery {
                                strategyId(1)
                            }
                        }
                    }
                strategy {
                defaultBranchPropertyStrategy {
                    props {
                        noTriggerBranchProperty()
                    }
                }
            }
        }        
    }
                    
configure {
    it / factory(class: 'org.jenkinsci.plugins.workflow.multibranch.WorkflowBranchProjectFactory') {
        owner(class: 'org.jenkinsci.plugins.workflow.multibranch.WorkflowMultiBranchProject', reference: '../..')
        scriptPath('jenkins/pipelines/ModelTraining.Jenkinsfile')
    }
}
    orphanedItemStrategy {
        discardOldItems {
            numToKeep(20)
        }
    }
  }
}