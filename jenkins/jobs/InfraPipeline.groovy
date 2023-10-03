multibranchPipelineJob('MSIL_Analytics_ACE/quality-dcp/terraform-infra-deploy') {


    branchSources {
        branchSource {
            source {
                github {
                     id('terraform-infra-deploy')
                     repositoryUrl ('quality-dcp')
                     repository('quality-dcp')
                     credentialsId('marutideep')
                     configuredByUrl(false)
                     repoOwner('MSIL-Analytics-ACE')
                      traits {
                           headWildcardFilter {
                            includes('main production uat')  
                            excludes('release test')
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
        scriptPath('jenkins/pipelines/Infra.Jenkinsfile')
    }
}
    orphanedItemStrategy {
        discardOldItems {
            numToKeep(20)
        }
    }
  }
}