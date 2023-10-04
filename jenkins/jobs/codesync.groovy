multibranchPipelineJob('MSIL/template-bu-use-case-dapm-framework/aws-code-sync') {

    branchSources {
        branchSource {
            source {
                github {
                     id('aws-code-sync')
                     repositoryUrl ('template-bu-use-case-dapm-framework')
                     repository('template-bu-use-case-dapm-framework')
                     configuredByUrl(false)
                     repoOwner('MSIL-Analytics-ACE')
                      traits {
                           headWildcardFilter {
                            includes('master production uat PR-*')  
                            excludes('release')
                           }
                            gitHubBranchDiscovery {
                                strategyId(1)
                            }
                            gitHubPullRequestDiscovery {
                                strategyId(2)
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
        scriptPath('jenkins/pipelines/CodeSync.Jenkinsfile')
    }
}
    orphanedItemStrategy {
        discardOldItems {
            numToKeep(20)
        }
    }
  }
}