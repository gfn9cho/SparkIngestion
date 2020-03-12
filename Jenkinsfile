#!/usr/bin/env groovy

library 'SCDM-lib'

def String appname = 'DatalakeProcessed'
def buildtype = 'sbt'

node('L5JENKSLVLA1') {

	env.CODEREPO = 'git'
	env.CODEQUAL = 'false'
	env.SBT_GOAL_LIST = 'assembly'
	env.appExt = 'jar'
	jdk = tool name: 'JDK8'
	env.JAVA_HOME = "${jdk}"

	//Add list of recipients separated by a single space
	env.RECIPIENTS = 'venkata.naram@stateauto.com,vinaykumar.kittur@stateauto.com,EDLakeBuild@stateauto.com'

	//Auto deploy to L1
	env.L1env = ''
	
	//For Infra deploy
	env.AMIDeploy = 'false' 
	
	
	//Call the sbt groovy file
	builder "${appname}", "${buildtype}"
}
