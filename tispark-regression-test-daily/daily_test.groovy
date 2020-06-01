def call(ghprbCommentBody, branch, notify) {
    env.GOROOT = "/usr/local/go"
    env.GOPATH = "/go"
    env.PATH = "/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin"
    env.PATH = "${env.GOROOT}/bin:/home/jenkins/bin:/bin:${env.PATH}"
    def TIDB_BRANCH = "master"
    def TIKV_BRANCH = "master"
    def PD_BRANCH = "master"
    def MVN_PROFILE = "-Pjenkins"
    def TEST_MODE = "simple"
    def PARALLEL_NUMBER = 18
    def TEST_REGION_SIZE = "normal"
    def TEST_NAME = ghprbCommentBody + " tispark-branch=" + branch

    // parse tidb branch
    def m1 = ghprbCommentBody =~ /tidb\s*=\s*([^\s\\]+)(\s|\\|$)/
    if (m1) {
        TIDB_BRANCH = "${m1[0][1]}"
    }
    m1 = null
    println "TIDB_BRANCH=${TIDB_BRANCH}"

    // parse pd branch
    def m2 = ghprbCommentBody =~ /pd\s*=\s*([^\s\\]+)(\s|\\|$)/
    if (m2) {
        PD_BRANCH = "${m2[0][1]}"
    }
    m2 = null
    println "PD_BRANCH=${PD_BRANCH}"

    // parse tikv branch
    def m3 = ghprbCommentBody =~ /tikv\s*=\s*([^\s\\]+)(\s|\\|$)/
    if (m3) {
        TIKV_BRANCH = "${m3[0][1]}"
    }
    m3 = null
    println "TIKV_BRANCH=${TIKV_BRANCH}"

    // parse mvn profile
    def m4 = ghprbCommentBody =~ /profile\s*=\s*([^\s\\]+)(\s|\\|$)/
    if (m4) {
        MVN_PROFILE = MVN_PROFILE + " -P${m4[0][1]}"
    }

    // parse test mode
    def m5 = ghprbCommentBody =~ /mode\s*=\s*([^\s\\]+)(\s|\\|$)/
    if (m5) {
        TEST_MODE = "${m5[0][1]}"
    }

    // parse test region size
    def m6 = ghprbCommentBody =~ /region\s*=\s*([^\s\\]+)(\s|\\|$)/
    if (m6) {
        TEST_REGION_SIZE = "${m6[0][1]}"
    }

    // parse test name
    def m99 = ghprbCommentBody =~ /name\s*=\s*([^\s\\]+)(\s|\\|$)/
    if (m99) {
        TEST_NAME = "${m99[0][1]}"
    }

    def readfile = { filename ->
        def file = readFile filename
        return file.split("\n") as List
    }

    def remove_last_str = { str ->
        return str.substring(0, str.length() - 1)
    }

    def get_mvn_str = { total_chunks ->
        def mvnStr = " -DwildcardSuites="
        for (int i = 0 ; i < total_chunks.size() - 1; i++) {
            // print total_chunks
            def trimStr = total_chunks[i]
            mvnStr = mvnStr + "${trimStr},"
        }
        def trimStr = total_chunks[total_chunks.size() - 1]
        mvnStr = mvnStr + "${trimStr}"
        mvnStr = mvnStr + " -DfailIfNoTests=false"
        mvnStr = mvnStr + " -DskipAfterFailureCount=1"
        return mvnStr
    }

    taskStartTimeInMillis = System.currentTimeMillis()
    taskResult = "FAILED"

    catchError {
        stage('Prepare') {
            node ('build_go1120') {
                println "${NODE_NAME}"
                container("golang") {
                    deleteDir()
                    def ws = pwd()

                    // tidb
                    def tidb_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/tidb/${TIDB_BRANCH}/sha1").trim()
                    sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/tidb/${tidb_sha1}/centos7/tidb-server.tar.gz | tar xz"
                    // tikv
                    def tikv_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/tikv/${TIKV_BRANCH}/sha1").trim()
                    sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/tikv/${tikv_sha1}/centos7/tikv-server.tar.gz | tar xz"
                    // pd
                    def pd_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/pd/${PD_BRANCH}/sha1").trim()
                    sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/pd/${pd_sha1}/centos7/pd-server.tar.gz | tar xz"
                    stash includes: "bin/**", name: "binaries"

                    dir("/home/jenkins/agent/git") {
                        if (sh(returnStatus: true, script: '[ -d .git ] && [ -f Makefile ] && git rev-parse --git-dir > /dev/null 2>&1') != 0) {
                            deleteDir()
                        }
                        checkout changelog: false, poll: false, scm: [$class: 'GitSCM', branches: [[name: branch]], doGenerateSubmoduleConfigurations: false, extensions: [[$class: 'PruneStaleBranch'], [$class: 'CleanBeforeCheckout']], submoduleCfg: [], userRemoteConfigs: [[credentialsId: 'github-sre-bot-ssh', url: 'git@github.com:pingcap/tispark.git']]]
                    }

                    dir("go/src/github.com/pingcap/tispark") {
                        deleteDir()
                        sh """
                        cp -R /home/jenkins/agent/git/. ./
                        find core/src -name '*Suite*' | grep -v 'MultiColumnPKDataTypeSuite' > test
                        shuf test -o  test2
                        mv test2 test
                        """

                        if(TEST_REGION_SIZE  != "normal" && branch != "release-2.1") {
                            sh "sed -i 's/\\# region-max-size = \\\"2MB\\\"/region-max-size = \\\"2MB\\\"/' config/tikv.toml"
                            sh "sed -i 's/\\# region-split-size = \\\"1MB\\\"/region-split-size = \\\"1MB\\\"/' config/tikv.toml"
                            sh "cat config/tikv.toml"
                        }

                        if(TEST_MODE != "simple") {
                            sh """
                            find core/src -name '*MultiColumnPKDataTypeSuite*' >> test
                            """
                        }

                        sh """
                        sed -i 's/core\\/src\\/test\\/scala\\///g' test
                        sed -i 's/\\//\\./g' test
                        sed -i 's/\\.scala//g' test
                        split test -n r/$PARALLEL_NUMBER test_unit_ -a 2 --numeric-suffixes=1
                        """

                        for (int i = 1; i <= PARALLEL_NUMBER; i++) {
                            if(i < 10) {
                                sh """cat test_unit_0$i"""
                            } else {
                                sh """cat test_unit_$i"""
                            }
                        }

                        sh """
                        cp .ci/log4j-ci.properties core/src/test/resources/log4j.properties
                        bash core/scripts/version.sh
                        bash core/scripts/fetch-test-data.sh
                        bash tikv-client/scripts/proto.sh
                        """
                        if(branch != "release-2.1") {
                          sh "mv core/src/test core-test/src/"
                        }
                    }

                    stash includes: "go/src/github.com/pingcap/tispark/**", name: "tispark", useDefaultExcludes: false
                }
            }
        }

        stage("Integration Tests: ${TEST_NAME}") {
            def tests = [:]

            def run_tispark_test = { chunk_suffix ->
                dir("go/src/github.com/pingcap/tispark") {
                    if(chunk_suffix < 10) {
                        run_chunks = readfile("test_unit_0${chunk_suffix}")
                    } else {
                        run_chunks = readfile("test_unit_${chunk_suffix}")
                    }

                    print run_chunks
                    def mvnStr = get_mvn_str(run_chunks)
                    sh """
                        rm -rf /maven/.m2/repository/*
                        cat /maven/.m2/settings.xml 2>/dev/null
                        archive_url=http://fileserver.pingcap.net/download/builds/pingcap/tispark/cache/tispark-m2-cache-latest.tar.gz
                        if [ ! "\$(ls -A /maven/.m2/repository)" ]; then curl -sL \$archive_url | tar -zx -C /maven || true; fi
                    """
                    sh """
                        export MAVEN_OPTS="-Xmx6G -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=51M"
                        mvn compile ${MVN_PROFILE}
                        mvn test ${MVN_PROFILE} -Dtest=moo ${mvnStr}
                    """
                }
            }

            def run_tikvclient_test = { chunk_suffix ->
                dir("go/src/github.com/pingcap/tispark") {
                    sh """
                        rm -rf /maven/.m2/repository/*
                        cat /maven/.m2/settings.xml 2>/dev/null
                        archive_url=http://fileserver.pingcap.net/download/builds/pingcap/tispark/cache/tispark-m2-cache-latest.tar.gz
                        if [ ! "\$(ls -A /maven/.m2/repository)" ]; then curl -sL \$archive_url | tar -zx -C /maven || true; fi
                    """
                    sh """
                        export MAVEN_OPTS="-Xmx6G -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512M"
                        mvn test ${MVN_PROFILE} -am -pl tikv-client
                    """
                }
            }

            def run_intergration_test = { chunk_suffix, run_test ->
                node("test_java") {
                    println "${NODE_NAME}"
                    container("java") {
                        def ws = pwd()
                        deleteDir()
                        unstash 'binaries'
                        unstash 'tispark'

                        try {
                            sh """
                            sudo sysctl -w net.ipv4.ip_local_port_range='10000 30000'
                            killall -9 tidb-server || true
                            killall -9 tikv-server || true
                            killall -9 pd-server || true
                            sleep 10
                            bin/pd-server --name=pd --data-dir=pd --config=go/src/github.com/pingcap/tispark/config/pd.toml &>pd.log &
                            sleep 30
                            bin/tikv-server --pd=127.0.0.1:2379 -s tikv --addr=0.0.0.0:20160 --advertise-addr=127.0.0.1:20160 --config=go/src/github.com/pingcap/tispark/config/tikv.toml &>tikv.log &
                            sleep 30
                            ps aux | grep '-server' || true
                            curl -s 127.0.0.1:2379/pd/api/v1/status || true
                            bin/tidb-server --store=tikv --path="127.0.0.1:2379" --config=go/src/github.com/pingcap/tispark/config/tidb.toml &>tidb.log &
                            sleep 60
                            """

                            timeout(180) {
                                run_test(chunk_suffix)
                            }
                        } catch (err) {
                            sh """
                            ps aux | grep '-server' || true
                            curl -s 127.0.0.1:2379/pd/api/v1/status || true
                            """
                            sh "cat pd.log"
                            sh "cat tikv.log"
                            sh "cat tidb.log"
                            throw err
                        }
                    }
                }
            }

           for (int i = 1; i <= PARALLEL_NUMBER; i++) {
                int x = i
                tests["Integration test = $i"] = {run_intergration_test(x, run_tispark_test)}
            }
            tests["Integration tikv-client test"] = {run_intergration_test(0, run_tikvclient_test)}

            parallel tests
        }

        taskResult = "SUCCESS"
    }

    stage('Summary') {
        if (notify == "true" || notify == true) {
            def duration = ((System.currentTimeMillis() - taskStartTimeInMillis) / 1000 / 60).setScale(2, BigDecimal.ROUND_HALF_UP)
            def slackmsg = "TiSpark Daily Integration Test\n" +
            "Argument: [${TEST_NAME}]\n" +
            "Result: `${taskResult}`\n" +
            "Elapsed Time: `${duration}` Mins\n" +
            "https://internal.pingcap.net/idc-jenkins/blue/organizations/jenkins/tispark_regression_test_daily/activity\n" +
            "https://internal.pingcap.net/idc-jenkins/job/tispark_regression_test_daily/"

            if (taskResult != "SUCCESS") {
                slackSend channel: '#tispark-daily-test', color: 'danger', teamDomain: 'pingcap', tokenCredentialId: 'slack-pingcap-token', message: "${slackmsg}"
            } else {
                slackSend channel: '#tispark-daily-test', color: 'good', teamDomain: 'pingcap', tokenCredentialId: 'slack-pingcap-token', message: "${slackmsg}"
            }
        }
    }
}

def runDailyIntegrationTest(tisparkBranch, tidbVersion, testMode, region, notify) {
  call("tikv=${tidbVersion} tidb=${tidbVersion} pd=${tidbVersion} mode=${testMode} region=${region}", tisparkBranch, notify)
}

return this
