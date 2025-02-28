pipeline {
    agent any

    stages {
        stage('Build') {
            steps {

                        echo "Building  in  environment"

            }
        }
        stage('🧪 Tests and build') {
            container("java-python-spark") {
            sh """
                ls -alh ~/.cache
                python3 --version
                pip install --upgrade pip
                pip install poetry
                export POETRY_CONFIG_DIR=.config
                poetry -vv config --local virtualenvs.in-project true
                poetry -vv config --local certificates.mirror.cert false
                poetry -vv config --local certificates.nexus.cert false
                poetry -vv config --local certificates.internal.cert false
                poetry run coverage run --source=cfy_lib_tranche -m pytest test
                poetry run coverage xml
                fi
                poetry -vv build -f wheel
            """
            }
        }


    }
}