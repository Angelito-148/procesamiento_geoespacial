// archivo: Jenkinsfile
// Pipeline CI/CD. Si cualquier prueba falla, la etapa "Desplegar" NO se ejecuta.
// Credenciales en Jenkins (Secret text): mongo-password, kaggle-username, kaggle-key
pipeline {
    agent any

    triggers { githubPush() }        // lo dispara el webhook de GitHub

    options {
        disableConcurrentBuilds()
        timeout(time: 120, unit: 'MINUTES')
    }

    parameters {
        booleanParam(name: 'RUN_INGEST', defaultValue: false,
                     description: 'Obtener el dataset con la API de Kaggle, limpiar con Dask y cargar a MongoDB (LENTO)')
        booleanParam(name: 'RUN_SPARK', defaultValue: false,
                     description: 'Recalcular las agregaciones con Spark')
    }

    environment {
        PROD = 'docker compose -p geo -f docker-compose.yml'
        CI   = 'docker compose -p geo-ci -f docker-compose.yml'
        SPARK_CONNECTOR = 'org.mongodb.spark:mongo-spark-connector_2.12:10.4.0'
    }

    stages {
        stage('Checkout') {
            steps { checkout scm }
        }

        stage('Preparar secretos') {
            steps {
                withCredentials([string(credentialsId: 'mongo-password', variable: 'MONGO_PASS')]) {
                    // Comillas simples: el secreto lo expande el shell, no Groovy (no queda en el log)
                    sh '''
                        printf "MONGO_USER=admin\\nMONGO_PASSWORD=%s\\n" "$MONGO_PASS" > .env
                    '''
                }
            }
        }

        stage('Construir imágenes') {
            steps { sh "${PROD} build" }
        }

        stage('Pruebas unitarias (pytest)') {
            steps { sh "${CI} run --rm --no-deps api pytest -q" }
        }

        stage('Levantar entorno de prueba') {
            steps { sh "API_PORT=5099 MONGO_PORT=27099 ${CI} up -d --wait mongo api" }
        }

        stage('Pruebas contra la API') {
            steps {
                sh "${CI} exec -T api python tests/seed.py"
                sh "${CI} exec -T api python tests/smoke.py"
            }
        }

        stage('Desplegar') {
            steps { sh "${PROD} up -d --wait" }
        }

        stage('Ingesta (opcional)') {
            when { expression { params.RUN_INGEST } }
            steps {
                // Token nuevo: usa string(credentialsId: 'kaggle-api-token', variable: 'KAGGLE_API_TOKEN')
                // y cambia "-e KAGGLE_USERNAME -e KAGGLE_KEY" por "-e KAGGLE_API_TOKEN".
                withCredentials([string(credentialsId: 'kaggle-username', variable: 'KAGGLE_USERNAME'),
                                 string(credentialsId: 'kaggle-key', variable: 'KAGGLE_KEY')]) {
                    sh "${PROD} run --rm -e KAGGLE_USERNAME -e KAGGLE_KEY ingest"
                }
            }
        }

        stage('Spark (opcional)') {
            when { expression { params.RUN_SPARK } }
            steps {
                sh """${PROD} exec -T spark-master /opt/spark/bin/spark-submit \\
                        --master spark://spark-master:7077 \\
                        --packages ${SPARK_CONNECTOR} \\
                        --conf spark.jars.ivy=/tmp/.ivy2 \\
                        --conf spark.driver.host=spark-master \\
                        /opt/jobs/aggregations.py"""
            }
        }
    }

    post {
        always {
            sh "API_PORT=5099 MONGO_PORT=27099 ${CI} down -v || true"
            sh 'rm -f .env'
        }
        success { echo 'Despliegue completado.' }
        failure { echo 'El pipeline falló: NO se desplegó la nueva versión.' }
    }
}