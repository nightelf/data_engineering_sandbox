terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
  }
}

provider "docker" {
  host = "unix:///Users/jaredkuemmerle/.docker/run/docker.sock"
}

# Pull the image once and share it across all containers
resource "docker_image" "spark_py" {
  name         = "apache/spark-py:latest"
  keep_locally = true
}

resource "docker_image" "postgres" {
  name         = "postgres:latest"
  keep_locally = true
}

resource "docker_image" "mongo" {
  name         = "mongo:latest"
  keep_locally = true
}

resource "docker_image" "kafka" {
  name         = "confluentinc/cp-kafka:7.6.1"
  keep_locally = true
}

resource "docker_image" "kafka_connect" {
  name         = "debezium/connect:2.7.3.Final"
  keep_locally = true
}

# Persistent volume for postgres data
resource "docker_volume" "postgres_data" {
  name = "postgres-data"
}

# Persistent volume for mongo data
resource "docker_volume" "mongo_data" {
  name = "mongo-data"
}

# Persistent volume for kafka data
resource "docker_volume" "kafka_data" {
  name = "kafka-data"
}

# Shared bridge network
resource "docker_network" "spark_net" {
  name   = "spark-net"
  driver = "bridge"
}

# Spark Master
resource "docker_container" "spark_master" {
  name  = "spark-master"
  image = docker_image.spark_py.image_id

  entrypoint = [
    "/opt/spark/bin/spark-class",
    "org.apache.spark.deploy.master.Master"
  ]

  env = [
    "SPARK_MASTER_HOST=spark-master",
    "SPARK_MASTER_PORT=7077",
    "SPARK_MASTER_WEBUI_PORT=8080",
    "SPARK_USER=spark",
    "HOME=/root",
    "HADOOP_USER_NAME=root",
  ]

  ports {
    internal = 7077
    external = 7077
  }

  ports {
    internal = 8080
    external = 8080
  }

  volumes {
    host_path      = var.spark_workdir
    container_path = "/opt/spark/work-dir"
  }

  networks_advanced {
    name = docker_network.spark_net.name
  }

  restart = "unless-stopped"
}

# Spark Worker 1
resource "docker_container" "spark_worker_1" {
  name  = "spark-worker-1"
  image = docker_image.spark_py.image_id

  entrypoint = [
    "/opt/spark/bin/spark-class",
    "org.apache.spark.deploy.worker.Worker",
    "spark://spark-master:7077"
  ]

  env = [
    "SPARK_WORKER_CORES=${var.worker_cores}",
    "SPARK_WORKER_MEMORY=${var.worker_memory}",
    "SPARK_WORKER_WEBUI_PORT=8081",
    "SPARK_USER=spark",
    "HADOOP_USER_NAME=root",
    "SPARK_WORKER_DIR=/tmp/spark-work",
  ]

  ports {
    internal = 8081
    external = 8081
  }

  volumes {
    host_path      = var.spark_workdir
    container_path = "/opt/spark/work-dir"
  }

  networks_advanced {
    name = docker_network.spark_net.name
  }

  depends_on = [docker_container.spark_master]
  restart    = "unless-stopped"
}

# Spark Worker 2
resource "docker_container" "spark_worker_2" {
  name  = "spark-worker-2"
  image = docker_image.spark_py.image_id

  entrypoint = [
    "/opt/spark/bin/spark-class",
    "org.apache.spark.deploy.worker.Worker",
    "spark://spark-master:7077"
  ]

  env = [
    "SPARK_WORKER_CORES=${var.worker_cores}",
    "SPARK_WORKER_MEMORY=${var.worker_memory}",
    "SPARK_WORKER_WEBUI_PORT=8082",
    "SPARK_USER=spark",
    "HADOOP_USER_NAME=root",
    "SPARK_WORKER_DIR=/tmp/spark-work",
  ]

  ports {
    internal = 8082
    external = 8082
  }

  volumes {
    host_path      = var.spark_workdir
    container_path = "/opt/spark/work-dir"
  }

  networks_advanced {
    name = docker_network.spark_net.name
  }

  depends_on = [docker_container.spark_master]
  restart    = "unless-stopped"
}

# PostgreSQL
resource "docker_container" "postgres" {
  name  = "postgres"
  image = docker_image.postgres.image_id

  # Enable logical replication so Debezium can capture row-level changes.
  command = [
    "postgres",
    "-c", "wal_level=logical",
    "-c", "max_wal_senders=10",
    "-c", "max_replication_slots=10",
  ]

  env = [
    "POSTGRES_USER=spark",
    "POSTGRES_PASSWORD=spark",
    "POSTGRES_DB=sparkdb",
  ]

  ports {
    internal = 5432
    external = 5432
  }

  volumes {
    host_path      = "${var.spark_workdir}/sql"
    container_path = "/sql"
  }

  volumes {
    host_path      = "${var.spark_workdir}/data"
    container_path = "/data"
  }

  volumes {
    volume_name    = docker_volume.postgres_data.name
    container_path = "/var/lib/postgresql"
  }

  networks_advanced {
    name = docker_network.spark_net.name
  }

  restart = "unless-stopped"
}

# MongoDB
resource "docker_container" "mongo" {
  name  = "mongo"
  image = docker_image.mongo.image_id

  ports {
    internal = 27017
    external = 27017
  }

  volumes {
    host_path      = "${var.spark_workdir}/mongo"
    container_path = "/mongo"
  }

  volumes {
    host_path      = "${var.spark_workdir}/data"
    container_path = "/data"
  }

  volumes {
    volume_name    = docker_volume.mongo_data.name
    container_path = "/data/db"
  }

  networks_advanced {
    name = docker_network.spark_net.name
  }

  restart = "unless-stopped"
}

# Kafka broker (KRaft mode, no Zookeeper)
resource "docker_container" "kafka" {
  name  = "kafka"
  image = docker_image.kafka.image_id

  env = [
    "KAFKA_NODE_ID=1",
    "KAFKA_PROCESS_ROLES=broker,controller",
    "KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:29093",
    "KAFKA_LISTENERS=PLAINTEXT://kafka:29092,CONTROLLER://kafka:29093,PLAINTEXT_HOST://0.0.0.0:9092",
    "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092",
    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT",
    "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
    "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
    "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
    "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
    "KAFKA_AUTO_CREATE_TOPICS_ENABLE=true",
    "CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk",
  ]

  ports {
    internal = 9092
    external = 9092
  }

  volumes {
    volume_name    = docker_volume.kafka_data.name
    container_path = "/var/lib/kafka/data"
  }

  networks_advanced {
    name = docker_network.spark_net.name
  }

  restart = "unless-stopped"
}

# Kafka Connect with the Debezium Postgres connector
resource "docker_container" "kafka_connect" {
  name  = "kafka-connect"
  image = docker_image.kafka_connect.image_id

  env = [
    "BOOTSTRAP_SERVERS=kafka:29092",
    "GROUP_ID=connect-cluster",
    "CONFIG_STORAGE_TOPIC=connect_configs",
    "OFFSET_STORAGE_TOPIC=connect_offsets",
    "STATUS_STORAGE_TOPIC=connect_statuses",
    "CONFIG_STORAGE_REPLICATION_FACTOR=1",
    "OFFSET_STORAGE_REPLICATION_FACTOR=1",
    "STATUS_STORAGE_REPLICATION_FACTOR=1",
  ]

  ports {
    internal = 8083
    external = 8083
  }

  volumes {
    host_path      = "${var.spark_workdir}/kafka"
    container_path = "/kafka-config"
  }

  networks_advanced {
    name = docker_network.spark_net.name
  }

  depends_on = [docker_container.kafka, docker_container.postgres]
  restart    = "unless-stopped"
}
