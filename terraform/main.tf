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

# Persistent volume for postgres data
resource "docker_volume" "postgres_data" {
  name = "postgres-data"
}

# Persistent volume for mongo data
resource "docker_volume" "mongo_data" {
  name = "mongo-data"
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
