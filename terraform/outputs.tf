output "spark_master_ui" {
  description = "Spark Master Web UI"
  value       = "http://localhost:8080"
}

output "spark_master_url" {
  description = "Spark Master connection URL"
  value       = "spark://localhost:7077"
}

output "spark_worker_1_ui" {
  description = "Spark Worker 1 Web UI"
  value       = "http://localhost:8081"
}

output "spark_worker_2_ui" {
  description = "Spark Worker 2 Web UI"
  value       = "http://localhost:8082"
}

output "postgres_connection" {
  description = "PostgreSQL connection string"
  value       = "postgresql://spark:spark@localhost:5432/sparkdb"
}
