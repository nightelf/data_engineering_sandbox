variable "spark_workdir" {
  description = "Absolute path to the local spark directory to mount into containers"
  type        = string
  default     = "/Users/jaredkuemmerle/Documents/repos/sample_apps/spark"
}

variable "worker_cores" {
  description = "Number of CPU cores per worker"
  type        = number
  default     = 2
}

variable "worker_memory" {
  description = "Memory allocation per worker (e.g. 2g)"
  type        = string
  default     = "2g"
}
