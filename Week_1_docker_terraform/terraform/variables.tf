variable "credentials" {
  description = "My credentials"
  default = "./keys/terraform-demo-447612-b1129dd13947.json"
}



variable "project" {
  description = "Project"
  default     = "terraform-demo-447612"
}

variable "region" {
  description = "Region"
  default     = "asia-southeast1"
}

variable "bq_dataset_name" {
  description = "My big query dataset name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My storage bucket name"
  default     = "terraform-demo-447612-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket storage class"
  default     = "STANDARD"
}