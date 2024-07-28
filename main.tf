terraform {
  required_providers {
    yandex = {
      source = "yandex-cloud/yandex"
    }
  }
  required_version = ">= 0.13"
}

variable "YC_FOLDER_ID" {
  type = string
}

variable "YC_SA_ID_OTUS" {
  type = string
}

variable "YC_CLOUD_ID" {
  type = string
}

locals {
  folder_id = var.YC_FOLDER_ID
}

provider "yandex" {
  folder_id = local.folder_id
  zone      = "ru-central1-a"
}

// Create Static Access Keys
resource "yandex_iam_service_account_static_access_key" "sa-static-key" {
  service_account_id = var.YC_SA_ID_OTUS
  description        = "static access key for object storage"
}

// Use keys to create bucket
resource "yandex_storage_bucket" "test" {
  access_key = yandex_iam_service_account_static_access_key.sa-static-key.access_key
  secret_key = yandex_iam_service_account_static_access_key.sa-static-key.secret_key
  bucket = "otus-hw-bucket"
}