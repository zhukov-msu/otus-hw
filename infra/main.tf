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

# variable "YC_SA_ID_OTUS" {
#   type = string
# }

variable "YC_OAUTH_TOKEN" {
  type = string
}

variable "YC_CLOUD_ID" {
  type = string
}

locals {
  folder_id = var.YC_FOLDER_ID
  cloud_id = var.YC_CLOUD_ID
  oauth = var.YC_OAUTH_TOKEN
  path_to_ssh_public_key = "~/.ssh/yc_cluster.pub"
}

provider "yandex" {
  folder_id = local.folder_id
  cloud_id  = local.cloud_id
  token      = local.oauth
  zone      = "ru-central1-a"
  #   service_account_key_file = file("secrets/terraform_service_key.json")
}

// example
resource "yandex_iam_service_account" "sa" {
  name = "dataproc1"
}

// Назначение роли сервисному аккаунту
resource "yandex_resourcemanager_folder_iam_member" "sa-admin" {
  folder_id = local.folder_id
  role      = "storage.admin"
  member    = "serviceAccount:${yandex_iam_service_account.sa.id}"
}

// Создание статического ключа доступа
resource "yandex_iam_service_account_static_access_key" "sa-static-key" {
  service_account_id = yandex_iam_service_account.sa.id
  description        = "static access key for object storage"
}

// Create Static Access Keys
# resource "yandex_iam_service_account_static_access_key" "sa-static-key" {
#   service_account_id = var.YC_SA_ID_OTUS
#   description        = "static access key for object storage"
# }

// Use keys to create bucket
resource "yandex_storage_bucket" "test" {
  access_key = yandex_iam_service_account_static_access_key.sa-static-key.access_key
  secret_key = yandex_iam_service_account_static_access_key.sa-static-key.secret_key
  bucket = "otus-hw-bucket"
}

