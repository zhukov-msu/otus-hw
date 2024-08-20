resource "yandex_dataproc_cluster" "hw_cluster" {
  depends_on = [yandex_resourcemanager_folder_iam_binding.dataproc]

  bucket      = yandex_storage_bucket.hw-bucket.bucket
  description = "Dataproc Cluster created by Terraform for OTUS MLOps course"
  name        = "hw-cluster"
  labels = {
    created_by = "terraform"
  }
  service_account_id = yandex_iam_service_account.dataproc.id
  zone_id            = "ru-central1-a"

  cluster_config {
    # Certain cluster version can be set, but better to use default value (last stable version)
    # version_id = "2.0"

    hadoop {
      services = ["HDFS", "YARN", "SPARK", "TEZ", "MAPREDUCE", "HIVE"]
      properties = {
        "yarn:yarn.resourcemanager.am.max-attempts" = 5
      }
      ssh_public_keys = [
        file("~/.ssh/yc_mlops.pub"),
      ]
    }

    subcluster_spec {
      name = "main"
      role = "MASTERNODE"
      resources {
        resource_preset_id = "s3-c2-m8"
        disk_type_id       = "network-hdd"
        disk_size          = 40
      }
      subnet_id   = yandex_vpc_subnet.hw_subnet.id
      hosts_count = 1
    }

    subcluster_spec {
      name = "data"
      role = "DATANODE"
      resources {
        resource_preset_id = "s3-c4-m16"
        disk_type_id       = "network-hdd"
        disk_size          = 128
      }
      subnet_id   = yandex_vpc_subnet.hw_subnet.id
      hosts_count = 3
    }
  }
}

resource "yandex_vpc_network" "hw_network" {
  name = "hw_network"
}

resource "yandex_vpc_subnet" "hw_subnet" {
  zone           = "ru-central1-a"
  network_id     = yandex_vpc_network.hw_network.id
  v4_cidr_blocks = ["10.1.0.0/24"]
  route_table_id = yandex_vpc_route_table.rt.id
}

resource "yandex_vpc_gateway" "nat_gateway" {
  name = "test-gateway"
  shared_egress_gateway {}
}

resource "yandex_vpc_route_table" "rt" {
  name       = "test-route-table"
  network_id = yandex_vpc_network.hw_network.id

  static_route {
    destination_prefix = "0.0.0.0/0"
    gateway_id         = yandex_vpc_gateway.nat_gateway.id
  }
}

resource "yandex_iam_service_account" "dataproc" {
  name        = "dataproc"
  description = "service account to manage Dataproc Cluster"
}

data "yandex_resourcemanager_folder" "hw_cluster_folder" {
  folder_id = local.folder_id
}


resource "yandex_resourcemanager_folder_iam_binding" "dataproc" {
  folder_id = data.yandex_resourcemanager_folder.hw_cluster_folder.id
  role      = "mdb.dataproc.agent"
  members = [
    "serviceAccount:${yandex_iam_service_account.dataproc.id}",
  ]
}

# required in order to create bucket
resource "yandex_resourcemanager_folder_iam_binding" "bucket-creator" {
  folder_id = data.yandex_resourcemanager_folder.hw_cluster_folder.id
  role      = "editor"
  members = [
    "serviceAccount:${yandex_iam_service_account.dataproc.id}",
  ]
}

resource "yandex_iam_service_account_static_access_key" "cluster_static_key" {
  service_account_id = yandex_iam_service_account.dataproc.id
}

resource "yandex_storage_bucket" "hw-bucket" {
  depends_on = [
    yandex_resourcemanager_folder_iam_binding.bucket-creator
  ]

  bucket     = "hw-bucket"
  access_key = yandex_iam_service_account_static_access_key.cluster_static_key.access_key
  secret_key = yandex_iam_service_account_static_access_key.cluster_static_key.secret_key
}