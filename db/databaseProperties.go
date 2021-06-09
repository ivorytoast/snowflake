package db

import "os"

var UserName = "!UNDEFINED USERNAME!"
var Password = "!UNDEFINED PASSWORD!"
var Host = "!UNDEFINED HOST!"
var ClusterName = "!UNDEFINED CLUSTER NAME!"
var PathToCert = "!UNDEFINED PATH TO CERTIFICATE!"

func InitializeDatabaseProperties() {
	if tempUserName := os.Getenv("CR_USERNAME"); tempUserName != "" {
		UserName = os.Getenv("CR_USERNAME")
	}
	if tempPassword := os.Getenv("CR_PASSWORD"); tempPassword != "" {
		Password = os.Getenv("CR_PASSWORD")
	}
	if tempHost := os.Getenv("CR_HOST"); tempHost != "" {
		Host = os.Getenv("CR_HOST")
	}
	if tempClusterName := os.Getenv("CR_CLUSTER_NAME"); tempClusterName != "" {
		ClusterName = os.Getenv("CR_CLUSTER_NAME")
	}
	if tempPathToCert := os.Getenv("CR_PATH_TO_CERT"); tempPathToCert != "" {
		PathToCert = os.Getenv("CR_PATH_TO_CERT")
	}
}