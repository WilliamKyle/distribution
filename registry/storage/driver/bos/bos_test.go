// +build include_bos

package bos

import (
	"os"
	"testing"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/testsuites"

	"gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { check.TestingT(t) }

var driverConstructor func(rootDirectory string) (*Driver, error)

var skipCheck func() string

func init() {
	accessKeyId := os.Getenv("ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("SECRET_ACCESS_KEY")
	bucketName := os.Getenv("BUCKET_NAME")
	endpoint := os.Getenv("ENDPOINT")

	driverConstructor := func() (storagedriver.StorageDriver, error) {
		parameters := DriverParameters{
			accessKeyId,
			secretAccessKey,
			bucketName,
			endpoint,
		}

		return New(parameters)
	}

	skipCheck := func() string {
		return ""
		if accessKeyId == "" {
			return "RADOS_POOL must be set to run Rado tests"
		}
		return ""
	}

	testsuites.RegisterSuite(driverConstructor, skipCheck)
}

func TestEmptyRootList(t *testing.T) {
	if skipCheck() != "" {
		t.Skip(skipCheck())
	}

	_, err := driverConstructor("")
	if err != nil {
		t.Fatalf("init failed.")
	}
}
