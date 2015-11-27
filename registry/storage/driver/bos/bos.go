// +build include_bos

package bos

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	_ "strconv"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/WilliamKyle/baidubce/auth"
	"github.com/WilliamKyle/baidubce/service/bos"
	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
)

const driverName = "bos"

// Stripes objects size to 4M
const defaultChunkSize = 4 << 20

//DriverParameters A struct that encapsulates all of the driver parameters after all values have been set
type DriverParameters struct {
	accessKeyId     string
	secretAccessKey string
	bucketName      string
	endpoint        string
}

func init() {
	factory.Register(driverName, &bosDriverFactory{})
}

// bosDriverFactory implements the factory.StorageDriverFactory interface
type bosDriverFactory struct{}

func (factory *bosDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

type driver struct {
	Client     *bos.Client
	bucketName string
}

type baseEmbed struct {
	base.Base
}

// Driver is a storagedriver.StorageDriver implementation backed by Ceph RADOS
// Objects are stored at absolute keys in the provided bucket.
type Driver struct {
	baseEmbed
}

// FromParameters constructs a new Driver with a given parameters map
// Required parameters:
func FromParameters(parameters map[string]interface{}) (*Driver, error) {

	accessKeyId, ok := parameters["accesskeyid"]
	if !ok || fmt.Sprint(accessKeyId) == "" {
		return nil, fmt.Errorf("No accesskeyid parameter provided")
	}

	secretAccessKey, ok := parameters["accesskeysecret"]
	if !ok || fmt.Sprint(secretAccessKey) == "" {
		return nil, fmt.Errorf("No accesskeysecret parameter provided")
	}

	bucketName, ok := parameters["bucket"]
	if !ok || fmt.Sprint(bucketName) == "" {
		return nil, fmt.Errorf("No bucket parameter provided")
	}

	endpoint, ok := parameters["endpoint"]
	if !ok {
		endpoint = ""
	}

	params := DriverParameters{
		fmt.Sprint(accessKeyId),
		fmt.Sprint(secretAccessKey),
		fmt.Sprint(bucketName),
		fmt.Sprint(endpoint),
	}

	return New(params)
}

// New constructs a new Driver
func New(params DriverParameters) (*Driver, error) {
	client, err := bos.NewClient(auth.NewBceCredentials(params.accessKeyId, params.secretAccessKey))
	if err != nil {
		return nil, err
	}
	//client.Debug = true
	if params.endpoint != "" {
		client.Host = params.endpoint
	}

	log.Infof("Connected")

	d := &driver{
		Client:     &client,
		bucketName: params.bucketName,
	}

	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: d,
			},
		},
	}, nil
}

// Implement the storagedriver.StorageDriver interface

func (d *driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	res, err := d.Client.GetObject(d.bucketName, path, 0, 0)
	if err != nil {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}
	data, _ := ioutil.ReadAll(res.Body)
	return data, nil
}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, path string, contents []byte) error {
	_, err := d.Client.PutObject(d.bucketName, path, bytes.NewReader(contents), "", "", nil)
	if err != nil {
		return err
	}
	return nil
}

func (d *driver) ReadStream(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	res, err := d.Client.GetObject(d.bucketName, path, 0, 0)
	if err != nil {
		return nil, err
	}
	return res.Body, nil
}

// TODO mutilUpload
func (d *driver) WriteStream(ctx context.Context, path string, offset int64, reader io.Reader) (totalRead int64, err error) {
	totalRead = 0
	buffSize := 5 << 20
	res, err := d.Client.InitiateMultipartUpload(d.bucketName, path, "")
	if err != nil {
		return
	}

	uploadId := res.UploadId
	index := 1
	parts := []bos.PartInfo{}
	for {
		buf := make([]byte, buffSize)
		readed, readErr := reader.Read(buf)
		totalRead += int64(readed)

		if index == 1 && readed < buffSize {
			d.Client.AbortMultipartUpload(d.bucketName, path, uploadId)
			_, err = d.Client.PutObject(d.bucketName, path, bytes.NewReader(buf[:readed]), "", "", nil)
			return
		}

		eTag, err := d.Client.UploadPart(d.bucketName, path, uploadId, fmt.Sprintf("%d", index), bytes.NewReader(buf[:readed]))
		if err != nil {
			return 0, err
		}
		partInfo := bos.PartInfo{PartNumber: index, ETag: eTag}
		parts = append(parts, partInfo)

		if readErr == io.EOF {
			_, err = d.Client.CompleteMultipartUpload(d.bucketName, path, uploadId, parts)
			return totalRead, err
		}

		index += 1
	}

	return
}

// Stat retrieves the FileInfo for the given path, including the current size
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	fi := storagedriver.FileInfoFields{
		Path: path,
	}

	res, err := d.Client.ListObjects(d.bucketName, nil, nil, nil, path)
	if err != nil {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}

	if len(res.Contents) < 1 {
		return nil, storagedriver.PathNotFoundError{Path: path}
	} else if len(res.Contents) > 1 {
		fi.IsDir = true
	} else {
		fi.IsDir = false
		fi.Size = res.Contents[0].Size
		//fi.Size, _ = strconv.ParseInt(res1["Size"], 10, 64)
	}
	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

func ItemInArray(arr []string, item string) bool {
	for _, f := range arr {
		if f == item {
			return true
		}
	}
	return false
}

// List returns a list of the objects that are direct descendants of the given path.
func (d *driver) List(ctx context.Context, path string) ([]string, error) {
	res, err := d.Client.ListObjects(d.bucketName, nil, nil, nil, path)
	if err != nil {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}

	var files []string
	var directories []string
	items := res.Contents
	for i := 0; i < len(items); i++ {
		item := items[i].ObjectName[len(path):len(items[i].ObjectName)]
		if strings.HasPrefix(item, "/") {
			item = item[1:len(item)]
		}
		fields := strings.Split(item, "/")
		if len(fields) > 1 {
			if !ItemInArray(directories, fields[0]) {
				directories = append(directories, fields[0])
			}
		} else {
			if !ItemInArray(files, fields[0]) {
				files = append(files, fields[0])
			}
		}
	}

	return append(files, directories...), nil
}

// Move moves an object stored at sourcePath to destPath, removing the original
// object.
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	_, err := d.Client.CopyObject(d.bucketName, sourcePath, d.bucketName, destPath, "", "")
	if err != nil {
		return err
	}
	d.Client.DeleteObject(d.bucketName, sourcePath)
	return nil
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, objectPath string) error {
	res, err := d.Client.ListObjects(d.bucketName, nil, nil, nil, objectPath)
	if err != nil {
		return nil
	}

	items := res.Contents
	for i := 0; i < len(items); i++ {
		item := items[i].ObjectName
		d.Client.DeleteObject(d.bucketName, item)
	}

	return nil
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path.
// May return an UnsupportedMethodErr in certain StorageDriver implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	return "", storagedriver.ErrUnsupportedMethod{}
}
