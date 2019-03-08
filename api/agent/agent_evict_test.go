package agent

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/fnproject/fn/api/agent/drivers/docker"
	"github.com/fnproject/fn/api/id"
	"github.com/fnproject/fn/api/logs"
	"github.com/fnproject/fn/api/models"
	"github.com/fnproject/fn/api/mqs"

	"github.com/sirupsen/logrus"
)

// create a simple non-blocking agent. Non-blocking does not queue, so it's
// easier to test and see if evictions took place.
func getAgent() (Agent, error) {
	ls := logs.NewMock()
	cfg, err := NewConfig()
	if err != nil {
		return nil, err
	}

	// 160MB memory
	cfg.EnableNBResourceTracker = true
	cfg.HotPoll = 20
	cfg.MaxTotalMemory = 160 * 1024 * 1024
	cfg.HotPullTimeout = time.Duration(80000) * time.Millisecond
	cfg.HotStartTimeout = time.Duration(10000) * time.Millisecond

	a := New(NewDirectCallDataAccess(ls, new(mqs.Mock)), WithConfig(cfg))
	return a, nil
}

func getFakeDocker(t *testing.T) (*httptest.Server, func()) {
	ctx, cancel := context.WithCancel(context.Background())

	first := `{
   "schemaVersion": 2,
   "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
   "manifests": [
      {
         "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
         "size": 524,
         "digest": "sha256:92c7f9c92844bbbb5d0a101b22f7c2a7949e40f8ea90c8b3bc396879d95e899a",
         "platform": {
            "architecture": "amd64",
            "os": "linux"
         }
      },
      {
         "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
         "size": 525,
         "digest": "sha256:1e44d8bca6fb0464794555e5ccd3a32e2a4f6e44a20605e4e82605189904f44d",
         "platform": {
            "architecture": "arm",
            "os": "linux",
            "variant": "v5"
         }
      },
      {
         "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
         "size": 524,
         "digest": "sha256:d1fd2e204af0a2bca3ab033b417b29c76d7950ed29a44e427d1c4d07d14f04f9",
         "platform": {
            "architecture": "arm",
            "os": "linux",
            "variant": "v7"
         }
      },
      {
         "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
         "size": 525,
         "digest": "sha256:d0d4c5389b53875b0f2364f94c466f77cf6f02811fb02f0477b97d609fb50568",
         "platform": {
            "architecture": "arm64",
            "os": "linux",
            "variant": "v8"
         }
      },
      {
         "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
         "size": 527,
         "digest": "sha256:5a4bdadd9acd8779ed6fcf007a4e7ed7f919056a92c3c67824b4fded06ef0a6e",
         "platform": {
            "architecture": "386",
            "os": "linux"
         }
      },
      {
         "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
         "size": 525,
         "digest": "sha256:12cf9ef90835465316cb0b3729c36bfd8654d7f2f697e23432fddfaa7d7e31b5",
         "platform": {
            "architecture": "ppc64le",
            "os": "linux"
         }
      },
      {
         "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
         "size": 525,
         "digest": "sha256:577ad4331d4fac91807308da99ecc107dcc6b2254bc4c7166325fd01113bea2a",
         "platform": {
            "architecture": "s390x",
            "os": "linux"
         }
      },
      {
         "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
         "size": 1359,
         "digest": "sha256:c1be6e1468485757698af528fff774d474e69f448eef43c368fa2f2be1288b4d",
         "platform": {
            "architecture": "amd64",
            "os": "windows",
            "os.version": "10.0.14393.2551"
         }
      },
      {
         "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
         "size": 1357,
         "digest": "sha256:4c0c09dd5a7fe632acd637acf0a676ead5ce2aeaa5b79c181636171bf57aa153",
         "platform": {
            "architecture": "amd64",
            "os": "windows",
            "os.version": "10.0.16299.846"
         }
      },
      {
         "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
         "size": 1357,
         "digest": "sha256:318b5ff947acc631fadaacf23eae890806f0e5219a0d85802877f619874e1a37",
         "platform": {
            "architecture": "amd64",
            "os": "windows",
            "os.version": "10.0.17134.469"
         }
      },
      {
         "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
         "size": 1358,
         "digest": "sha256:dd9b7482975b66309507df1365b17ea38ecd5e79005eaa8a57136a60ef5e3cf5",
         "platform": {
            "architecture": "amd64",
            "os": "windows",
            "os.version": "10.0.17763.194"
         }
      }
   ]
}`

	second := `{
   "schemaVersion": 2,
   "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
   "config": {
      "mediaType": "application/vnd.docker.container.image.v1+json",
      "size": 1510,
      "digest": "sha256:fce289e99eb9bca977dae136fbe2a82b6b7d4c372474c9235adc1741675f587e"
   },
   "layers": [
      {
         "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
         "size": 977,
         "digest": "sha256:1b930d010525941c1d56ec53b97bd057a67ae1865eebf042686d2a2d18271ced"
      }
   ]
}`
	third := `{"architecture":"amd64","config":{"Hostname":"","Domainname":"","User":"","AttachStdin":false,"AttachStdout":false,"AttachStderr":false,"Tty":false,"OpenStdin":false,"StdinOnce":false,"Env":["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"],"Cmd":["/hello"],"ArgsEscaped":true,"Image":"sha256:a6d1aaad8ca65655449a26146699fe9d61240071f6992975be7e720f1cd42440","Volumes":null,"WorkingDir":"","Entrypoint":null,"OnBuild":null,"Labels":null},"container":"8e2caa5a514bb6d8b4f2a2553e9067498d261a0fd83a96aeaaf303943dff6ff9","container_config":{"Hostname":"8e2caa5a514b","Domainname":"","User":"","AttachStdin":false,"AttachStdout":false,"AttachStderr":false,"Tty":false,"OpenStdin":false,"StdinOnce":false,"Env":["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"],"Cmd":["/bin/sh","-c","#(nop) ","CMD [\"/hello\"]"],"ArgsEscaped":true,"Image":"sha256:a6d1aaad8ca65655449a26146699fe9d61240071f6992975be7e720f1cd42440","Volumes":null,"WorkingDir":"","Entrypoint":null,"OnBuild":null,"Labels":{}},"created":"2019-01-01T01:29:27.650294696Z","docker_version":"18.06.1-ce","history":[{"created":"2019-01-01T01:29:27.416803627Z","created_by":"/bin/sh -c #(nop) COPY file:f77490f70ce51da25bd21bfc30cb5e1a24b2b65eb37d4af0c327ddc24f0986a6 in / "},{"created":"2019-01-01T01:29:27.650294696Z","created_by":"/bin/sh -c #(nop)  CMD [\"/hello\"]","empty_layer":true}],"os":"linux","rootfs":{"type":"layers","diff_ids":["sha256:af0b15c8625bb1938f1d7b17081031f649fd14e6b233688eea3c5483994a66a3"]}}`

	fourth, err := os.Open("../../layer.tar")
	if err != nil {
		t.Fatalf("failed %v", err)
	}

	spitError := func(w http.ResponseWriter, status int) {
		w.WriteHeader(status)
		errMsg := `{"errors": [{ "code": "TOOMANYREQUESTS", "message": "Tenant rate limit exceeded. reqRatePerScond: 200" }]}`
		w.Write([]byte(errMsg))
		// TENANT_REQ_RATE_EXCEEDED
		//
		//TenantReqRateExceeded TOOMANYREQUESTS
	}

	if first == "" || second == "" || third == "" {
	}
	if spitError == nil {
	}
	if ctx.Err() != nil {
		io.Copy(nil, nil)
	}

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logrus.Warnf("from=%s method=%s proto=%s host=%s url=%s", r.RemoteAddr, r.Method, r.Proto, r.Host, r.URL.String())

		if r.URL.String() == "/v2/" {
			w.WriteHeader(200)
			return
		}

		if r.URL.String() == "/v2/registry-1.docker.io/hello-world/manifests/latest" {
			w.Header().Set("Content-Type", `application/vnd.docker.distribution.manifest.list.v2+json`)
			w.Header().Set("Docker-Content-Digest", `sha256:2557e3c07ed1e38f26e389462d03ed943586f744621577a99efb77324b0fe535`)
			w.Header().Set("Docker-Distribution-Api-Version", `registry/2.0`)
			w.WriteHeader(200)
			w.Write([]byte(first))
			return
		}

		if r.URL.String() == `/v2/registry-1.docker.io/hello-world/manifests/sha256:92c7f9c92844bbbb5d0a101b22f7c2a7949e40f8ea90c8b3bc396879d95e899a` {
			w.Header().Set("Content-Type", `application/vnd.docker.distribution.manifest.v2+json`)
			w.Header().Set("Docker-Content-Digest", `sha256:92c7f9c92844bbbb5d0a101b22f7c2a7949e40f8ea90c8b3bc396879d95e899a`)
			w.Header().Set("Docker-Distribution-Api-Version", `registry/2.0`)
			w.WriteHeader(200)
			w.Write([]byte(second))
			return
		}

		if r.URL.String() == `/v2/registry-1.docker.io/hello-world/blobs/sha256:fce289e99eb9bca977dae136fbe2a82b6b7d4c372474c9235adc1741675f587e` {
			w.WriteHeader(200)
			w.Write([]byte(third))
			return
		}

		if r.URL.String() == `/v2/registry-1.docker.io/hello-world/blobs/sha256:1b930d010525941c1d56ec53b97bd057a67ae1865eebf042686d2a2d18271ced` {
			//			spitError(w, 429)
			//			return
			w.Header().Set("Content-Type", `application/octet-stream`)
			w.WriteHeader(200)
			io.Copy(w, fourth)
			return
		}

		w.WriteHeader(500)
		return
	}))

	closer := func() {
		cancel()
		srv.Close()
		fourth.Close()
	}

	return srv, closer

}

func getHungDocker() (*httptest.Server, func()) {
	hung, cancel := context.WithCancel(context.Background())

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// version check seem to have a sane timeout in docker, let's serve this, then stop
		if r.URL.String() == "/v2/" {
			w.WriteHeader(200)
			return
		}
		<-hung.Done()
	}))

	closer := func() {
		cancel()
		srv.Close()
	}

	return srv, closer
}

func getApp() *models.App {
	return &models.App{ID: id.New().String()}
}

func getFn(initDelayMsecs int) *models.Fn {
	fn := &models.Fn{
		ID:    id.New().String(),
		Image: "fnproject/fn-test-utils",
		ResourceConfig: models.ResourceConfig{
			Timeout:     10,
			IdleTimeout: 60,
			Memory:      128, // only 1 fit in 160MB
		},
	}
	if initDelayMsecs > 0 {
		fn.Config = models.Config{"ENABLE_INIT_DELAY_MSEC": strconv.FormatUint(uint64(initDelayMsecs), 10)}
	}
	return fn
}

// simple GetCall/Submit combo.
func execFn(input string, fn *models.Fn, app *models.App, a Agent, tmsec int) error {

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(tmsec)*time.Millisecond)
	defer cancel()

	url := "http://127.0.0.1:8080/invoke/" + fn.ID

	req, err := http.NewRequest("GET", url, &dummyReader{Reader: strings.NewReader(input)})
	if err != nil {
		return err
	}

	req = req.WithContext(ctx)

	var out bytes.Buffer
	callI, err := a.GetCall(FromHTTPFnRequest(app, fn, req), WithWriter(&out))
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}

	err = a.Submit(callI)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func TestBadContainer1(t *testing.T) {
	a, err := getAgent()
	if err != nil {
		t.Fatal("cannot create agent")
	}
	defer checkClose(t, a)

	fn := getFn(0)
	fn.Config = models.Config{"ENABLE_INIT_EXIT": "0"}

	err = execFn(`{"sleepTime": 8000}`, fn, getApp(), a, 20000)
	if err != models.ErrContainerInitFail {
		t.Fatalf("submit unexpected error! %v", err)
	}
}

func TestBadContainer2(t *testing.T) {
	a, err := getAgent()
	if err != nil {
		t.Fatal("cannot create agent")
	}
	defer checkClose(t, a)

	fn := getFn(0)
	fn.Config = models.Config{"ENABLE_INIT_EXIT": "0", "ENABLE_INIT_DELAY_MSEC": "200"}

	err = execFn(`{"sleepTime": 8000}`, fn, getApp(), a, 20000)
	if err != models.ErrContainerInitFail {
		t.Fatalf("submit unexpected error! %v", err)
	}
}

func TestBadContainer3(t *testing.T) {
	a, err := getAgent()
	if err != nil {
		t.Fatal("cannot create agent")
	}
	defer checkClose(t, a)

	fn := getFn(0)

	err = execFn(`{"isCrash": true }`, fn, getApp(), a, 20000)
	if err != models.ErrFunctionResponse {
		t.Fatalf("submit unexpected error! %v", err)
	}
}

func TestBadContainer4(t *testing.T) {
	a, err := getAgent()
	if err != nil {
		t.Fatal("cannot create agent")
	}
	defer checkClose(t, a)

	fn := getFn(0)

	err = execFn(`{"isExit": true, "exitCode": 0 }`, fn, getApp(), a, 20000)
	if err != models.ErrFunctionResponse {
		t.Fatalf("submit unexpected error! %v", err)
	}
}

// Eviction will NOT take place since the first container is busy
func TestPlainNoEvict(t *testing.T) {
	a, err := getAgent()
	if err != nil {
		t.Fatal("cannot create agent")
	}
	defer checkClose(t, a)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		err := execFn(`{"sleepTime": 8000}`, getFn(0), getApp(), a, 20000)
		if err != nil {
			t.Fatalf("submit should not error! %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		time.Sleep(3000 * time.Millisecond)
		err := execFn(`{"sleepTime": 0}`, getFn(0), getApp(), a, 20000)
		if err != models.ErrCallTimeoutServerBusy {
			t.Fatalf("unexpected error %v", err)
		}
	}()

	wg.Wait()
}

// Eviction will take place since the first container is idle
func TestPlainDoEvict(t *testing.T) {
	a, err := getAgent()
	if err != nil {
		t.Fatal("cannot create agent")
	}
	defer checkClose(t, a)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		err := execFn(`{"sleepTime": 0}`, getFn(0), getApp(), a, 20000)
		if err != nil {
			t.Fatalf("submit should not error! %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		time.Sleep(3000 * time.Millisecond)
		err := execFn(`{"sleepTime": 0}`, getFn(0), getApp(), a, 20000)
		if err != nil {
			t.Fatalf("submit should not error! %v", err)
		}
	}()

	wg.Wait()
}

func TestHungFDKNoEvict(t *testing.T) {
	a, err := getAgent()
	if err != nil {
		t.Fatal("cannot create agent")
	}
	defer checkClose(t, a)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		err := execFn(`{"sleepTime": 0}`, getFn(11000), getApp(), a, 20000)
		if err != models.ErrContainerInitTimeout {
			t.Fatalf("submit unexpected error! %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		time.Sleep(3000 * time.Millisecond)
		err := execFn(`{"sleepTime": 0}`, getFn(0), getApp(), a, 20000)
		if err != models.ErrCallTimeoutServerBusy {
			t.Fatalf("unexpected error %v", err)
		}
	}()

	wg.Wait()
}

func TestDockerPullHungNoEvict(t *testing.T) {
	dockerSrv, dockerCancel := getHungDocker()
	defer dockerCancel()

	a, err := getAgent()
	if err != nil {
		t.Fatal("cannot create agent")
	}
	defer checkClose(t, a)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		fn := getFn(0)
		fn.Image = strings.TrimPrefix(dockerSrv.URL, "http://") + "/" + fn.Image

		err := execFn(`{"sleepTime": 0}`, fn, getApp(), a, 20000)
		if err != models.ErrDockerPullTimeout {
			t.Fatalf("unexpected error %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		time.Sleep(3000 * time.Millisecond)
		err := execFn(`{"sleepTime": 0}`, getFn(0), getApp(), a, 20000)
		if err != models.ErrCallTimeoutServerBusy {
			t.Fatalf("unexpected error %v", err)
		}
	}()

	wg.Wait()

}

func TestDockerPullFake(t *testing.T) {
	formatter := &logrus.TextFormatter{
		FullTimestamp: true,
	}
	formatter.TimestampFormat = time.RFC3339Nano
	logrus.SetFormatter(formatter)
	logrus.SetLevel(logrus.DebugLevel)

	dockerSrv, dockerCancel := getFakeDocker(t)
	defer dockerCancel()

	a, err := getAgent()
	if err != nil {
		t.Fatal("cannot create agent")
	}
	defer checkClose(t, a)

	start := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		fn := getFn(0)
		fn.Timeout = 10
		fn.Memory = 64
		fn.Image = strings.TrimPrefix(dockerSrv.URL, "https://") + "/registry-1.docker.io/hello-world:latest"

		<-start

		err := execFn(`{"sleepTime": 0}`, fn, getApp(), a, 400000)
		if err != models.ErrDockerPullTimeout {
			t.Fatalf("unexpected error %v", err)
		}
	}()

	go func() {
		defer wg.Done()

		fn := getFn(0)
		fn.Timeout = 11
		fn.Memory = 64
		fn.Image = strings.TrimPrefix(dockerSrv.URL, "https://") + "/registry-1.docker.io/hello-world:latest"

		<-start

		err := execFn(`{"sleepTime": 0}`, fn, getApp(), a, 400000)
		if err != models.ErrDockerPullTimeout {
			t.Fatalf("unexpected error %v", err)
		}
	}()

	start <- struct{}{}
	start <- struct{}{}

	wg.Wait()

}
