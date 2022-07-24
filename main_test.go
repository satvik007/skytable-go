package skytable_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/satvik007/skytable-go"
)

const (
	skytablePort = "2003"
	skytableAddr = "localhost:" + skytablePort
)

var (
	processes    map[string]*skytableProcess
	skytableMain *skytableProcess
)

func registerProcess(port string, p *skytableProcess) {
	if processes == nil {
		processes = make(map[string]*skytableProcess)
	}
	processes[port] = p
}

var _ = BeforeSuite(func() {
	// var err error

	// skytableMain, err = startSkytable(skytablePort)
	// Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	for _, p := range processes {
		Expect(p.Close()).NotTo(HaveOccurred())
	}
	processes = nil
})

func TestGinkgoSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "skytable-go")
}

// ------------------------------------------------------------------------------

func skytableOptions() *skytable.Options {
	return &skytable.Options{
		Addr:  skytableAddr,
		Table: "test15",

		DialTimeout:  10 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,

		MaxRetries: -1,

		PoolSize:           10,
		PoolTimeout:        30 * time.Second,
		IdleTimeout:        time.Minute,
		IdleCheckFrequency: 100 * time.Millisecond,
	}
}

func performAsync(n int, cbs ...func(int)) *sync.WaitGroup {
	var wg sync.WaitGroup
	for _, cb := range cbs {
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func(cb func(int), i int) {
				defer GinkgoRecover()
				defer wg.Done()

				cb(i)
			}(cb, i)
		}
	}
	return &wg
}

func perform(n int, cbs ...func(int)) {
	wg := performAsync(n, cbs...)
	wg.Wait()
}

func eventually(fn func() error, timeout time.Duration) error {
	errCh := make(chan error, 1)
	done := make(chan struct{})
	exit := make(chan struct{})

	go func() {
		for {
			err := fn()
			if err == nil {
				close(done)
				return
			}

			select {
			case errCh <- err:
			default:
			}

			select {
			case <-exit:
				return
			case <-time.After(timeout / 100):
			}
		}
	}()

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		close(exit)
		select {
		case err := <-errCh:
			return err
		default:
			return fmt.Errorf("timeout after %s without an error", timeout)
		}
	}
}

func execCmd(name string, args ...string) (*os.Process, error) {
	cmd := exec.Command(name, args...)
	if testing.Verbose() {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}
	return cmd.Process, cmd.Start()
}

func connectTo(port string) (*skytable.Client, error) {
	client := skytable.NewClient(&skytable.Options{
		Addr:       ":" + port,
		MaxRetries: -1,
	})

	err := eventually(func() error {
		return client.Heya(ctx, "").Err()
	}, 30*time.Second)
	if err != nil {
		return nil, err
	}

	return client, nil
}

type skytableProcess struct {
	*os.Process
	*skytable.Client
}

func (p *skytableProcess) Close() error {
	if err := p.Kill(); err != nil {
		return err
	}

	err := eventually(func() error {
		if err := p.Client.Heya(ctx, "").Err(); err != nil {
			return nil
		}
		return errors.New("client is not shutdown")
	}, 10*time.Second)
	if err != nil {
		return err
	}

	p.Client.Close()
	return nil
}

var (
	skytableServerBin, _ = filepath.Abs(filepath.Join("/usr", "bin", "skyd"))
	// skytableServerConf, _ = filepath.Abs(filepath.Join("testdata", "skytable", "skytable.conf"))
)

// func skytableDir(port string) (string, error) {
// 	dir, err := filepath.Abs(filepath.Join("testdata", "instances", port))
// 	if err != nil {
// 		return "", err
// 	}
// 	if err := os.RemoveAll(dir); err != nil {
// 		return "", err
// 	}
// 	if err := os.MkdirAll(dir, 0o775); err != nil {
// 		return "", err
// 	}
// 	return dir, nil
// }

func startSkytable(port string, args ...string) (*skytableProcess, error) {
	// dir, err := skytableDir(port)
	// if err != nil {
	// 	return nil, err
	// }
	//
	// if err := exec.Command("cp", "-f", skytableServerConf, dir).Run(); err != nil {
	// 	return nil, err
	// }

	// baseArgs := []string{filepath.Join(dir, "skytable.conf"), "--port", port, "--dir", dir}

	var baseArgs []string
	process, err := execCmd(skytableServerBin, append(baseArgs, args...)...)
	if err != nil {
		return nil, err
	}

	client, err := connectTo(port)
	if err != nil {
		process.Kill()
		return nil, err
	}

	_ = client.CreateTable(ctx, "test15", "keymap", []string{"str", "binstr"}).Err()

	p := &skytableProcess{process, client}
	registerProcess(port, p)
	return p, nil
}

// ------------------------------------------------------------------------------

type badConnError string

func (e badConnError) Error() string   { return string(e) }
func (e badConnError) Timeout() bool   { return true }
func (e badConnError) Temporary() bool { return false }

type badConn struct {
	net.TCPConn

	readDelay, writeDelay time.Duration
	readErr, writeErr     error
}

var _ net.Conn = &badConn{}

func (cn *badConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (cn *badConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func (cn *badConn) Read([]byte) (int, error) {
	if cn.readDelay != 0 {
		time.Sleep(cn.readDelay)
	}
	if cn.readErr != nil {
		return 0, cn.readErr
	}
	return 0, badConnError("bad connection")
}

func (cn *badConn) Write([]byte) (int, error) {
	if cn.writeDelay != 0 {
		time.Sleep(cn.writeDelay)
	}
	if cn.writeErr != nil {
		return 0, cn.writeErr
	}
	return 0, badConnError("bad connection")
}

// ------------------------------------------------------------------------------

type hook struct {
	beforeProcess func(ctx context.Context, cmd skytable.Cmder) (context.Context, error)
	afterProcess  func(ctx context.Context, cmd skytable.Cmder) error

	beforeProcessPipeline func(ctx context.Context, cmds []skytable.Cmder) (context.Context, error)
	afterProcessPipeline  func(ctx context.Context, cmds []skytable.Cmder) error
}

func (h *hook) BeforeProcess(ctx context.Context, cmd skytable.Cmder) (context.Context, error) {
	if h.beforeProcess != nil {
		return h.beforeProcess(ctx, cmd)
	}
	return ctx, nil
}

func (h *hook) AfterProcess(ctx context.Context, cmd skytable.Cmder) error {
	if h.afterProcess != nil {
		return h.afterProcess(ctx, cmd)
	}
	return nil
}

func (h *hook) BeforeProcessPipeline(ctx context.Context, cmds []skytable.Cmder) (context.Context, error) {
	if h.beforeProcessPipeline != nil {
		return h.beforeProcessPipeline(ctx, cmds)
	}
	return ctx, nil
}

func (h *hook) AfterProcessPipeline(ctx context.Context, cmds []skytable.Cmder) error {
	if h.afterProcessPipeline != nil {
		return h.afterProcessPipeline(ctx, cmds)
	}
	return nil
}
