package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "fvs2/internal/controlpb"
)

// controlSocketDir is where per-mount control sockets and logs are kept.
func controlSocketDir() string {
	if x := os.Getenv("XDG_RUNTIME_DIR"); x != "" {
		return filepath.Join(x, "fvs2d")
	}
	return filepath.Join(os.TempDir(), "fvs2d")
}

// socketForMount derives a deterministic control socket path from a mountpoint,
// so `mount` and `unmount` agree on the endpoint without any shared state.
func socketForMount(mountpoint string) (string, error) {
	abs, err := filepath.Abs(mountpoint)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(abs))
	return filepath.Join(controlSocketDir(), hex.EncodeToString(sum[:8])+".sock"), nil
}

// dialControl connects to a daemon control endpoint at the given unix socket.
func dialControl(sock string) (pb.ControlClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(
		"passthrough:///"+sock,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		}),
	)
	if err != nil {
		return nil, nil, err
	}
	return pb.NewControlClient(conn), conn, nil
}

// spawnDaemon starts fvs2d for a single mount, detaches it, and waits until it
// reports healthy. Daemon output goes to a per-mount log next to the socket.
func spawnDaemon(bin string, args []string, sock string, timeout time.Duration) error {
	if err := os.MkdirAll(controlSocketDir(), 0o700); err != nil {
		return err
	}

	logPath := strings.TrimSuffix(sock, ".sock") + ".log"
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("open daemon log: %w", err)
	}
	defer logFile.Close()

	cmd := exec.Command(bin, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true} // detach into its own session
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start %s: %w", bin, err)
	}
	// Let the daemon outlive this CLI process.
	if err := cmd.Process.Release(); err != nil {
		return err
	}

	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		client, conn, derr := dialControl(sock)
		if derr == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			resp, herr := client.Health(ctx, &pb.HealthRequest{})
			cancel()
			conn.Close()
			if herr == nil && resp.GetOk() {
				return nil
			}
			lastErr = herr
		} else {
			lastErr = derr
		}
		time.Sleep(100 * time.Millisecond)
	}
	if lastErr != nil {
		return fmt.Errorf("daemon did not become healthy within %s (see %s): %w", timeout, logPath, lastErr)
	}
	return fmt.Errorf("daemon did not become healthy within %s (see %s)", timeout, logPath)
}

// shutdownDaemon asks the daemon owning sock to unmount and exit.
func shutdownDaemon(sock string, lazy bool) error {
	client, conn, err := dialControl(sock)
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.Shutdown(ctx, &pb.ShutdownRequest{Lazy: lazy})
	if err != nil {
		return err
	}
	if !resp.GetOk() {
		return fmt.Errorf("daemon reported shutdown failure")
	}
	return nil
}
