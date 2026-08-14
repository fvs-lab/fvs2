//go:build !linux

package repo

func setOverlayWhiteout(string) error {
	return ErrOverlayWhiteoutsUnsupported
}

func setOverlayOpaque(string, string) error {
	return ErrOverlayWhiteoutsUnsupported
}

func verifyOverlayWhiteout(string) error {
	return ErrOverlayWhiteoutsUnsupported
}

func verifyOverlayOpaque(string, string) error {
	return ErrOverlayWhiteoutsUnsupported
}
