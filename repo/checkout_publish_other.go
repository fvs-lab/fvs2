//go:build !linux

package repo

func publishCheckout(source, destination, previous string) error {
	return publishCheckoutFallback(source, destination, previous)
}
