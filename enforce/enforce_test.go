package enforce

import "testing"

func TestEnforceNil(t *testing.T) {
	ENFORCE(nil, "Should allow nil to pass")
}
