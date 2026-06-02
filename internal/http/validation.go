package http

import "strings"

func isManifestValidationError(err error) bool {
	if err == nil {
		return false
	}

	msg := err.Error()
	return strings.Contains(msg, "invalid commit:") ||
		strings.Contains(msg, "commit too long:") ||
		strings.Contains(msg, "invalid changes-id:") ||
		strings.Contains(msg, "changes-id too long:") ||
		strings.Contains(msg, "invalid build-type:") ||
		strings.Contains(msg, "build-type too long:")
}
