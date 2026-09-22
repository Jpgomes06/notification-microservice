package handler

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestParseAndValidateRequest(t *testing.T) {
	tests := []struct {
		name             string
		body             string
		wantStatusCode   int
		wantErrMsg       string
		checkScheduledAt func(t *testing.T, scheduledAt *time.Time)
	}{
		{
			name:           "invalid json body",
			body:           `{invalid-json`,
			wantStatusCode: http.StatusBadRequest,
			wantErrMsg:     "invalid request body",
		},
		{
			name:           "missing user_id",
			body:           `{"message": "hello", "type": "web"}`,
			wantStatusCode: http.StatusBadRequest,
			wantErrMsg:     "user_id and message are required",
		},
		{
			name:           "missing message",
			body:           `{"user_id": "123", "type": "web"}`,
			wantStatusCode: http.StatusBadRequest,
			wantErrMsg:     "user_id and message are required",
		},
		{
			name:           "missing type",
			body:           `{"user_id": "123", "message": "hello"}`,
			wantStatusCode: http.StatusBadRequest,
			wantErrMsg:     "type is required",
		},
		{
			name:           "invalid notification type",
			body:           `{"user_id": "123", "message": "hello", "type": "pigeon"}`,
			wantStatusCode: http.StatusBadRequest,
			wantErrMsg:     "invalid notification type",
		},
		{
			name:           "valid immediate notification (no scheduled_at)",
			body:           `{"user_id": "123", "message": "hello", "type": "web"}`,
			wantStatusCode: http.StatusOK,
			checkScheduledAt: func(t *testing.T, scheduledAt *time.Time) {
				if scheduledAt != nil {
					t.Errorf("expected no scheduled_at for immediate notification, got %v", *scheduledAt)
				}
			},
		},
		{
			name:           "invalid scheduled_at format (no timezone)",
			body:           `{"user_id": "123", "message": "hello", "type": "web", "scheduled_at": "2026-09-25 10:00:00"}`,
			wantStatusCode: http.StatusBadRequest,
			wantErrMsg:     "must be in RFC3339 format with explicit timezone",
		},
		{
			name:           "past scheduled_at",
			body:           `{"user_id": "123", "message": "hello", "type": "web", "scheduled_at": "2020-01-01T10:00:00Z"}`,
			wantStatusCode: http.StatusBadRequest,
			wantErrMsg:     "date/time cannot be in the past",
		},
		{
			name:           "valid future scheduled_at",
			body:           `{"user_id": "123", "message": "hello", "type": "web", "scheduled_at": "2099-01-01T12:00:00Z"}`,
			wantStatusCode: http.StatusOK,
			checkScheduledAt: func(t *testing.T, scheduledAt *time.Time) {
				expected, _ := time.Parse(time.RFC3339, "2099-01-01T12:00:00Z")
				if scheduledAt == nil || !scheduledAt.Equal(expected) {
					t.Errorf("expected scheduled_at %v, got %v", expected, scheduledAt)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/send-notification", bytes.NewBufferString(tt.body))
			notification, status, err := ParseAndValidateRequest(req)

			if tt.wantStatusCode != http.StatusOK {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if status != tt.wantStatusCode {
					t.Errorf("expected status %d, got %d", tt.wantStatusCode, status)
				}
				if tt.wantErrMsg != "" && !bytes.Contains([]byte(err.Error()), []byte(tt.wantErrMsg)) {
					t.Errorf("expected error containing %q, got %q", tt.wantErrMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				if tt.checkScheduledAt != nil {
					tt.checkScheduledAt(t, notification.ScheduledAt)
				}
			}
		})
	}
}
