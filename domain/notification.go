package domain

type Notification struct {
	UserID    string `json:"user_id"`
	Message   string `json:"message"`
	Type      string `json:"type"`
	CreatedAt string `json:"created_at"`
}
