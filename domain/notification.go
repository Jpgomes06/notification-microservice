package domain

type Notification struct {
	UserID    string `json:"user_id" bson:"user_id"`
	Message   string `json:"message" bson:"message"`
	Type      string `json:"type" bson:"type"`
	CreatedAt string `json:"created_at" bson:"created_at"`
}
