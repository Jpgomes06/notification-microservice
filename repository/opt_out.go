package repository

import "math/rand/v2"

type RandomOptOutChecker struct{}

func (RandomOptOutChecker) IsOptedOut(userID string) (bool, error) {
	return rand.IntN(2) == 1, nil
}
