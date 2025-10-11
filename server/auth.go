package main

import (
	"errors"
	"os"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	jwt "github.com/golang-jwt/jwt/v5"
)

// Minimal JWT implementation (HS256) using stdlib to avoid external deps.
var (
	jwtSecretKey = []byte("please-change-this-secret")
	jwtExpiry    = int64(3600)
)

func init() {
	// Attempt to read from ini if loaded
	if ini_file != nil {
		s := ini_string("jwt", "secret", "")
		if s != "" {
			jwtSecretKey = []byte(s)
		}
		e := ini_int("jwt", "expiry", int(jwtExpiry))
		jwtExpiry = int64(e)
	}

	// Environment variable fallback (used if ini values are not present)
	// This allows runtime overrides without changing the config file.
	if len(jwtSecretKey) == 0 || string(jwtSecretKey) == "please-change-this-secret" {
		if v := os.Getenv("MOCHI_JWT_SECRET"); v != "" {
			jwtSecretKey = []byte(v)
		}
	}
	if jwtExpiry == 0 {
		if v := os.Getenv("MOCHI_JWT_EXPIRY"); v != "" {
			if i, err := strconv.Atoi(v); err == nil {
				jwtExpiry = int64(i)
			}
		}
	}
}

type MochiClaims struct {
	User int `json:"user"`
	jwt.RegisteredClaims
}

// Create a JWT for a user id using github.com/golang-jwt/jwt/v5
func jwt_create(userID int) (string, error) {
	claims := MochiClaims{
		User: userID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Unix(now()+jwtExpiry, 0)),
			IssuedAt:  jwt.NewNumericDate(time.Unix(now(), 0)),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(jwtSecretKey)
	if err != nil {
		return "", err
	}
	return signed, nil
}

// Verify a JWT and return the user id, or -1 if invalid
func jwt_verify(tokenString string) (int, error) {
	var claims MochiClaims
	tkn, err := jwt.ParseWithClaims(tokenString, &claims, func(token *jwt.Token) (interface{}, error) {
		return jwtSecretKey, nil
	})
	if err != nil {
		return -1, err
	}
	if !tkn.Valid {
		return -1, errors.New("invalid token")
	}
	return claims.User, nil
}

// API handler: exchange a login code for a JWT token
func api_login_auth(c *gin.Context) {
	var body struct {
		Code string `json:"code"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(400, gin.H{"error": "invalid request"})
		return
	}
	if body.Code == "" {
		c.JSON(400, gin.H{"error": "missing code"})
		return
	}
	user := user_from_code(body.Code)
	if user == nil {
		c.JSON(401, gin.H{"error": "invalid code"})
		return
	}
	token, err := jwt_create(user.ID)
	if err != nil {
		c.JSON(500, gin.H{"error": "unable to create token"})
		return
	}
	// keep legacy login token for compatibility
	login := login_create(user.ID)
	c.JSON(200, gin.H{"token": token, "login": login})
}
