package main

import (
	"errors"
	"time"

	"github.com/gin-gonic/gin"
	jwt "github.com/golang-jwt/jwt/v5"
)

// Minimal JWT implementation (HS256) using stdlib to avoid external deps.
var (
	jwtExpiry = int64(3600)
)

func init() {
	// Attempt to read from ini if loaded
	if ini_file != nil {
		e := ini_int("jwt", "expiry", int(jwtExpiry))
		jwtExpiry = int64(e)
	}
	if jwtExpiry == 0 {
		if v := ini_string("jwt", "expiry", ""); v != "" {
			// ini_int already handled the numeric case above; leave as-is
		}
	}
}

type MochiClaims struct {
	User int `json:"user"`
	jwt.RegisteredClaims
}

// Create a JWT using a specific HMAC secret
func jwt_create_with_secret(userID int, secret []byte) (string, error) {
	claims := MochiClaims{
		User: userID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Unix(now()+jwtExpiry, 0)),
			IssuedAt:  jwt.NewNumericDate(time.Unix(now(), 0)),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(secret)
	if err != nil {
		return "", err
	}
	return signed, nil
}

// Verify a JWT and return the user id, or -1 if invalid.
// If the token header contains a "kid" referencing a login code, attempt to verify
// using that login's secret. Otherwise fall back to the global secret.
func jwt_verify(tokenString string) (int, error) {
	// First parse the token without verification to read header/kid
	token, _, err := new(jwt.Parser).ParseUnverified(tokenString, &MochiClaims{})
	if err != nil {
		return -1, err
	}

	// Require kid header (login code) to look up per-login secret
	kid, ok := token.Header["kid"].(string)
	if !ok || kid == "" {
		return -1, errors.New("token missing kid header referencing login code")
	}
	var l Login
	db := db_open("db/users.db")
	if !db.scan(&l, "select * from logins where code=? and expires>=?", kid, now()) {
		return -1, errors.New("login not found for kid")
	}
	if l.Secret == "" {
		return -1, errors.New("login has no secret")
	}
	secret := []byte(l.Secret)

	var claims MochiClaims
	tkn, err := jwt.ParseWithClaims(tokenString, &claims, func(token *jwt.Token) (interface{}, error) {
		return secret, nil
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
	// create a legacy login entry (per-device) which stores a per-login secret
	login := login_create(user.ID)

	// create a JWT signed with the per-login secret and include the login code in the kid header
	var l Login
	db := db_open("db/users.db")
	if !db.scan(&l, "select * from logins where code=? and expires>=?", login, now()) {
		c.JSON(500, gin.H{"error": "unable to find login after creation"})
		return
	}

	// Use the per-login secret; it must be present
	if l.Secret == "" {
		c.JSON(500, gin.H{"error": "login has no secret; re-authenticate"})
		return
	}
	secret := []byte(l.Secret)

	// Build token with kid header set to the login code so verification can find the secret
	claims := MochiClaims{
		User: user.ID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Unix(now()+jwtExpiry, 0)),
			IssuedAt:  jwt.NewNumericDate(time.Unix(now(), 0)),
		},
	}
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	t.Header["kid"] = login
	signed, err := t.SignedString(secret)
	if err != nil {
		c.JSON(500, gin.H{"error": "unable to create token"})
		return
	}

	c.JSON(200, gin.H{"token": signed, "login": login})
}