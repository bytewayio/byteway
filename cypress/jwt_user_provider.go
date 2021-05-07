package cypress

import (
	"crypto/rsa"
	"net/http"
	"strings"

	"go.uber.org/zap"
	"gopkg.in/square/go-jose.v2/jwt"
)

const (
	DefaultJwtProviderName = "JWT"
	bearerAuthPrefix       = "Bearer "
)

// JwtUserPrincipal a user principal created from JWT token
type JwtUserPrincipal struct {
	UserPrincipal
	SecurityToken string
	DisplayName   string
	Parent        string
	Source        string
}

// UserPrincipalLoader loads a user principal by user domain and id
type UserPrincipalLoader interface {
	Load(domain, id string) *UserPrincipal
}

// UserPrincipalLoaderFunc delegates function to UserPrincipalLoader interface
type UserPrincipalLoaderFunc func(domain, id string) *UserPrincipal

func (f UserPrincipalLoaderFunc) Load(domain, id string) *UserPrincipal {
	return f(domain, id)
}

// JwtUserClaims Microsoft claims spec compatible user claims
type JwtUserClaims struct {
	jwt.Claims
	Sid           string   `json:"http://schemas.xmlsoap.org/ws/2005/05/identity/claims/sid,omitempty"`
	Name          string   `json:"http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name,omitempty"`
	AccountName   string   `json:"http://schemas.xmlsoap.org/ws/2005/05/identity/claims/upn,omitempty"`
	Roles         []string `json:"http://schemas.microsoft.com/ws/2008/06/identity/claims/role,omitempty"`
	Domain        string   `json:"TenantId,omitempty"`
	SecurityToken string   `json:"sectok,omitempty"`
	Parent        string   `json:"ParentAccountName,omitempty"`
}

func (claims *JwtUserClaims) toUserPrincipal() *UserPrincipal {
	jwtPrincipal := &JwtUserPrincipal{
		UserPrincipal: UserPrincipal{
			ID:     claims.Sid,
			Domain: claims.Domain,
			Name:   claims.AccountName,
			Roles:  claims.Roles,
		},
		SecurityToken: claims.SecurityToken,
		DisplayName:   claims.Name,
		Parent:        claims.Parent,
		Source:        claims.Issuer,
	}

	jwtPrincipal.Self = jwtPrincipal
	return &jwtPrincipal.UserPrincipal
}

// JwtKeyProvider RSA public key provider for retrieving issuer public keys
type JwtKeyProvider interface {
	GetKey(issuer string) *rsa.PublicKey
}

// JwtKeyMap maps issuer to a public key
type JwtKeyMap map[string]*rsa.PublicKey

func (m JwtKeyMap) GetKey(issuer string) *rsa.PublicKey {
	key, ok := m[issuer]
	if !ok {
		return nil
	}

	return key
}

// JwtUserProvider jwt token based user provider
type JwtUserProvider struct {
	keyProvider JwtKeyProvider
	userLoader  UserPrincipalLoader
}

// NewJwtUserProvider creates a new instances of jwt user provider
func NewJwtUserProvider(keyProvider JwtKeyProvider, userLoader UserPrincipalLoader) *JwtUserProvider {
	return &JwtUserProvider{
		keyProvider: keyProvider,
		userLoader:  userLoader,
	}
}

func (provider *JwtUserProvider) GetName() string {
	return DefaultJwtProviderName
}

func (provider *JwtUserProvider) Load(domain, id string) *UserPrincipal {
	if provider.userLoader != nil {
		return provider.userLoader.Load(domain, id)
	}

	return nil
}

func (provider *JwtUserProvider) Authenticate(request *http.Request) *UserPrincipal {
	authHeader := strings.TrimSpace(request.Header.Get("Authorization"))
	if len(authHeader) > 0 && strings.HasPrefix(authHeader, bearerAuthPrefix) {
		authToken := authHeader[len(bearerAuthPrefix):]
		token, err := jwt.ParseSigned(authToken)
		if err != nil {
			zap.L().Warn("failed to parse signed jwt token", zap.String("token", authToken), zap.Error(err))
			return nil
		}

		defaultClaims := new(jwt.Claims)
		if err = token.UnsafeClaimsWithoutVerification(defaultClaims); err == nil {
			keyNames := []string{defaultClaims.Issuer, defaultClaims.Issuer + "_backup"}
			for _, keyName := range keyNames {
				if key := provider.keyProvider.GetKey(keyName); key != nil {
					claims := new(JwtUserClaims)
					if err = token.Claims(key, claims); err == nil {
						return claims.toUserPrincipal()
					} else {
						zap.L().Warn("failed to verify signature of jwt token", zap.String("key", keyName), zap.Error(err))
					}
				} else {
					zap.L().Warn("jwt key not found", zap.String("key", keyName))
				}
			}
		} else {
			zap.L().Warn("failed to parse claims from token", zap.String("token", authToken), zap.Error(err))
		}
	}

	return nil
}
