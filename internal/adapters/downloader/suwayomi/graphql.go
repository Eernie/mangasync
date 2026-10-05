package suwayomi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"mangasync/internal/httpx"
)

var errUnauthorized = errors.New("suwayomi: unauthorized")

const (
	loginMutation   = `mutation($u: String!, $p: String!) { login(input: {username: $u, password: $p}) { accessToken refreshToken } }`
	refreshMutation = `mutation($t: String!) { refreshToken(input: {refreshToken: $t}) { accessToken } }`
)

// gql runs a GraphQL operation. With ui_login, an unauthorized response triggers one
// token refresh (or a fresh login) and a retry.
func (c *Client) gql(ctx context.Context, query string, vars map[string]any, out any) error {
	err := c.gqlOnce(ctx, query, vars, out)
	if c.cfg.Auth == "ui_login" && errors.Is(err, errUnauthorized) {
		if rerr := c.reauth(ctx); rerr != nil {
			return fmt.Errorf("reauthenticate: %w", rerr)
		}
		err = c.gqlOnce(ctx, query, vars, out)
	}
	return err
}

func (c *Client) gqlOnce(ctx context.Context, query string, vars map[string]any, out any) error {
	var resp struct {
		Data   json.RawMessage `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	body := map[string]any{"query": query, "variables": vars}
	err := c.http.DoJSON(ctx, http.MethodPost, c.cfg.URL+"/api/graphql", c.authHeader(), body, &resp)
	if httpx.IsStatus(err, http.StatusUnauthorized) {
		return fmt.Errorf("%w: %v", errUnauthorized, err)
	}
	if err != nil {
		return err
	}
	if len(resp.Errors) > 0 {
		msgs := make([]string, 0, len(resp.Errors))
		for _, e := range resp.Errors {
			msgs = append(msgs, e.Message)
		}
		joined := strings.Join(msgs, "; ")
		if strings.Contains(strings.ToLower(joined), "unauthorized") {
			return fmt.Errorf("%w: %s", errUnauthorized, joined)
		}
		return fmt.Errorf("graphql: %s", joined)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(resp.Data, out)
}

func (c *Client) authHeader() http.Header {
	h := http.Header{}
	switch c.cfg.Auth {
	case "basic":
		h.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(c.cfg.User+":"+c.cfg.Pass)))
	case "ui_login":
		c.mu.Lock()
		if c.access != "" {
			h.Set("Authorization", "Bearer "+c.access)
		}
		c.mu.Unlock()
	}
	return h
}

func (c *Client) login(ctx context.Context) error {
	var out struct {
		Login struct {
			AccessToken  string `json:"accessToken"`
			RefreshToken string `json:"refreshToken"`
		} `json:"login"`
	}
	if err := c.gqlOnce(ctx, loginMutation, map[string]any{"u": c.cfg.User, "p": c.cfg.Pass}, &out); err != nil {
		return err
	}
	if out.Login.AccessToken == "" {
		return errors.New("login returned no access token")
	}
	c.mu.Lock()
	c.access, c.refresh = out.Login.AccessToken, out.Login.RefreshToken
	c.mu.Unlock()
	return nil
}

func (c *Client) reauth(ctx context.Context) error {
	c.mu.Lock()
	rt := c.refresh
	c.access = ""
	c.mu.Unlock()
	if rt != "" {
		var out struct {
			RefreshToken struct {
				AccessToken string `json:"accessToken"`
			} `json:"refreshToken"`
		}
		if err := c.gqlOnce(ctx, refreshMutation, map[string]any{"t": rt}, &out); err == nil && out.RefreshToken.AccessToken != "" {
			c.mu.Lock()
			c.access = out.RefreshToken.AccessToken
			c.mu.Unlock()
			return nil
		}
	}
	return c.login(ctx)
}
