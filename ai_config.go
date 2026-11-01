package lucida

import "regexp"

// AIMessage is one prompt message; Role is "system", "user" or "assistant".
type AIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// AIConfig is the model and prompt an AI Config flag (LC-FLG-25) serves: a multivariate flag whose
// variations carry a versioned model and prompt. The SDK only selects and renders the variation.
type AIConfig struct {
	// Enabled is false when the flag served nothing (off, unknown); the fallback is then returned.
	Enabled    bool           `json:"enabled"`
	Variation  string         `json:"variation,omitempty"`
	Version    *float64       `json:"version,omitempty"`
	Model      string         `json:"model"`
	Provider   string         `json:"provider,omitempty"`
	Messages   []AIMessage    `json:"messages"`
	Parameters map[string]any `json:"parameters"`
}

var aiVar = regexp.MustCompile(`\{\{\s*([\w.-]+)\s*\}\}`)

func aiFill(s string, vars map[string]string) string {
	return aiVar.ReplaceAllStringFunc(s, func(m string) string {
		if v, ok := vars[aiVar.FindStringSubmatch(m)[1]]; ok {
			return v
		}
		return m
	})
}

// AIConfigFrom returns the AI config served by r (nil = not evaluated) with `{{var}}` placeholders in
// messages filled from vars. Without a served variation whose payload names a model, fallback is
// returned with Enabled false.
func AIConfigFrom(r *FlagResult, fallback AIConfig, vars map[string]string) AIConfig {
	out := fallback
	out.Enabled = false
	var p map[string]any
	if r != nil && r.Variant != "" {
		p, _ = r.Payload.(map[string]any)
	}
	if model, ok := p["model"].(string); ok {
		out = AIConfig{Enabled: true, Variation: r.Variant, Model: model, Parameters: map[string]any{}}
		if v, ok := p["version"].(float64); ok {
			out.Version = &v
		}
		out.Provider, _ = p["provider"].(string)
		if params, ok := p["parameters"].(map[string]any); ok {
			out.Parameters = params
		}
		ms, _ := p["messages"].([]any)
		for _, m := range ms {
			mm, _ := m.(map[string]any)
			role, _ := mm["role"].(string)
			content, _ := mm["content"].(string)
			out.Messages = append(out.Messages, AIMessage{role, content})
		}
	}
	msgs := make([]AIMessage, len(out.Messages))
	for i, m := range out.Messages {
		msgs[i] = AIMessage{m.Role, aiFill(m.Content, vars)}
	}
	out.Messages = msgs
	return out
}
