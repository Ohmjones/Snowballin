// File: dataprovider/feargreed.go
// Relevant Succeeded: NewFearGreedClient, GetFearGreedIndex

package dataprovider

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"Snowballin/utilities"
)

// altFearGreedDataPoint models a single data point from alternative.me
type altFearGreedDataPoint struct {
	Value               string `json:"value"`
	ValueClassification string `json:"value_classification"`
	Timestamp           string `json:"timestamp"`
	TimeUntilUpdate     string `json:"time_until_update,omitempty"`
}

// altFearGreedResponse is the full API payload
type altFearGreedResponse struct {
	Name     string                  `json:"name"`
	Data     []altFearGreedDataPoint `json:"data"`
	Metadata struct {
		Error *string `json:"error,omitempty"`
	} `json:"metadata"`
}

// Client is a Fear & Greed data‐provider client.
type Client struct {
	HTTPClient *http.Client
	logger     *utilities.Logger
	apiURL     string
}

// NewFearGreedClient creates a new FearGreedProvider-compatible client explicitly matching app.go call.
func NewFearGreedClient(cfg *utilities.FearGreedConfig, logger *utilities.Logger, client *http.Client) (FearGreedProvider, error) {
	if cfg == nil { // Added nil check for robustness, though app.go checks cfg.FearGreed
		return nil, errors.New("FearGreedClient: FearGreedConfig cannot be nil")
	}
	if logger == nil { // Should be handled by caller, but as a safeguard
		logger = utilities.NewLogger(utilities.Info) // Use a default logger if nil
		logger.LogWarn("FearGreedClient: Logger not provided, using default.")
	}
	if client == nil { // Should be handled by caller
		return nil, errors.New("FearGreedClient: HTTPClient cannot be nil")
	}
	return &Client{
		HTTPClient: client,
		logger:     logger,
		apiURL:     cfg.BaseURL + "/fng/?limit=1&format=json",
	}, nil
}

// GetFearGreedIndex fetches the current Fear & Greed Index from alternative.me.
func (c *Client) GetFearGreedIndex(ctx context.Context) (FearGreedIndex, error) {
	c.logger.LogDebug("Fetching Fear & Greed Index from %s", c.apiURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.apiURL, nil)
	if err != nil {
		return FearGreedIndex{}, fmt.Errorf("F&G: create request failed: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "SnowballinBot/1.0")

	var raw altFearGreedResponse
	// one retry, short backoff
	if err := utilities.DoJSONRequest(c.HTTPClient, req, 1, 2*time.Second, &raw); err != nil {
		return FearGreedIndex{}, fmt.Errorf("F&G: request/decoding failed: %w", err)
	}

	if raw.Metadata.Error != nil {
		return FearGreedIndex{}, fmt.Errorf("F&G API error: %s", *raw.Metadata.Error)
	}
	if len(raw.Data) == 0 {
		return FearGreedIndex{}, errors.New("F&G: no data returned")
	}

	dp := raw.Data[0]
	value, err := strconv.Atoi(dp.Value)
	if err != nil {
		return FearGreedIndex{}, fmt.Errorf("F&G: invalid value '%s': %w", dp.Value, err)
	}
	ts, err := strconv.ParseInt(dp.Timestamp, 10, 64)
	if err != nil {
		return FearGreedIndex{}, fmt.Errorf("F&G: invalid timestamp '%s': %w", dp.Timestamp, err)
	}

	return FearGreedIndex{
		Value:     value,
		Level:     dp.ValueClassification,
		Timestamp: ts,
	}, nil
}
