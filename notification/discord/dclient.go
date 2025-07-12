// File: notification/discord/client.go
package discord

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"Snowballin/pkg/broker"
	"Snowballin/utilities" // Used for utilities.Logger

	"github.com/spf13/viper" // Used in NewClient for logger config
)

// Client sends notifications to a Discord webhook.
type Client struct {
	webhookURL string
	HTTPClient *http.Client
	logger     *utilities.Logger
}

// DiscordMessage represents the structure for a Discord webhook message.
// See: https://discord.com/developers/docs/resources/webhook#execute-webhook
type DiscordMessage struct {
	Content string         `json:"content,omitempty"`
	Embeds  []DiscordEmbed `json:"embeds,omitempty"`
}

// DiscordEmbed represents an embed object in a Discord message.
type DiscordEmbed struct {
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
	URL         string `json:"url,omitempty"`       // Kept, but not set by NotifyOrderFilled
	Timestamp   string `json:"timestamp,omitempty"` // ISO8601 timestamp
	Color       int    `json:"color,omitempty"`     // Decimal color code
}

func NewClient(webhookURL string) *Client {
	logLevel := utilities.Info
	if viper.GetBool("debug") {
		logLevel = utilities.Debug
	}

	logger := utilities.NewLogger(logLevel)

	if webhookURL == "" {
		logger.LogWarn("Discord Client: Webhook URL is empty. Notifications will not be sent.")
	} else {
		logger.LogInfo("Discord Client initialized with webhook URL.")
	}

	return &Client{
		webhookURL: webhookURL,
		HTTPClient: &http.Client{Timeout: 10 * time.Second},
		logger:     logger,
	}
}

// SendMessage sends a simple text message to the configured Discord webhook.
func (c *Client) SendMessage(message string) error {
	if c.webhookURL == "" {
		c.logger.LogDebug("Discord SendMessage: Webhook URL is not set, skipping.")
		return nil
	}

	if strings.TrimSpace(message) == "" {
		c.logger.LogDebug("Discord SendMessage: Message is empty, skipping.")
		return nil
	}

	payload := DiscordMessage{
		Content: message,
	}
	return c.sendPayload(payload)
}

// SendEmbedMessage sends a message with one or more embeds.
func (c *Client) SendEmbedMessage(embeds ...DiscordEmbed) error {
	if c.webhookURL == "" {
		c.logger.LogDebug("Discord SendEmbedMessage: Webhook URL is not set, skipping.")
		return nil
	}
	if len(embeds) == 0 {
		c.logger.LogDebug("Discord SendEmbedMessage: No embeds provided, skipping.")
		return nil
	}
	payload := DiscordMessage{
		Embeds: embeds,
	}
	return c.sendPayload(payload)
}

// sendPayload is an internal helper to send the marshalled JSON payload.
func (c *Client) sendPayload(payload DiscordMessage) error {
	if c.webhookURL == "" {
		return fmt.Errorf("discord webhook URL not configured")
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		c.logger.LogError("Discord sendPayload: Failed to marshal JSON: %v", err)
		return fmt.Errorf("failed to marshal discord message: %w", err)
	}

	c.logger.LogDebug("Discord sendPayload: Sending to webhook. Payload size: %d bytes", len(payloadBytes))

	req, err := http.NewRequest("POST", c.webhookURL, bytes.NewBuffer(payloadBytes))
	if err != nil {
		c.logger.LogError("Discord sendPayload: Failed to create HTTP request: %v", err)
		return fmt.Errorf("failed to create discord request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "SnowballinBot/1.0")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		c.logger.LogError("Discord sendPayload: Failed to send HTTP request: %v", err)
		return fmt.Errorf("failed to send discord message: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		c.logger.LogDebug("Discord sendPayload: Message sent successfully (Status: %s)", resp.Status)
		return nil
	}

	bodyBytes, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		c.logger.LogError("Discord sendPayload: Received non-OK status: %s. Failed to read body: %v", resp.Status, readErr)
		return fmt.Errorf("discord API error: %s, failed to read response body", resp.Status)
	}
	c.logger.LogError("Discord sendPayload: Received non-OK status: %s. Body: %s", resp.Status, string(bodyBytes))
	return fmt.Errorf("discord API error: %s, response: %s", resp.Status, string(bodyBytes))
}

// NotifyOrderFilled sends a formatted notification for a filled order.
func (c *Client) NotifyOrderFilled(order broker.Order, additionalDetails string) error {
	if c.webhookURL == "" {
		c.logger.LogDebug("Discord NotifyOrderFilled: Webhook URL is not set, skipping.")
		return nil
	}

	var title string
	var color int

	sideUpper := strings.ToUpper(order.Side)

	// Set the title and color based on the order side (buy/sell)
	if sideUpper == "BUY" {
		title = fmt.Sprintf("✅ BUY Order Filled: %s", order.Pair)
		color = 3066993 // Green
	} else if sideUpper == "SELL" {
		title = fmt.Sprintf("💰 SELL Order Filled: %s", order.Pair)
		color = 15158332 // Red
	} else {
		title = fmt.Sprintf("ℹ️ Order Update: %s (%s)", order.Pair, sideUpper)
		color = 3447003 // Blue
	}

	// --- FIX: The rest of the details will now be in the description, not the title ---

	var baseAsset, quoteAsset string
	pairParts := strings.Split(order.Pair, "/")
	if len(pairParts) > 1 {
		baseAsset = pairParts[0]
		quoteAsset = pairParts[1]
	} else {
		baseAsset = order.Pair // Fallback
	}

	// Create the main block of details about the order
	fieldDetails := fmt.Sprintf(
		"**Pair**: %s\n"+
			"**Avg. Fill Price**: `%.4f %s`\n"+
			"**Filled Volume**: `%.8f %s`\n"+
			"**Total Cost**: `%.2f %s`\n"+
			"**Order ID**: `%s`",
		order.Pair,
		order.AvgFillPrice, quoteAsset,
		order.FilledVolume, baseAsset,
		order.Cost, quoteAsset,
		order.ID,
	)

	// --- FIX: Construct the full description by combining the dynamic details from app.go
	// with the static details we just created. This keeps the title clean.
	fullDescription := fmt.Sprintf("%s\n\n%s", additionalDetails, fieldDetails)

	// Use a valid timestamp for the embed
	timestamp := order.TimeCompleted
	if timestamp.IsZero() {
		timestamp = time.Now() // Fallback if the completion time isn't set
	}

	embed := DiscordEmbed{
		Title:       title,
		Description: fullDescription,
		Color:       color,
		Timestamp:   timestamp.Format(time.RFC3339),
	}

	return c.SendEmbedMessage(embed)
}
