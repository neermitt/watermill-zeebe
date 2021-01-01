package zeebe

import (
	"strconv"

	"github.com/zeebe-io/zeebe/clients/go/pkg/entities"
)

type ConfigurationMap struct {
	config map[string]interface{}
}

func NewConfigurationMap(job entities.Job) (*ConfigurationMap, error) {
	headers, err := job.GetCustomHeadersAsMap()
	if err != nil {
		return nil, err
	}
	variables, err := job.GetVariablesAsMap()
	if err != nil {
		return nil, err
	}

	config := make(map[string]interface{})

	for k, v := range headers {
		config[k] = v
	}
	for k, v := range variables {
		config[k] = v
	}

	config["jobKey"] = job.Key
	config["workflowInstanceKey"] = job.WorkflowInstanceKey

	return &ConfigurationMap{config: config}, nil
}

func (c *ConfigurationMap) HasKey(key string) bool {
	_, ok := c.config[key]
	return ok
}

func (c *ConfigurationMap) Get(key string) interface{} {
	if val, ok := c.config[key]; ok {
		return val
	}
	return nil
}

func (c *ConfigurationMap) convertToString(val interface{}) string {
	switch val.(type) {
	case string:
		return val.(string)
	}

	return ""
}

func (c *ConfigurationMap) GetMap(key string) map[string]interface{} {
	if val, ok := c.config[key]; ok {
		switch val.(type) {
		case map[string]interface{}:
			return val.(map[string]interface{})
		default:
			return nil
		}
	}

	return nil
}

func (c *ConfigurationMap) GetString(key string) string {
	if val, ok := c.config[key]; ok {
		switch val.(type) {
		case string:
			return val.(string)
		}
	}
	return ""
}

func (c *ConfigurationMap) GetInt(key string) int64 {
	if val, ok := c.config[key]; ok {
		switch val.(type) {
		case string:
			parseInt, _ := strconv.ParseInt(val.(string), 10, 64)
			return parseInt
		case float64:
			return int64(val.(float64))
		case int64:
			return val.(int64)
		}
	}
	return 0
}

func (c *ConfigurationMap) getConfig() map[string]interface{} {
	return c.config
}
