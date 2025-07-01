package broker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

// Partition log persistence
func logPath(topic string, partition int) string {
	return filepath.Join("data", fmt.Sprintf("%s_%d.log", topic, partition))
}

func LoadPartitionLog(topic string, partition int) ([]string, error) {
	path := logPath(topic, partition)
	var messages []string
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return []string{}, nil
	} else if err != nil {
		return nil, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		messages = append(messages, scanner.Text())
	}
	return messages, scanner.Err()
}

func AppendPartitionLog(topic string, partition int, msg string) error {
	path := logPath(topic, partition)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.WriteString(msg + "\n")
	return err
}

// SCHEMA REGISTRY 
// Store schema to disk
func SaveSchema(topic string, schema map[string]interface{}) error {
	if err := os.MkdirAll("data", 0755); err != nil {
		return err
	}
	fpath := filepath.Join("data", fmt.Sprintf("%s.schema.json", topic))
	b, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(fpath, b, 0644)
}

// Load all topic schemas from disk
func LoadAllSchemas() (map[string]map[string]interface{}, error) {
	schemas := make(map[string]map[string]interface{})
	files, err := ioutil.ReadDir("data")
	if err != nil {
		if os.IsNotExist(err) {
			return schemas, nil // No schemas yet
		}
		return nil, err
	}
	for _, f := range files {
		if !f.IsDir() && filepath.Ext(f.Name()) == ".json" && len(f.Name()) > len(".schema.json") {
			if len(f.Name()) > len(".schema.json") && f.Name()[len(f.Name())-len(".schema.json"):] == ".schema.json" {
				topic := f.Name()[:len(f.Name())-len(".schema.json")]
				raw, err := ioutil.ReadFile(filepath.Join("data", f.Name()))
				if err != nil {
					continue
				}
				var schema map[string]interface{}
				if err := json.Unmarshal(raw, &schema); err != nil {
					continue
				}
				schemas[topic] = schema
			}
		}
	}
	return schemas, nil
}
