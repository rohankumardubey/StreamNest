package broker

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

// Compressed logs
func logPath(topic string, partition int) string {
	return filepath.Join("data", fmt.Sprintf("%s_%d.log.gz", topic, partition))
}

// Write a message as gzip-compressed line
func AppendPartitionLog(topic string, partition int, msg string) error {
	path := logPath(topic, partition)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	// Open file in append mode
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// Compress the message to bytes
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, err = gz.Write([]byte(msg + "\n"))
	if err != nil {
		return err
	}
	gz.Close()
	// Write compressed bytes to file
	_, err = f.Write(buf.Bytes())
	return err
}

// Save topic metadata as gzip-compressed JSON
func SaveTopicMetadata(topic string, owners []string) error {
	if err := os.MkdirAll("data", 0755); err != nil {
		return err
	}
	meta := map[string]interface{}{
		"topic":  topic,
		"owners": owners,
	}
	b, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(b); err != nil {
		return err
	}
	gz.Close()

	path := filepath.Join("data", topic+".meta.json.gz")
	return os.WriteFile(path, buf.Bytes(), 0644)
}


// Load all topic metadata from gzip-compressed files
func LoadAllTopicMetadata() (map[string][]string, error) {
	mapper := make(map[string][]string)
	files, err := ioutil.ReadDir("data")
	if err != nil {
		if os.IsNotExist(err) {
			return mapper, nil
		}
		return nil, err
	}
	for _, f := range files {
		name := f.Name()
		if !f.IsDir() && strings.HasSuffix(name, ".meta.json.gz") {
			raw, err := os.ReadFile(filepath.Join("data", name))
			if err != nil {
				continue
			}
			gr, err := gzip.NewReader(bytes.NewReader(raw))
			if err != nil {
				continue
			}
			uncompressed, err := io.ReadAll(gr)
			gr.Close()
			if err != nil {
				continue
			}
			var meta struct {
				Topic  string   `json:"topic"`
				Owners []string `json:"owners"`
			}
			if err := json.Unmarshal(uncompressed, &meta); err != nil {
				continue
			}
			mapper[meta.Topic] = meta.Owners
		}
	}
	return mapper, nil
}


// loading the partitioned logs
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

	gz, err := gzip.NewReader(file)
	if err != nil {
		return nil, err
	}
	defer gz.Close()

	scanner := bufio.NewScanner(gz)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 {
			messages = append(messages, line)
		}
	}
	return messages, nil
}

// Save schema to disk as gzip-compressed JSON
func SaveSchema(topic string, schema map[string]interface{}) error {
	if err := os.MkdirAll("data", 0755); err != nil {
		return err
	}
	fpath := filepath.Join("data", topic+".schema.json.gz")
	b, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(b); err != nil {
		return err
	}
	gz.Close()

	return os.WriteFile(fpath, buf.Bytes(), 0644)
}

// Load all schemas from gzip-compressed files
func LoadAllSchemas() (map[string]map[string]interface{}, error) {
	schemas := make(map[string]map[string]interface{})
	files, err := ioutil.ReadDir("data")
	if err != nil {
		if os.IsNotExist(err) {
			return schemas, nil
		}
		return nil, err
	}
	for _, f := range files {
		name := f.Name()
		if !f.IsDir() && strings.HasSuffix(name, ".schema.json.gz") {
			raw, err := os.ReadFile(filepath.Join("data", name))
			if err != nil {
				continue
			}
			gr, err := gzip.NewReader(bytes.NewReader(raw))
			if err != nil {
				continue
			}
			uncompressed, err := io.ReadAll(gr)
			gr.Close()
			if err != nil {
				continue
			}
			var schema map[string]interface{}
			if err := json.Unmarshal(uncompressed, &schema); err != nil {
				continue
			}
			topic := strings.TrimSuffix(name, ".schema.json.gz")
			schemas[topic] = schema
		}
	}
	return schemas, nil
}
