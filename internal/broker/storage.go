package broker

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
)

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
