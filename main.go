package main

import (
	"context"
	"github.com/VadimGossip/kafkaGoExample/kafka"
	"github.com/sirupsen/logrus"
)

func main() {
	for {
		type test struct {
			Id   int64
			Text string
		}

		msg := []test{
			{1, "222"},
			{2, "d222"},
			{3, "X222"},
		}

		kw := kafka.NewWriter[test]()
		if err := kw.WriteBatch(context.Background(), msg...); err != nil {
			logrus.Info(err)
		}
	}
}
