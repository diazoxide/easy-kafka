package easykafka

func StreamWithConsumerOptions[T any](opts ...ConsumerOption[T]) StreamOption[T] {
	return func(s *Stream[T]) (err error) {
		s.consumerOptions = opts
		return nil
	}
}

func StreamWithProducerOptions[T any](opts ...BaseProducerOption) StreamOption[T] {
	return func(s *Stream[T]) (err error) {
		s.producerOptions = opts
		return nil
	}
}
