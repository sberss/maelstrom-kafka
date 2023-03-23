package fakeafka

import "sync"

type Store struct {
	logs sync.Map
}

func NewStore() *Store {
	return &Store{
		logs: sync.Map{},
	}
}

func (s *Store) AppendToLog(key string, msg int) int {
	log, _ := s.logs.LoadOrStore(key, NewLog(key))
	return log.(*Log).append(msg)
}

func (s *Store) Poll(offsets map[string]int) map[string][]Message {
	messages := make(map[string][]Message)

	for key, offset := range offsets {
		log, ok := s.logs.Load(key)
		if !ok {
			messages[key] = []Message{}
			continue
		}

		messages[key] = log.(*Log).poll(offset, 1000)
	}

	return messages
}

func (s *Store) CommitOffsets(offsets map[string]int) {
	for key, offset := range offsets {
		log, _ := s.logs.Load(key)
		log.(*Log).commitOffset(offset)
	}
}

func (s *Store) GetCommittedOffsets(keys []string) map[string]int {
	offsets := make(map[string]int)

	for _, key := range keys {
		if _, ok := offsets[key]; !ok {
			offsets[key] = 0
			continue
		}

		log, _ := s.logs.Load(key)
		offsets[key] = log.(*Log).getCommittedOffset()
	}

	return offsets
}
