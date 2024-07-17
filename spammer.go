package main

import (
	"fmt"
	"sort"
	"sync"
)

// тип для результата, используемый для функции сортировки
type MsgDatas []MsgData

// функции, необходимые для возможности сортировки типа MsgDatas
func (msgDatas MsgDatas) Len() int { return len(msgDatas) }

func (msgDatas MsgDatas) Swap(a, b int) { msgDatas[a], msgDatas[b] = msgDatas[b], msgDatas[a] }

func (msgDatas MsgDatas) Less(a, b int) bool {
	if msgDatas[a].HasSpam != msgDatas[b].HasSpam {
		return msgDatas[a].HasSpam
	}
	return msgDatas[a].ID < msgDatas[b].ID
}

// функция запуска конвейера
func RunPipeline(cmds ...cmd) {
	var in, out chan interface{}
	wg := &sync.WaitGroup{}

	for _, c := range cmds {
		in = out
		out = make(chan interface{})

		wg.Add(1)
		go func(c cmd, in, out chan interface{}) {
			defer wg.Done()
			defer close(out)

			c(in, out)
		}(c, in, out)
	}
	wg.Wait()
}

func SelectUsers(in, out chan interface{}) {
	// 	in - string
	// 	out - User

	seen := make(map[uint64]bool) // мапа для уже встреченных user.ID
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}

	for email := range in {
		wg.Add(1)
		go func(email interface{}) {
			defer wg.Done()
			user := GetUser(email.(string)) // получение пользователя из базы по email

			// мьютекс используется для защищенного доступа к мапе
			mu.Lock()
			// проверка уникальности пользователя по user.ID
			if _, ok := seen[user.ID]; !ok {
				out <- user
				seen[user.ID] = true
			}
			mu.Unlock()
		}(email)
	}
	wg.Wait()
}

func SelectMessages(in, out chan interface{}) {
	// 	in - User
	// 	out - MsgID

	wg := &sync.WaitGroup{}

	for {
		users := make([]User, 0, GetMessagesMaxUsersBatch) // слайс пользователей, используемый для батчей
		for i := 0; i < GetMessagesMaxUsersBatch; i++ {
			user, ok := <-in // чтение юзеров из канала, пока не достигнут лимит батча
			if !ok {
				break
			}
			users = append(users, user.(User))
		}

		if len(users) == 0 {
			break
		}

		wg.Add(1)
		go func(users []User) {
			defer wg.Done()
			msgIDs, err := GetMessages(users...) // получение списка писем (msgID) по заданным юзерам при помощи батчей
			if err != nil {
				fmt.Printf("error happened: %v\n", err)
			} else {
				for _, msgID := range msgIDs {
					out <- msgID
				}
			}
		}(users)
	}
	wg.Wait()
}

func CheckSpam(in, out chan interface{}) {
	// in - MsgID
	// out - MsgData

	// семафор используется для ограничения количества асинхронных запросов
	semaphore := make(chan struct{}, HasSpamMaxAsyncRequests)
	wg := &sync.WaitGroup{}

	for msgID := range in {
		semaphore <- struct{}{} // запись пустой структуры в семафор перед запуском очередной горутины
		wg.Add(1)
		go func(msgID interface{}) {
			defer func() {
				wg.Done()
				<-semaphore // чтение из семафора при завершении горутины
			}()
			hasSpam, err := HasSpam(msgID.(MsgID)) // определение наличия спама в письме по msgID
			if err != nil {
				fmt.Printf("error happened: %v\n", err)
			} else {
				out <- MsgData{msgID.(MsgID), hasSpam}
			}
		}(msgID)
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	// in - MsgData
	// out - string

	results := make(MsgDatas, 0)

	for msgData := range in {
		results = append(results, msgData.(MsgData))
	}
	sort.Sort(results)

	for _, elem := range results {
		out <- fmt.Sprint(elem.HasSpam, elem.ID)
	}
}

