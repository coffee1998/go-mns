package topic

import (
	"encoding/json"
	"fmt"
	ali_mns "github.com/aliyun/aliyun-mns-go-sdk"
	"github.com/coffee1998/go-mns/DB"
	"github.com/coffee1998/go-mns/config"
	"github.com/coffee1998/go-mns/crontab"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"log"
	"sync"
	"time"
)

type Retry struct {
	Id         string `json:"id" bson:"_id" comment:"id"`
	RetryNum   int    `json:"retry_num" bson:"retry_num" comment:"重试次数"`
	IsDone     int    `json:"is_done" bson:"is_done" comment:"是否重试完毕 0：否 1：是"`
	IsSucc     int    `json:"is_succ" bson:"is_succ" comment:"是否发送队列成功 0：否 1：是"`
	Data       string `json:"data" bson:"data" comment:"入列数据"`
	TopicName  string `json:"topic_name" bson:"topic_name" comment:"主题名称"`
	UpdateTime int64  `json:"update_time" bson:"update_time" comment:"更新时间"`
}

type MNSTopic struct {
	NoticeFunc config.NoticeFunc
	c        *config.MNSConfig
	topic    ali_mns.AliMNSTopic
	isUpdate bool
}

func NewMNSTopic(c *config.MNSConfig, noticeFunc config.NoticeFunc) *MNSTopic {
	if c.Endpoint == "" {
		log.Fatal("endpoint is required")
	}
	if c.AccessKeySecret == "" {
		log.Fatal("accessKeySecret is required")
	}
	if c.AccessKeyId == "" {
		log.Fatal("accessKeyId is required")
	}
	if c.TopicName == "" {
		log.Fatal("topic name is required")
	}
	client := ali_mns.NewAliMNSClient(c.Endpoint, c.AccessKeyId, c.AccessKeySecret)
	topic := ali_mns.NewMNSTopic(c.TopicName, client)

	mns := &MNSTopic{c: c, topic: topic, NoticeFunc: noticeFunc}

	crontab.AddJob(c.RetryIntervalInSecond, c.TopicName, mns.CronRetry)
	return mns
}

// 发送消息到主题
func (this *MNSTopic) SendMessage(messageBody bson.M) (ali_mns.MessageSendResponse, error) {
	data, _ := json.Marshal(messageBody)
	resp, err := this.topic.PublishMessage(ali_mns.MessagePublishRequest{MessageBody: string(data)})

	if err != nil {
		if !this.isUpdate {
			this.Addretry(string(data))
		}
	}
	return resp, err
}

func (this *MNSTopic) Addretry(data string) {
	DB.Exec(this.c.MongoURI, func(collection *mgo.Collection) error {
		doc := &Retry{
			Id:         bson.NewObjectId().Hex(),
			RetryNum:   0,
			IsDone:     0,
			IsSucc:     0,
			Data:       data,
			TopicName:  this.c.TopicName,
			UpdateTime: time.Now().Unix(),
		}
		return collection.Insert(doc)
	})
}

// 定时重试
func (this *MNSTopic) CronRetry()  {
	times := this.c.RetryIntervalInTimes
	if times == 0 {
		times = config.DefaultRetryTimes
	}
	this.Retry(this.c.RetryIntervalInTimes, this.NoticeFunc)
}

// 重试
func (this *MNSTopic) Retry(retryTimes int, noticeFunc config.NoticeFunc) {
	if this.c.MongoURI == "" {
		return
	}
	s, err := DB.GetMongoSession(this.c.MongoURI)
	if err != nil {
		return
	}
	defer s.Close()

	query := bson.M{"retry_num": bson.M{"$lt": retryTimes}, "is_done": 0, "topic_name": this.c.TopicName}

	limit := 1000
	page := 1
	wg := sync.WaitGroup{}
	for {
		var data []*Retry
		s.DB(DB.DataBase).C(DB.RetryC).Find(query).Skip((page-1)*limit).Limit(limit).All(&data)
		if len(data) == 0 {
			break
		}
		wg.Add(1)
		go func(items []*Retry) {
			defer wg.Done()

			bulk := s.DB(DB.DataBase).C(DB.RetryC).Bulk()
			for _, item := range items {
				var ret bson.M
				if e := json.Unmarshal([]byte(item.Data), &ret); e != nil {
					fmt.Println(e)
				} else {
					this.isUpdate = true
					_, err := this.SendMessage(ret)
					newRetryNum := item.RetryNum + 1
					doc := bson.M{
						"retry_num":   newRetryNum,
						"update_time": time.Now().Unix(),
					}
					if err != nil {
						if newRetryNum >= retryTimes {
							doc["is_done"] = 1
						}
						doc["is_succ"] = 0
					} else {
						doc["is_done"] = 1
						doc["is_succ"] = 1
					}
					bulk.Update(bson.M{"_id": item.Id}, bson.M{"$set": doc})
					//出错次数达到可重试总次数，则发送通知到指定途径
					if err != nil && newRetryNum >= retryTimes && noticeFunc != nil {
						noticeFunc()
					}
				}
			}
			bulk.Run()
		}(data)
		page++
		time.Sleep(3*time.Second)
	}
	wg.Wait()
}
