package topic

import (
	"encoding/json"
	"fmt"
	ali_mns "github.com/aliyun/aliyun-mns-go-sdk"
	"github.com/coffee1998/go-mns/config"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"log"
	"sync"
	"time"
)

const (
	DataBase = "coffee1998_mns"
	RetryC   = "retry"
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
	c        *config.MNSConfig
	s        *mgo.Session
	topic    ali_mns.AliMNSTopic
	isUpdate bool
}

func NewMNSTopic(c *config.MNSConfig) *MNSTopic {
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
	var session *mgo.Session
	var err error
	if c.MongoURI != "" {
		if session, err = mgo.Dial(c.MongoURI); err != nil {
			log.Fatal("Failed to connection a MongoDB")
		}
	}
	client := ali_mns.NewAliMNSClient(c.Endpoint, c.AccessKeyId, c.AccessKeySecret)
	topic := ali_mns.NewMNSTopic(c.TopicName, client)

	return &MNSTopic{c: c, s: session, topic: topic}
}

// 发送消息到主题
func (this *MNSTopic) SendMessage(messageBody bson.M) (ali_mns.MessageSendResponse, error) {
	data, _ := json.Marshal(messageBody)
	resp, err := this.topic.PublishMessage(ali_mns.MessagePublishRequest{MessageBody: string(data)})

	if err != nil {
		if !this.isUpdate && this.s != nil {
			this.Addretry(string(data))
		}
	}
	return resp, err
}

func (this *MNSTopic) Addretry(data string) {
	if this.s == nil {
		return
	}

	session := this.s.Copy()
	defer session.Close()

	doc := &Retry{
		Id:         bson.NewObjectId().Hex(),
		RetryNum:   0,
		IsDone:     0,
		IsSucc:     0,
		Data:       data,
		TopicName:  this.c.TopicName,
		UpdateTime: time.Now().Unix(),
	}
	session.DB(DataBase).C(RetryC).Insert(doc)
}

// 重试
func (this *MNSTopic) Retry(retryTimes int, callback func() error) {
	if this.s == nil {
		return
	}

	session := this.s.Copy()
	defer session.Close()

	var data []*Retry
	session.DB(DataBase).C(RetryC).Find(bson.M{"retry_num": bson.M{"$lt": retryTimes}, "is_done": 0, "topic_name": this.c.TopicName}).All(&data)
	if len(data) == 0 {
		return
	}

	var ret bson.M
	wg := sync.WaitGroup{}
	wg.Add(len(data))
	for _, item := range data {
		go func(item *Retry) {
			this.isUpdate = true
			if e := json.Unmarshal([]byte(item.Data), &ret); e != nil {
				fmt.Println(e)
			} else {
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
					if callback != nil {
						callback()
					}
				} else {
					doc["is_done"] = 1
					doc["is_succ"] = 1
				}
				session.DB(DataBase).C(RetryC).UpdateId(item.Id, bson.M{"$set": doc})
			}
			wg.Done()
		}(item)
	}
	wg.Wait()
}
