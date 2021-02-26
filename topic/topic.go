package topic

import (
	"encoding/json"
	ali_mns "github.com/aliyun/aliyun-mns-go-sdk"
	"github.com/coffee1998/go-mns/DB"
	"github.com/coffee1998/go-mns/config"
	"github.com/coffee1998/go-mns/crontab"
	"github.com/coffee1998/go-mns/models"
	"log"
	"sync"
	"time"
)

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
func (this *MNSTopic) SendMessage(messageBody interface{}) (ali_mns.MessageSendResponse, error) {
	var ret string
	if data, ok := messageBody.(string); ok {
		ret = data
	} else {
		data, _ := json.Marshal(messageBody)
		ret = string(data)
	}
	resp, err := this.topic.PublishMessage(ali_mns.MessagePublishRequest{MessageBody: ret})

	if err != nil {
		if !this.isUpdate {
			this.Addretry(ret, err.Error())
		}
	}
	return resp, err
}

func (this *MNSTopic) Addretry(data, errMsg string) {
	DB.InsertRetry(errMsg, data, this.c.TopicName, time.Now().Unix())
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
	limit := 1000
	page := 1
	wg := sync.WaitGroup{}
	for {
		data, err := DB.QueryRetry(retryTimes, this.c.TopicName, limit, page)
		if err != nil || len(data) == 0{
			break
		}
		wg.Add(1)
		go func(items []*models.Retry) {
			defer wg.Done()
			for _, item := range items {
				this.isUpdate = true
				_, err = this.SendMessage(item.Data)
				newRetryNum := item.RetryNum + 1
				isDone := item.IsDone
				if err != nil {
					if newRetryNum >= retryTimes {
						isDone = 1
					}
					DB.UpdateRetry(newRetryNum, isDone, err.Error(), item.Id)
				} else {
					DB.DelByID(item.Id)
				}

				//出错次数达到可重试总次数，则发送通知到指定途径
				if err != nil && newRetryNum >= retryTimes && noticeFunc != nil {
					noticeFunc()
				}
			}
		}(data)
		page++
		time.Sleep(3*time.Second)
	}
	wg.Wait()
}
