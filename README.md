#### 项目背景

---

目前运维平台组在做游戏库的时候，使用到了阿里云MNS消息中间件，阿里云也提供了官方的 `aliyun-mns-go-sdk` ，但是如果消息由于某些原因发送失败了，官方的SDK并没有提供重试的方法。



#### 简介

---

`go-mns `是基于 `aliyun-mns-go-sdk` (https://github.com/aliyun/aliyun-mns-go-sdk)进行二次封装的库，目前只提供以下功能：

+ 基于主题模型发送消息
+ 消息发送失败之后，提供重试方法



#### 使用

---

1. 下载特定的标记版本

   ```powershell
   go get -u github.com/coffee1998/go-mns@1.0.0
   ```

2. 使用范例

   ```go
   package main
   
   import (
   	"fmt"
   	"github.com/coffee1998/go-mns/config"
   	"github.com/coffee1998/go-mns/dingTalk"
   	"github.com/coffee1998/go-mns/topic"
   	"github.com/globalsign/mgo/bson"
   	"github.com/robfig/cron/v3"
   )
   
   func main()  {
       SendMessageToTopic()
   }
   
   // 发送消息到主题模型队列
   func SendMessageToTopic()  {
   	options := config.MNSConfig{
   		Endpoint:        "http://xxxx.mns.cn-hangzhou.aliyuncs.com/",  //必填
   		AccessKeyId:     "xxxx",     //必填
   		AccessKeySecret: "xxxx",     //必填
   		TopicName:       "MyTopic",  //必填
   		MongoURI:        "mongodb://localhost/admin",  //非必填。如果不填，则将数据插入MongoDB，然后可以使用定时任务重试，否则不入库
   	}
   	data := bson.M{
   		"task_id": bson.NewObjectId().Hex(),
   		"game_id": "123",
   	}
   	resp, err := topic.NewMNSTopic(&options).SendMessage(data)
   	if err != nil {
   		fmt.Println("err：", err)
   	} else {
   		fmt.Println("succ：", resp)
   	}
   }
   
   //使用定时任务定时重试
   func Crontab()  {
   	c := cron.New(cron.WithSeconds())
   	c.AddFunc("0 */10 * * * *", func() {
   		options := config.MNSConfig{
   			Endpoint:        "http://xxxx.mns.cn-hangzhou.aliyuncs.com/",
   			AccessKeyId:     "xxxx",
   			AccessKeySecret: "xxxx",
   			TopicName:       "MyTopic",
   			MongoURI:        "mongodb://localhost/admin",
   		}
   
   		// 参数1：重试次数 参数2：回调函数。 如果不发送钉钉通知，则参数2：nil
   		//topic.NewMNSTopic(&options).Retry(5, nil)
   		topic.NewMNSTopic(&options).Retry(5, dingTalk.SendMessageToUser(config.DingTalkConfig{
   			Url: "https://xxx.xxx.xxx/v1d0/dingtalk/message",
   			UserList: "5ec1f8504c0c2f0001240063",
   			Message: "测试！！！",
   		}))
   	})
   	c.Start()
   }
   
   ```

   

